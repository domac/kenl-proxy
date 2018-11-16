package proxy

import (
	"fmt"
	log "github.com/domac/kenl-proxy/logger"
	srv "github.com/domac/kenl-proxy/server"
	bp "github.com/tevid/go-tevid-utils/bytes_pool"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

const (
	connSlots = 32
)

type ProxyConfig struct {
	MaxConn          int
	MsgBuffSize      int
	MsgSendChanSize  int
	ProxyIdleTimeout time.Duration
	AuthKey          string
}

//对端信息
type PeersInfo struct {
	peers [2]*srv.Session
}

func (ps *PeersInfo) GetPeers() [2]*srv.Session {
	return ps.peers
}

type IOUnit struct {
	Units [2]*srv.Server
}

func (io *IOUnit) ServerUnit() *srv.Server {
	return io.Units[1]
}

func (io *IOUnit) SetServerUnit(server *srv.Server) {
	io.Units[1] = server
}

func (io *IOUnit) SetClientUnit(server *srv.Server) {
	io.Units[0] = server
}

func (io *IOUnit) ClientUnit() *srv.Server {
	return io.Units[0]
}

func (io *IOUnit) Stop() {
	io.ClientUnit().Stop()
	io.ServerUnit().Stop()
}

// 代理层
type Proxy struct {
	proxyProtocol
	clock *SimpleClock
	//servers [2]*srv.Server
	iounit *IOUnit

	gNetConnId  uint32
	sessionhubs [connSlots][2]*srv.SessionHub

	gVConnId   uint32
	vConns     [connSlots]map[uint32]*PeersInfo
	vConnMutex [connSlots]sync.RWMutex
}

func NewProxy(pool *bp.BytesPool, maxPacketSize int) *Proxy {
	proxy := new(Proxy)
	proxy.pool = pool
	proxy.maxPacketSize = maxPacketSize
	proxy.clock = NewSimpleClock(100*time.Millisecond, 18000)

	for i := 0; i < connSlots; i++ {
		proxy.vConns[i] = make(map[uint32]*PeersInfo)
		proxy.sessionhubs[i][0] = srv.NewSessionHub()
		proxy.sessionhubs[i][1] = srv.NewSessionHub()
	}
	proxy.iounit = new(IOUnit)
	return proxy
}

func (p *Proxy) ServeServers(ln net.Listener, cfg ProxyConfig) {
	p.iounit.SetServerUnit(srv.NewServer(ln,
		srv.ProtocolFunc(func(rw io.ReadWriter) (srv.Codec, error) {
			serverId, err := p.serverAuth(rw.(net.Conn), []byte(cfg.AuthKey))
			if err != nil {
				log.GetLogger().Errorf("error accept server from %s: %s", rw.(net.Conn).RemoteAddr(), err)
				return nil, err
			}
			log.GetLogger().Infof("accept server %d from %s", serverId, rw.(net.Conn).RemoteAddr())
			return p.newCodec(serverId, rw.(net.Conn), cfg.MsgBuffSize), nil
		}), cfg.MsgSendChanSize,
		srv.DefaultHandlerFunc(func(session *srv.Session) {
			p.handleSession(session, 1, 0, cfg.ProxyIdleTimeout)
		})))
	p.iounit.ServerUnit().Serve()
}

func (p *Proxy) ServeClients(ln net.Listener, cfg ProxyConfig) {
	p.iounit.SetClientUnit(srv.NewServer(ln,
		srv.ProtocolFunc(func(rw io.ReadWriter) (srv.Codec, error) {
			cid := atomic.AddUint32(&p.gNetConnId, 1)
			return p.newCodec(cid, rw.(net.Conn), cfg.MsgBuffSize), nil
		}), cfg.MsgSendChanSize,
		srv.DefaultHandlerFunc(func(session *srv.Session) {
			p.handleSession(session, 0, cfg.MaxConn, cfg.ProxyIdleTimeout)
		})))
	p.iounit.ClientUnit().Serve()
}

//处理会话信息
func (p *Proxy) handleSession(session *srv.Session, side, maxConn int, idleTimeout time.Duration) {

	cid := session.Codec().(*proxyCodec).id
	state := p.newSessionState(cid, session)
	session.State = state
	p.addNetConn(cid, side, session)

	defer func() {
		state.Disposed()
		if err := recover(); err != nil {
			log.GetLogger().Error("Proxy Panic:", err)
		}
	}()

	otherside := 1 &^ side

	log.GetLogger().Infof("side = %d , othersize = %d ", side, otherside)

	for {

		//设置最大空闲处理
		if idleTimeout > 0 {
			err := session.Codec().(*proxyCodec).conn.SetDeadline(time.Now().Add(idleTimeout))
			if err != nil {
				return
			}
		}

		buf, err := session.Receive()

		if err != nil {
			return
		}

		msg := *(buf.(*[]byte))
		connId := p.decodePacket(msg)
		if connId == 0 {
			p.processCmd(msg, session, state, side, otherside, maxConn)
			continue
		}

		//获取对端信息
		peers := p.getVirtualConn(connId)

		if peers.GetPeers()[side] == nil || peers.GetPeers()[otherside] == nil {
			p.free(msg)
			p.send(session, p.encodeCloseCmd(connId))
			continue
		}

		if peers.GetPeers()[side] != session {
			p.free(msg)
			panic("peer session info not match")
		}
		p.send(peers.GetPeers()[otherside], msg)
	}
}

func (p *Proxy) processCmd(msg []byte, session *srv.Session, state *proxyState, side, otherside, maxConn int) {
	cmd := p.decodeCmd(msg)
	log.GetLogger().Debugf("%v | proxy processCmd = %d", msg, cmd)
	switch cmd {
	case dialCmd:
		remoteId := p.decodeDialCmd(msg)
		p.free(msg)

		var peers [2]*srv.Session
		peers[side] = session
		peers[otherside] = p.getNetConn(remoteId, otherside)

		info := &PeersInfo{peers}

		if peers[otherside] == nil || !p.acceptVirtualConns(info, session, maxConn) {
			p.send(session, p.encodeRefuseCmd(remoteId))
		}
	case closeCmd:
		connId := p.decodeCloseCmd(msg)
		p.free(msg)
		p.closeVirtualConns(connId)

	case pingCmd:
		p.free(msg)
		p.send(session, p.encodePingCmd())

	default:
		p.free(msg)
		panic(fmt.Sprintf("unsupported proxy command : %d", p.decodeCmd(msg)))
	}
}

func (p *Proxy) Stop() {
	p.iounit.Stop()
	p.clock.Stop()
}

//端服务状态信息
type proxyState struct {
	sync.Mutex
	id          uint32
	proxy       *Proxy
	session     *srv.Session
	lastActive  int64
	pingChan    chan struct{}
	watchChan   chan struct{}
	disposeChan chan struct{}
	disposeOnce sync.Once
	disposed    bool
	vConns      map[uint32]struct{}
}

func (p *Proxy) newSessionState(id uint32, session *srv.Session) *proxyState {
	return &proxyState{
		id:          id,
		session:     session,
		proxy:       p,
		watchChan:   make(chan struct{}),
		pingChan:    make(chan struct{}),
		disposeChan: make(chan struct{}),
		vConns:      make(map[uint32]struct{}),
	}
}

func (ps *proxyState) Disposed() {
	ps.disposeOnce.Do(func() {
		close(ps.disposeChan)
		ps.session.Close()

		ps.Lock()
		ps.disposed = true
		ps.Unlock()

		for connId := range ps.vConns {
			ps.proxy.closeVirtualConns(connId)
		}
	})
}

//put到channel后会在session中注册一个closedcallback
//session关闭后，会自动清除这个关系表
func (p *Proxy) addNetConn(connId uint32, side int, session *srv.Session) {
	p.sessionhubs[connId%connSlots][side].Put(connId, session)
}

func (p *Proxy) getNetConn(connId uint32, side int) *srv.Session {
	return p.sessionhubs[connId%connSlots][side].Get(connId)
}

//添加虚链接
func (p *Proxy) addVirtualConn(connId uint32, peers *PeersInfo) {
	slotIndex := connId % connSlots
	p.vConnMutex[slotIndex].Lock()
	defer p.vConnMutex[slotIndex].Unlock()
	if _, exists := p.vConns[slotIndex][connId]; exists {
		panic("virtual connection already exists")
	}
	p.vConns[slotIndex][connId] = peers
}

//获取虚链接
func (p *Proxy) getVirtualConn(connId uint32) *PeersInfo {
	slotIndex := connId % connSlots
	p.vConnMutex[slotIndex].Lock()
	defer p.vConnMutex[slotIndex].Unlock()
	return p.vConns[slotIndex][connId]
}

//删除虚链接
func (p *Proxy) delVirtualConn(connId uint32) (*PeersInfo, bool) {
	slotIndex := connId % connSlots
	p.vConnMutex[slotIndex].Lock()
	pi, ok := p.vConns[slotIndex][connId]
	//如果虚链接的结构全部已经清理了，是 非ok, 这样返回 false
	if ok {
		delete(p.vConns[slotIndex], connId)
	}
	p.vConnMutex[slotIndex].Unlock()
	return pi, ok
}

//接受虚链接请求
func (p *Proxy) acceptVirtualConns(info *PeersInfo, session *srv.Session, maxConn int) bool {
	var connId uint32
	for connId == 0 {
		connId = atomic.AddUint32(&p.gVConnId, 1)
	}

	for i := 0; i < 2; i++ {
		state := info.GetPeers()[i].State.(*proxyState)
		state.Lock()
		defer state.Unlock()

		if state.disposed {
			log.GetLogger().Error("proxy was disposed")
			return false
		}

		if info.GetPeers()[i] == session && maxConn != 0 && len(state.vConns) >= maxConn {
			log.GetLogger().Error("conn size over maxconn")
			return false
		}

		if _, exists := state.vConns[connId]; exists {
			panic("virtual connection already exists")
		}
		state.vConns[connId] = struct{}{}
	}

	//更新虚链接信息
	p.addVirtualConn(connId, info)

	pcount := len(info.GetPeers())

	for i := 0; i < pcount; i++ {
		//other size cid
		remoteId := info.GetPeers()[(i+1)%pcount].State.(*proxyState).id
		if info.GetPeers()[i] == session {
			p.send(info.GetPeers()[i], p.encodeAcceptCmd(connId, remoteId))
		} else {
			p.send(info.GetPeers()[i], p.encodeConnectCmd(connId, remoteId))
		}
	}
	return true
}

//关闭虚链接
func (p *Proxy) closeVirtualConns(connId uint32) {
	peers, ok := p.delVirtualConn(connId)
	if !ok {
		return
	}
	for i := 0; i < len(peers.GetPeers()); i++ {
		state := peers.GetPeers()[i].State.(*proxyState)
		state.Lock()
		defer state.Unlock()
		if state.disposed {
			continue
		}
		//只清集合中的信息，和发送清理信息，不直接对session有任何关闭操作
		delete(state.vConns, connId)
		p.send(peers.GetPeers()[i], p.encodeCloseCmd(connId))
	}
}
