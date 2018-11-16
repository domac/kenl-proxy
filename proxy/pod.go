package proxy

import (
	"errors"
	log "github.com/domac/kenl-proxy/logger"
	srv "github.com/domac/kenl-proxy/server"
	bp "github.com/tevid/go-tevid-utils/bytes_pool"
	"io"
	"net"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"
)

var PodClock = NewSimpleClock(time.Second, 1800)

var ErrRefused = errors.New("virtual connection refused error")

type PodConfig struct {
	pool            *bp.BytesPool
	MaxPacket       int
	MsgBufferSize   int
	MsgSendChanSize int
	MsgRecvChanSize int
	PingInterval    time.Duration
	PingTimeout     time.Duration
	TimeoutCallBack func() bool
	ServerId        uint32
	AuthKey         string
	MsgFormat       MsgFormat
}

type ConnInfo struct {
	connId   uint32
	remoteId uint32
}

func (c *ConnInfo) ConnID() uint32 {
	return c.connId
}

func (c *ConnInfo) RemoteID() uint32 {
	return c.remoteId
}

type Pod struct {
	proxyProtocol
	format       MsgFormat
	manager      *srv.SessionManager
	recvChanSize int
	session      *srv.Session
	lastActive   int64
	newConnMutex sync.Mutex
	newConnChan  chan uint32
	dialMutex    sync.Mutex
	acceptChan   chan *srv.Session
	connectChan  chan *srv.Session
	sessionhub   *srv.SessionHub
	pingChan     chan struct{}
	closeChan    chan struct{}
	closeFlag    int32
}

func newPod(pool *bp.BytesPool, maxPacketSize, recvChanSize int, msgFormat MsgFormat) *Pod {
	return &Pod{
		proxyProtocol: proxyProtocol{
			pool:          pool,
			maxPacketSize: maxPacketSize,
		},
		format:       msgFormat,
		manager:      srv.NewSessionManager(),
		recvChanSize: recvChanSize,
		newConnChan:  make(chan uint32),
		acceptChan:   make(chan *srv.Session, 1),
		connectChan:  make(chan *srv.Session, 1000),
		sessionhub:   srv.NewSessionHub(),
		closeChan:    make(chan struct{}),
	}
}

func DialServer(network, addr string, cfg PodConfig) (*Pod, error) {
	conn, err := net.Dial(network, addr)
	if err != nil {
		return nil, err
	}
	return NewServer(conn, cfg)
}

func DialClient(network, addr string, cfg PodConfig) (*Pod, error) {
	conn, err := net.Dial(network, addr)
	if err != nil {
		return nil, err
	}
	return NewClient(conn, cfg), nil
}

func NewServer(conn net.Conn, cfg PodConfig) (*Pod, error) {

	pod := newPod(cfg.pool, cfg.MaxPacket, cfg.MsgRecvChanSize, cfg.MsgFormat)

	if err := pod.serverInit(conn, cfg.ServerId, []byte(cfg.AuthKey)); err != nil {
		log.GetLogger().Errorf("pod server auth error : %v", err)
		return nil, err
	}

	pod.session = srv.NewSession(pod.newCodec(0, conn, cfg.MsgBufferSize), cfg.MsgSendChanSize)

	go pod.loop()

	if cfg.PingInterval != 0 {
		if cfg.PingInterval > 1800*time.Second {
			panic("over ping interval limit")
		}

		if cfg.PingTimeout == 0 {
			panic("ping timeout is 0")
		}
		go pod.keepalive(cfg.PingInterval, cfg.PingTimeout, cfg.TimeoutCallBack)
	}

	return pod, nil
}

func NewClient(conn net.Conn, cfg PodConfig) *Pod {

	pod := newPod(cfg.pool, cfg.MaxPacket, cfg.MsgRecvChanSize, cfg.MsgFormat)

	pod.session = srv.NewSession(pod.newCodec(0, conn, cfg.MsgBufferSize), cfg.MsgSendChanSize)

	go pod.loop()

	if cfg.PingInterval != 0 {
		if cfg.PingInterval > 1800*time.Second {
			panic("over ping interval limit")
		}

		if cfg.PingTimeout == 0 {
			panic("ping timeout is 0")
		}

		go pod.keepalive(cfg.PingInterval, cfg.PingTimeout, cfg.TimeoutCallBack)
	}
	return pod
}

//读写Run-Loop
func (pod *Pod) loop() {
	defer func() {
		pod.Close()
		if err := recover(); err != nil {
			log.GetLogger().Errorf("panic info : %v\n%s", err, debug.Stack())
		}
	}()

	for {
		atomic.StoreInt64(&pod.lastActive, time.Now().UnixNano())

		msg, err := pod.session.Receive()
		if err != nil {
			return
		}

		buf := *(msg.(*[]byte))
		connId := pod.decodePacket(buf)

		if connId == 0 {
			pod.processCmd(buf)
			continue
		}

		sess := pod.sessionhub.Get(connId)
		if sess != nil {
			sess.Codec().(*virtualCodec).forward(buf)
		} else {
			pod.free(buf)
			pod.send(pod.session, pod.encodeCloseCmd(connId))
		}
	}
}

//命令处理
func (pod *Pod) processCmd(buf []byte) {
	cmd := pod.decodeCmd(buf)
	switch cmd {
	case acceptCmd:
		connId, remoteId := pod.decodeAcceptCmd(buf)
		pod.free(buf)
		pod.addVirtualConn(connId, remoteId, pod.acceptChan)

	case refuseCmd:
		pod.free(buf)
		select {
		case pod.acceptChan <- nil:
		case <-pod.closeChan:
			return
		}

	case connectCmd:
		connId, remoteId := pod.decodeConnectCmd(buf)
		pod.free(buf)
		pod.addVirtualConn(connId, remoteId, pod.connectChan)

	case closeCmd:
		connId := pod.decodeCloseCmd(buf)
		pod.free(buf)
		sess := pod.sessionhub.Get(connId)
		if sess != nil {
			sess.Close()
		}

	case pingCmd:
		pod.pingChan <- struct{}{}
		pod.free(buf)

	default:
		pod.free(buf)
		log.GetLogger().Errorf("unsupported cmd: %v", cmd)
		panic("unsupported cmd")
	}
}

//创建虚连接
func (pod *Pod) addVirtualConn(connId, remoteId uint32, c chan *srv.Session) {
	codec := pod.newVirtualCodec(pod.session, connId, pod.recvChanSize, &pod.lastActive, pod.format)
	session := pod.manager.NewSession(codec, 0)
	session.State = &ConnInfo{connId, remoteId}
	pod.sessionhub.Put(connId, session)
	select {
	case c <- session:
	case <-pod.closeChan:
	default:
		pod.send(pod.session, pod.encodeCloseCmd(connId))
	}
}

func (pod *Pod) Accept() (*srv.Session, error) {
	select {
	case sess := <-pod.connectChan:
		return sess, nil
	case <-pod.closeChan:
		return nil, io.EOF
	}
}

func (pod *Pod) Dial(remoteId uint32) (*srv.Session, error) {
	pod.dialMutex.Lock()
	defer pod.dialMutex.Unlock()

	if err := pod.send(pod.session, pod.encodeDialCmd(remoteId)); err != nil {
		log.GetLogger().Error(err)
		return nil, err
	}
	select {
	case sess := <-pod.acceptChan:
		if sess == nil {
			return nil, ErrRefused
		}
		return sess, nil
	case <-pod.closeChan:
		log.GetLogger().Error("got eof")
		return nil, io.EOF
	}
}

//关闭
func (pod *Pod) Close() {
	if atomic.CompareAndSwapInt32(&pod.closeFlag, 0, 1) {
		pod.manager.Dispose()
		pod.session.Close()
		close(pod.closeChan)
	}
}

func (pod *Pod) GetSession(sessionId uint64) *srv.Session {
	return pod.manager.GetSession(sessionId)
}

//保持连接
func (pod *Pod) keepalive(pingInterval, pingTimeout time.Duration, timeoutCallback func() bool) {
	pod.pingChan = make(chan struct{})
	for {
		select {
		case <-PodClock.After(pingTimeout):
			if pod.send(pod.session, pod.encodePingCmd()) != nil {
				return
			}
			select {
			case <-pod.pingChan:
			case <-PodClock.After(pingTimeout):
				if timeoutCallback != nil || !timeoutCallback() {
					return
				}
			case <-pod.closeChan:
				return
			}
		case <-pod.closeChan:
			return
		}
	}
}
