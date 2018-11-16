package proxy

import (
	"bufio"
	"bytes"
	"crypto/md5"
	"crypto/rand"
	"encoding/binary"
	"errors"
	log "github.com/domac/kenl-proxy/logger"
	srv "github.com/domac/kenl-proxy/server"
	pool "github.com/tevid/go-tevid-utils/bytes_pool"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

const (
	SizeofLen   = 4
	CmdTypeSize = 1
	CmdIdSize   = 4

	//命令头长度 = len(id + type)
	CmdHeadSize = CmdIdSize + CmdTypeSize
	//命令字长度
	START_CMD_CONNID = SizeofLen
	START_CMD_TYPE   = START_CMD_CONNID + CmdIdSize
	START_ARGS       = START_CMD_TYPE + CmdTypeSize
)

var ErrTooLargePacket = errors.New("too large packet size")
var ErrServerAuthFail = errors.New("server auth fail")

// -------------  Protocol 定义 -------------

type proxyProtocol struct {
	pool          *pool.BytesPool
	maxPacketSize int
}

func (p *proxyProtocol) newCodec(id uint32, conn net.Conn, bufferSize int) *proxyCodec {
	pc := &proxyCodec{
		id:            id,
		proxyProtocol: p,
		conn:          conn,
		reader:        bufio.NewReaderSize(conn, bufferSize), //读缓存区读取器
	}
	pc.headerBuf = pc.headDat[:]
	return pc
}

func (p *proxyProtocol) allocCmd(t byte, allocSize int) []byte {
	buff := p.alloc(SizeofLen + allocSize)
	binary.BigEndian.PutUint32(buff, uint32(allocSize))    //伪分配一个header
	binary.BigEndian.PutUint32(buff[START_CMD_CONNID:], 0) //分配一个 connId = 0
	buff[START_CMD_TYPE] = t                               //分配一个 cmd byte
	return buff
}

func (p *proxyProtocol) decodePacket(msg []byte) (connId uint32) {
	//从包含connId的位置开始获取数据
	return binary.BigEndian.Uint32(msg[START_CMD_CONNID:])
}

func (p *proxyProtocol) decodeCmd(msg []byte) byte {
	return msg[START_CMD_TYPE]
}

func (p *proxyProtocol) alloc(allocsize int) []byte {
	return p.pool.Alloc(allocsize)
}

func (p *proxyProtocol) free(msg []byte) {
	p.pool.Release(msg)
}

func (p *proxyProtocol) send(session *srv.Session, buf []byte) error {
	err := session.Send(buf)
	if err != nil {
		log.GetLogger().Errorf("proxy protocol send error: %v", err)
		session.Close()
	}
	return err

}
func (p *proxyProtocol) sendv(session *srv.Session, buf [][]byte) error {
	err := session.Send(buf)
	if err != nil {
		log.GetLogger().Errorf("proxy protocol send error: %v", err)
		session.Close()
	}
	return err
}

// ------------- Codec 定义 -------------

type proxyCodec struct {
	*proxyProtocol
	id        uint32
	conn      net.Conn
	reader    *bufio.Reader
	headerBuf []byte
	headDat   [SizeofLen]byte
}

//数据接收
func (c *proxyCodec) Receive() (interface{}, error) {
	//先把头信息读处理
	//头信息包含 length
	if _, err := io.ReadFull(c.reader, c.headerBuf); err != nil {
		return nil, err
	}

	//因为采用大端序作为默认字节序，所以通过大端序获取消息长度

	length := int(binary.BigEndian.Uint32(c.headerBuf))
	if length > c.maxPacketSize {
		return nil, ErrTooLargePacket
	}

	buff := c.alloc(length + SizeofLen)
	//把头信息写回去
	copy(buff, c.headerBuf)

	if _, err := io.ReadFull(c.reader, buff[SizeofLen:]); err != nil {
		c.free(buff)
		return nil, err
	}
	//返回地址，以防后续跟进这个指针回收
	return &buff, nil

}

//数据发送
func (c *proxyCodec) Send(msg interface{}) error {
	if buff, ok := msg.([][]byte); ok {
		nb := net.Buffers(buff)
		_, err := nb.WriteTo(c.conn)
		return err
	}
	_, err := c.conn.Write(msg.([]byte))
	return err
}

//链接关闭
func (c *proxyCodec) Close() error {
	return c.conn.Close()
}

// ===================== 授权验证 ===============================

//服务运行初始化
func (p *proxyProtocol) serverInit(conn net.Conn, serverId uint32, key []byte) error {

	var buf [md5.Size + CmdIdSize]byte

	conn.SetDeadline(time.Now().Add(5 * time.Second))

	if _, err := io.ReadFull(conn, buf[:8]); err != nil {
		conn.Close()
		return err
	}

	hash := md5.New()
	hash.Write(buf[:8])
	hash.Write(key)
	verify := hash.Sum(nil)

	//buf复用
	copy(buf[:md5.Size], verify)
	binary.BigEndian.PutUint32(buf[md5.Size:], serverId)

	if _, err := conn.Write(buf[:]); err != nil {
		conn.Close()
		return err
	}
	conn.SetDeadline(time.Time{})
	return nil
}

//服务验证
func (p *proxyProtocol) serverAuth(conn net.Conn, key []byte) (uint32, error) {
	var buf [md5.Size + CmdIdSize]byte
	conn.SetDeadline(time.Now().Add(5 * time.Second))

	rand.Read(buf[:8])
	if _, err := conn.Write(buf[:8]); err != nil {
		conn.Close()
		return 0, err
	}

	hash := md5.New()
	hash.Write(buf[:8])
	hash.Write(key)
	verify := hash.Sum(nil)

	if _, err := io.ReadFull(conn, buf[:]); err != nil {
		conn.Close()
		return 0, err
	}

	if !bytes.Equal(verify, buf[:md5.Size]) {
		conn.Close()
		return 0, ErrServerAuthFail
	}
	conn.SetDeadline(time.Time{})
	return binary.BigEndian.Uint32(buf[md5.Size:]), nil

}

// =====================  connId = 0 的命令处理   ==============================

//-------- dialCmd -----------
const (
	dialCmd              = 0
	dialCmdSize          = CmdHeadSize + CmdIdSize
	startDialCmdRemoteId = START_ARGS
)

func (p *proxyProtocol) decodeDialCmd(data []byte) (remoteID uint32) {
	return binary.BigEndian.Uint32(data[startDialCmdRemoteId:])
}

func (p *proxyProtocol) encodeDialCmd(remoteID uint32) []byte {
	buffer := p.allocCmd(dialCmd, dialCmdSize)
	binary.BigEndian.PutUint32(buffer[startDialCmdRemoteId:], remoteID)
	return buffer
}

//-------- acceptCmd -----------
const (
	acceptCmd              = 1
	acceptCmdSize          = CmdHeadSize + CmdIdSize*2
	startAcceptCmdConnId   = START_ARGS
	startAcceptCmdRemoteId = startAcceptCmdConnId + CmdIdSize
)

func (p *proxyProtocol) decodeAcceptCmd(data []byte) (connId, remoteId uint32) {
	connId = binary.BigEndian.Uint32(data[startAcceptCmdConnId:])
	remoteId = binary.BigEndian.Uint32(data[startAcceptCmdRemoteId:])
	return
}

func (p *proxyProtocol) encodeAcceptCmd(connId, remoteId uint32) []byte {
	buffer := p.allocCmd(acceptCmd, acceptCmdSize)
	binary.BigEndian.PutUint32(buffer[startAcceptCmdConnId:], connId)
	binary.BigEndian.PutUint32(buffer[startAcceptCmdRemoteId:], remoteId)
	return buffer
}

//-------- connectCmd -----------

const (
	connectCmd              = 2
	connectCmdSize          = CmdHeadSize + CmdIdSize*2
	startConnectCmdConnId   = START_ARGS
	startConnectCmdRemoteId = startConnectCmdConnId + CmdIdSize
)

func (p *proxyProtocol) decodeConnectCmd(data []byte) (connId, remoteId uint32) {
	connId = binary.BigEndian.Uint32(data[startConnectCmdConnId:])
	remoteId = binary.BigEndian.Uint32(data[startConnectCmdRemoteId:])
	return
}

func (p *proxyProtocol) encodeConnectCmd(connId, remoteId uint32) []byte {
	buffer := p.allocCmd(connectCmd, connectCmdSize)
	binary.BigEndian.PutUint32(buffer[startConnectCmdConnId:], connId)
	binary.BigEndian.PutUint32(buffer[startConnectCmdRemoteId:], remoteId)
	return buffer
}

//-------- refuseCmd -----------

const (
	refuseCmd              = 3
	refuseCmdSize          = CmdHeadSize + CmdIdSize
	startRefuseCmdRemoteId = START_ARGS
)

func (p *proxyProtocol) decodeRefuseCmd(data []byte) (remoteId uint32) {
	remoteId = binary.BigEndian.Uint32(data[startRefuseCmdRemoteId:])
	return
}

func (p *proxyProtocol) encodeRefuseCmd(remoteId uint32) []byte {
	buffer := p.allocCmd(refuseCmd, refuseCmdSize)
	binary.BigEndian.PutUint32(buffer[startRefuseCmdRemoteId:], remoteId)
	return buffer
}

//-------- closedCmd -----------

const (
	closeCmd            = 4
	closeCmdSize        = CmdHeadSize + CmdIdSize
	startCloseCmdConnId = START_ARGS
)

func (p *proxyProtocol) decodeCloseCmd(data []byte) (connId uint32) {
	connId = binary.BigEndian.Uint32(data[startCloseCmdConnId:])
	return
}

func (p *proxyProtocol) encodeCloseCmd(connId uint32) []byte {
	buffer := p.allocCmd(closeCmd, closeCmdSize)
	binary.BigEndian.PutUint32(buffer[startCloseCmdConnId:], connId)
	return buffer
}

//-------- pingCmd -----------

const (
	pingCmd     = 5
	pingCmdSize = CmdHeadSize
)

func (p *proxyProtocol) encodePingCmd() []byte {
	return p.allocCmd(pingCmd, pingCmdSize)
}

//------ 消息接口

type MsgFormat interface {
	DecodeMessage([]byte) (interface{}, error)
	EncodeMessage(interface{}) ([]byte, error)
}

//pod 的自定义codec
type virtualCodec struct {
	*proxyProtocol
	netConn     *srv.Session
	connId      uint32
	recvChan    chan []byte
	closeMutex  sync.Mutex
	closed      bool
	lastActived *int64
	format      MsgFormat
}

func (p *proxyProtocol) newVirtualCodec(netConn *srv.Session, connId uint32, recvChanSize int, lastActived *int64, format MsgFormat) *virtualCodec {
	return &virtualCodec{
		proxyProtocol: p,
		netConn:       netConn,
		connId:        connId,
		recvChan:      make(chan []byte, recvChanSize),
		lastActived:   lastActived,
		format:        format,
	}
}

func (v *virtualCodec) forward(buf []byte) {
	v.closeMutex.Lock()
	if v.closed {
		v.closeMutex.Unlock()
		v.free(buf)
		return
	}
	select {
	case v.recvChan <- buf:
		v.closeMutex.Unlock()
		return
	default:
		v.closeMutex.Unlock()
		v.Close()
		v.free(buf)
	}
}

func (v *virtualCodec) Receive() (interface{}, error) {
	buf, ok := <-v.recvChan
	if !ok {
		return nil, io.EOF
	}

	defer v.free(buf)
	return v.format.DecodeMessage(buf[START_CMD_CONNID+CmdIdSize:])
}

func (v *virtualCodec) Send(msg interface{}) error {
	data, err := v.format.EncodeMessage(msg)
	if err != nil {
		return err
	}

	if len(data) > v.maxPacketSize {
		return ErrTooLargePacket
	}

	headBuf := make([]byte, SizeofLen+CmdIdSize)
	binary.BigEndian.PutUint32(headBuf, uint32(CmdIdSize+len(data))) //设置长度
	binary.BigEndian.PutUint32(headBuf[START_CMD_CONNID:], v.connId) //设置connid

	buffers := make([][]byte, 2)
	buffers[0] = headBuf
	buffers[1] = data
	err = v.sendv(v.netConn, buffers)
	if err != nil {
		atomic.StoreInt64(v.lastActived, time.Now().Unix())
	}
	return err
}

func (v *virtualCodec) Close() error {
	v.closeMutex.Lock()
	if !v.closed {
		v.closed = true
		close(v.recvChan)
		v.send(v.netConn, v.encodeCloseCmd(v.connId))
	}
	v.closeMutex.Unlock()

	for buf := range v.recvChan {
		v.free(buf)
	}
	return nil
}
