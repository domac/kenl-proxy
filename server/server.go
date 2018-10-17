package server

import (
	"io"
	"net"
	"strings"
	"time"
)

// encode/decode interface
type Codec interface {
	Receive() (interface{}, error)
	Send(interface{}) error
	Close() error
}

type Protocol interface {
	NewCodec(rw io.ReadWriter) (Codec, error)
}

type ProtocolFunc func(rw io.ReadWriter) (Codec, error)

func (pf ProtocolFunc) NewCodec(rw io.ReadWriter) (Codec, error) {
	return pf(rw)
}

// 会话处理接口
type Handler interface {
	HandleSession(*Session)
}

var _ Handler = DefaultHandlerFunc(nil)

//默认会话处理接口
type DefaultHandlerFunc func(*Session)

func (f DefaultHandlerFunc) HandleSession(session *Session) {
	f(session)
}

type ClearSendChan interface {
	ClearSendChan(<-chan interface{})
}

/** 服务器结果 **/

type Server struct {
	manager      *SessionManager
	listener     net.Listener
	protocol     Protocol
	handler      Handler
	sendChanSize int
}

func NewServer(listener net.Listener, protocol Protocol, sendChanSize int, handler Handler) *Server {

	return &Server{
		manager:      NewSessionManager(),
		listener:     listener,
		protocol:     protocol,
		handler:      handler,
		sendChanSize: sendChanSize,
	}
}

func (server *Server) Listener() net.Listener {
	return server.listener
}

func (server *Server) Serve() error {
	for {
		conn, err := Accept(server.listener)
		if err != nil {
			return err
		}
		go server.handleConnection(conn)
	}
}

func (server *Server) handleConnection(conn net.Conn) {
	codec, err := server.protocol.NewCodec(conn)
	if err != nil {
		conn.Close()
		return
	}
	session := server.manager.NewSession(codec, server.sendChanSize)
	server.handler.HandleSession(session)
}

func (server *Server) GetSession(sessionID uint64) *Session {
	return server.manager.GetSession(sessionID)
}

func (server *Server) Stop() {
	server.listener.Close()
	server.manager.Dispose()
}

func Listen(network, address string, protocol Protocol, sendChanSize int, handler Handler) (*Server, error) {
	listener, err := net.Listen(network, address)
	if err != nil {
		return nil, err
	}
	return NewServer(listener, protocol, sendChanSize, handler), nil
}

func Dial(network, address string, protocol Protocol, sendChanSize int) (*Session, error) {
	conn, err := net.Dial(network, address)
	if err != nil {
		return nil, err
	}
	codec, err := protocol.NewCodec(conn)
	if err != nil {
		return nil, err
	}
	return NewSession(codec, sendChanSize), nil
}

//----------- api --------------

func DialTimeout(network, address string, timeout time.Duration, protocol Protocol, sendChanSize int) (*Session, error) {
	conn, err := net.DialTimeout(network, address, timeout)
	if err != nil {
		return nil, err
	}
	codec, err := protocol.NewCodec(conn)
	if err != nil {
		return nil, err
	}
	return NewSession(codec, sendChanSize), nil
}

func Accept(listener net.Listener) (net.Conn, error) {
	var tempDelay time.Duration
	for {
		conn, err := listener.Accept()
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				if tempDelay == 0 {
					tempDelay = 5 * time.Millisecond
				} else {
					tempDelay *= 2
				}
				if max := 1 * time.Second; tempDelay > max {
					tempDelay = max
				}
				time.Sleep(tempDelay)
				continue
			}
			if strings.Contains(err.Error(), "use of closed network connection") {
				return nil, io.EOF
			}
			return nil, err
		}
		return conn, nil
	}
}
