package server

import (
	"errors"
	"sync"
	"sync/atomic"
)

var (
	SessClosedError  = errors.New("session was closed")
	SessBlockedError = errors.New("session was blocked")
	globalSessionId  uint64
)

const sessionChunksNum = 32

//连接会话
type Session struct {
	id        uint64
	codec     Codec
	manager   *SessionManager
	sendChan  chan interface{}
	sendMutex sync.RWMutex
	recvMutex sync.Mutex

	closeFlag  int32
	closeChan  chan int
	closeMutex sync.Mutex

	firstCloseCallback *closeCallback
	lastCloseCallback  *closeCallback

	State interface{}
}

func NewSession(codec Codec, sendChanSize int) *Session {
	return newSession(nil, codec, sendChanSize)
}

func newSession(manager *SessionManager, codec Codec, sendChanSize int) *Session {
	session := &Session{
		codec:     codec,
		manager:   manager,
		closeChan: make(chan int),
		id:        atomic.AddUint64(&globalSessionId, 1),
	}
	if sendChanSize > 0 {
		session.sendChan = make(chan interface{}, sendChanSize)
		go session.sendLoop()
	}
	return session
}

func (session *Session) ID() uint64 {
	return session.id
}

func (session *Session) IsClosed() bool {
	return atomic.LoadInt32(&session.closeFlag) == 1
}

func (session *Session) Close() error {
	if atomic.CompareAndSwapInt32(&session.closeFlag, 0, 1) {
		close(session.closeChan)

		if session.sendChan != nil {
			session.sendMutex.Lock()
			close(session.sendChan)
			if clear, ok := session.codec.(ClearSendChan); ok {
				clear.ClearSendChan(session.sendChan)
			}
			session.sendMutex.Unlock()
		}

		err := session.codec.Close()

		go func() {
			if session.manager != nil {
				session.manager.delSession(session)
			}
		}()
		return err
	}
	return SessClosedError
}

func (session *Session) Codec() Codec {
	return session.codec
}

func (session *Session) Receive() (interface{}, error) {
	session.recvMutex.Lock()
	defer session.recvMutex.Unlock()

	msg, err := session.codec.Receive()
	if err != nil {
		session.Close()
	}
	return msg, err
}

func (session *Session) sendLoop() {
	defer session.Close()
	for {
		select {
		case msg, ok := <-session.sendChan:
			if !ok || session.codec.Send(msg) != nil {
				return
			}
		case <-session.closeChan:
			return
		}
	}
}

func (session *Session) Send(msg interface{}) error {
	if session.sendChan == nil {
		if session.IsClosed() {
			return SessClosedError
		}

		session.sendMutex.Lock()
		defer session.sendMutex.Unlock()

		err := session.codec.Send(msg)
		if err != nil {
			session.Close()
		}
		return err
	}

	session.sendMutex.RLock()
	if session.IsClosed() {
		session.sendMutex.RUnlock()
		return SessClosedError
	}

	select {
	case session.sendChan <- msg:
		session.sendMutex.RUnlock()
		return nil
	default:
		session.sendMutex.RUnlock()
		session.Close()
		return SessClosedError
	}
}

type closeCallback struct {
	Handler interface{}
	Key     interface{}
	Func    func()
	Next    *closeCallback
}

func (session *Session) AddCloseCallback(handler, key interface{}, callback func()) {
	if session.IsClosed() {
		return
	}

	session.closeMutex.Lock()
	defer session.closeMutex.Unlock()

	newItem := &closeCallback{handler, key, callback, nil}

	if session.firstCloseCallback == nil {
		session.firstCloseCallback = newItem
	} else {
		session.lastCloseCallback.Next = newItem
	}
	session.lastCloseCallback = newItem
}

func (session *Session) RemoveCloseCallback(handler, key interface{}) {
	if session.IsClosed() {
		return
	}

	session.closeMutex.Lock()
	defer session.closeMutex.Unlock()

	var prev *closeCallback
	for callback := session.firstCloseCallback; callback != nil; prev, callback = callback, callback.Next {
		if callback.Handler == handler && callback.Key == key {
			if session.firstCloseCallback == callback {
				session.firstCloseCallback = callback.Next
			} else {
				prev.Next = callback.Next
			}
			if session.lastCloseCallback == callback {
				session.lastCloseCallback = prev
			}
			return
		}
	}
}

func (session *Session) invokeCloseCallbacks() {
	session.closeMutex.Lock()
	defer session.closeMutex.Unlock()

	for callback := session.firstCloseCallback; callback != nil; callback = callback.Next {
		callback.Func()
	}
}

/** ******************** session 管理 ******************** **/

type sessionSlot struct {
	sync.RWMutex
	sessions map[uint64]*Session
	disposed bool
}

type SessionManager struct {
	sessionChunks [sessionChunksNum]sessionSlot
	disposeOnce   sync.Once
	disposeWait   sync.WaitGroup
}

func NewSessionManager() *SessionManager {
	manager := &SessionManager{}
	for i := 0; i < len(manager.sessionChunks); i++ {
		manager.sessionChunks[i].sessions = make(map[uint64]*Session)
	}
	return manager
}

func (manager *SessionManager) Dispose() {
	manager.disposeOnce.Do(func() {
		for i := 0; i < sessionChunksNum; i++ {
			chunk := &manager.sessionChunks[i]
			chunk.Lock()
			chunk.disposed = true
			for _, session := range chunk.sessions {
				session.Close()
			}
			chunk.Unlock()
		}
		manager.disposeWait.Wait()
	})
}

func (manager *SessionManager) NewSession(codec Codec, sendChanSize int) *Session {
	session := newSession(manager, codec, sendChanSize)
	manager.putSession(session)
	return session
}

func (manager *SessionManager) GetSession(sessionID uint64) *Session {
	chunk := &manager.sessionChunks[sessionID%sessionChunksNum]
	chunk.RLock()
	defer chunk.RUnlock()

	session, _ := chunk.sessions[sessionID]
	return session
}

func (manager *SessionManager) putSession(session *Session) {
	chunk := &manager.sessionChunks[session.id%sessionChunksNum]

	chunk.Lock()
	defer chunk.Unlock()

	if chunk.disposed {
		session.Close()
		return
	}

	chunk.sessions[session.id] = session
	manager.disposeWait.Add(1)
}

func (manager *SessionManager) delSession(session *Session) {
	chunk := &manager.sessionChunks[session.id%sessionChunksNum]

	chunk.Lock()
	defer chunk.Unlock()

	delete(chunk.sessions, session.id)
	manager.disposeWait.Done()
}
