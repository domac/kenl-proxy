package server

import (
	"sync"
)

type SessionHub struct {
	mutex    sync.RWMutex
	sessions map[uint32]*Session
	State    interface{}
}

func NewSessionHub() *SessionHub {
	return &SessionHub{
		sessions: make(map[uint32]*Session),
	}
}

func (sessionhub *SessionHub) Len() int {
	sessionhub.mutex.RLock()
	defer sessionhub.mutex.RUnlock()
	return len(sessionhub.sessions)
}

func (sessionhub *SessionHub) Fetch(callback func(*Session)) {
	sessionhub.mutex.RLock()
	defer sessionhub.mutex.RUnlock()
	for _, session := range sessionhub.sessions {
		callback(session)
	}
}

func (sessionhub *SessionHub) Get(key uint32) *Session {
	sessionhub.mutex.RLock()
	defer sessionhub.mutex.RUnlock()
	session, _ := sessionhub.sessions[key]
	return session
}

func (sessionhub *SessionHub) Put(key uint32, session *Session) {
	sessionhub.mutex.Lock()
	defer sessionhub.mutex.Unlock()
	if session, exists := sessionhub.sessions[key]; exists {
		sessionhub.remove(key, session)
	}
	session.AddCloseCallback(sessionhub, key, func() {
		sessionhub.Remove(key)
	})
	sessionhub.sessions[key] = session
}

func (sessionhub *SessionHub) remove(key uint32, session *Session) {
	//使用回调
	session.RemoveCloseCallback(sessionhub, key)
	delete(sessionhub.sessions, key)
}

func (sessionhub *SessionHub) Remove(key uint32) bool {
	sessionhub.mutex.Lock()
	defer sessionhub.mutex.Unlock()
	session, exists := sessionhub.sessions[key]
	if exists {
		sessionhub.remove(key, session)
	}
	return exists
}

func (sessionhub *SessionHub) FetchAndRemove(callback func(*Session)) {
	sessionhub.mutex.Lock()
	defer sessionhub.mutex.Unlock()
	for key, session := range sessionhub.sessions {
		sessionhub.remove(key, session)
		callback(session)
	}
}

func (sessionhub *SessionHub) Close() {
	sessionhub.mutex.Lock()
	defer sessionhub.mutex.Unlock()
	for key, session := range sessionhub.sessions {
		sessionhub.remove(key, session)
	}
}
