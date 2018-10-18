package main

import (
	"fmt"
	log "github.com/domac/kenl-proxy/logger"
	"github.com/domac/kenl-proxy/protocol"
	srv "github.com/domac/kenl-proxy/server"
	"os"
)

func main() {
	simpleProtocol := protocol.Simple()
	bufProtocol := protocol.Bufio(simpleProtocol, 64*1024, 64*1024)

	server, err := srv.Listen("tcp", "0.0.0.0:0", bufProtocol, 0, srv.DefaultHandlerFunc(sererSideHandle))
	if err != nil {
		log.GetLogger().Error(err)
		os.Exit(1)
	}
	if err != nil {
		log.GetLogger().Error(err)
		os.Exit(1)
	}

	addr := server.Listener().Addr().String()
	go server.Serve()

	session, err := srv.Dial("tcp", addr, bufProtocol, 0)

	clientSideHandle(session)
}

func clientSideHandle(session *srv.Session) {
	for i := 0; i < 3; i++ {
		msg := []byte("Boy")
		err := session.Send(msg)
		if err != nil {
			log.GetLogger().Error(err)
		}

		rep, err := session.Receive()
		if err != nil {
			log.GetLogger().Error(err)
		}
		log.GetLogger().Infof("recv data from server : %s\n", rep)
	}
}

func sererSideHandle(session *srv.Session) {
	for {
		req, err := session.Receive()
		if err != nil {
			log.GetLogger().Error(err)
		}

		reqStr := fmt.Sprintf("%s", req)
		session.Send([]byte("Hello," + reqStr))
	}
}
