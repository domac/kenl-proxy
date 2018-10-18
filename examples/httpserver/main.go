package main

import (
	"fmt"
	log "github.com/domac/kenl-proxy/logger"
	"github.com/domac/kenl-proxy/protocol"
	srv "github.com/domac/kenl-proxy/server"
	"os"
)

func main() {
	httpProtocol := protocol.Http()
	server, err := srv.Listen("tcp", "0.0.0.0:8080", httpProtocol, 0, srv.DefaultHandlerFunc(sererSideHandle))
	if err != nil {
		log.GetLogger().Error(err)
		os.Exit(1)
	}
	server.Serve()
}

func sererSideHandle(session *srv.Session) {
	req, err := session.Receive()
	if err != nil {
		log.GetLogger().Error(err)
	}
	fmt.Println(req)
	session.Send([]byte("hi"))
}
