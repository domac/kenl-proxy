package main

import (
	"github.com/domac/kenl-proxy/protocol"
	srv "github.com/domac/kenl-proxy/server"
	"log"
)

type AddReq struct {
	A, B int
}

func main() {
	jsonProtocol := protocol.Json()
	jsonProtocol.Register(AddReq{})

	server, err := srv.Listen("tcp", "0.0.0.0:0", jsonProtocol, 0, srv.DefaultHandlerFunc(sererSideHandle))
	checkErr(err)
	addr := server.Listener().Addr().String()
	go server.Serve()

	client, err := srv.Dial("tcp", addr, jsonProtocol, 0)
	checkErr(err)
	clientSideHandle(client)
}

func sererSideHandle(session *srv.Session) {
	for {
		req, err := session.Receive()
		checkErr(err)
		err = session.Send(req.(*AddReq).A + req.(*AddReq).B)
		checkErr(err)
	}
}

func clientSideHandle(session *srv.Session) {
	for i := 0; i < 10; i++ {
		err := session.Send(&AddReq{
			i, i,
		})
		checkErr(err)
		log.Printf("Send: %d + %d", i, i)

		rsp, err := session.Receive()
		checkErr(err)
		log.Printf("Receive: %v", rsp)
	}
}

func checkErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
