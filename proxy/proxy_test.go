package proxy

import (
	srv "github.com/domac/kenl-proxy/server"
	bp "github.com/tevid/go-tevid-utils/bytes_pool"
	. "github.com/tevid/gohamcrest"
	"math/rand"
	"net"
	"runtime"
	"sync"
	"testing"
	"time"
)

var (
	TestMaxConn      = 10000
	TestMaxPacket    = 2048
	TestBuffsize     = 1024
	TestSendChanSize = int(runtime.GOMAXPROCS(-1) * 6000)
	TestRecvChanSize = 6000
	TestIdleTimeout  = time.Second * 2
	TestPingInterval = time.Second
	TestPingTimeout  = time.Second
	TestAuthKey      = "123"
	TestServerId     = uint32(123)
)

var TestProxyCfg = ProxyConfig{
	MaxConn:          TestMaxConn,
	MsgBuffSize:      TestBuffsize,
	MsgSendChanSize:  TestSendChanSize,
	ProxyIdleTimeout: TestIdleTimeout,
	AuthKey:          TestAuthKey,
}

var TestPodConfig = PodConfig{
	pool:            TestPool,
	MaxPacket:       TestMaxPacket,
	MsgBufferSize:   TestBuffsize,
	MsgSendChanSize: TestSendChanSize,
	MsgRecvChanSize: TestRecvChanSize,
	PingInterval:    TestPingInterval,
	PingTimeout:     TestPingTimeout,
	ServerId:        TestServerId,
	AuthKey:         TestAuthKey,
	MsgFormat:       &TestMsgFormat{},
}

var TestPool = bp.NewBytesPool(64, 64*1024, 256*1024)

type TestMsgFormat struct {
}

func (f *TestMsgFormat) EncodeMessage(msg interface{}) ([]byte, error) {
	buf := make([]byte, len(msg.([]byte)))
	copy(buf, msg.([]byte))
	return buf, nil
}

func (f *TestMsgFormat) DecodeMessage(msg []byte) (interface{}, error) {
	buf := make([]byte, len(msg))
	copy(buf, msg)
	return buf, nil
}

//---------------- proxy test ---------

func TestProxy(t *testing.T) {
	listener1, err := net.Listen("tcp", "127.0.0.1:0")
	Assert(t, err, NilVal())
	defer listener1.Close()

	listener2, err := net.Listen("tcp", "127.0.0.1:0")
	Assert(t, err, NilVal())
	defer listener2.Close()

	pool := TestPool

	proxy := NewProxy(pool, TestMaxPacket)

	go proxy.ServeClients(listener1, TestProxyCfg)
	go proxy.ServeServers(listener2, TestProxyCfg)

	time.Sleep(time.Second)

	client, err := DialClient("tcp", listener1.Addr().String(), TestPodConfig)
	Assert(t, err, NilVal())

	server, err := DialServer("tcp", listener2.Addr().String(), TestPodConfig)
	Assert(t, err, NilVal())

	OUTCOME := 0
	INCOME := 1

	CLIENT_PEER := 0
	SERVER_PEER := 1

L:
	for {
		n := 0
		for i := 0; i < len(proxy.sessionhubs); i++ {
			n += proxy.sessionhubs[i][CLIENT_PEER].Len()
			n += proxy.sessionhubs[i][SERVER_PEER].Len()
			if n >= 2 {
				break L
			}
		}
		runtime.Gosched()
	}

	var sesses [2][2]*srv.Session
	var acceptChan = [2]chan int{
		make(chan int),
		make(chan int),
	}

	go func() {
		var err error

		//server-input
		sesses[CLIENT_PEER][OUTCOME], err = server.Accept()
		Assert(t, err, NilVal())
		acceptChan[CLIENT_PEER] <- 1

		sesses[SERVER_PEER][OUTCOME], err = client.Accept()
		Assert(t, err, NilVal())
		acceptChan[SERVER_PEER] <- 1

	}()

	connId := uint32(123)

	sesses[CLIENT_PEER][INCOME], err = client.Dial(connId)
	Assert(t, err, NilVal())
	<-acceptChan[CLIENT_PEER]

	sesses[SERVER_PEER][INCOME], err = server.Dial(sesses[CLIENT_PEER][OUTCOME].State.(*ConnInfo).RemoteID())
	Assert(t, err, NilVal())
	<-acceptChan[SERVER_PEER]

	Assert(t, sesses[CLIENT_PEER][OUTCOME].State.(*ConnInfo).ConnID(), Equal(sesses[CLIENT_PEER][INCOME].State.(*ConnInfo).ConnID()))
	Assert(t, sesses[SERVER_PEER][OUTCOME].State.(*ConnInfo).ConnID(), Equal(sesses[SERVER_PEER][INCOME].State.(*ConnInfo).ConnID()))

	for i := 0; i < 1000; i++ {
		buffer1 := make([]byte, 1024)

		for i := 0; i < len(buffer1); i++ {
			buffer1[i] = byte(rand.Intn(256))
		}

		x := rand.Intn(2)
		y := rand.Intn(2)

		err := sesses[x][y].Send(buffer1)
		Assert(t, err, NilVal())

		buffer2, err := sesses[x][(y+1)%2].Receive()
		Assert(t, err, NilVal())

		Assert(t, buffer1, Equal(buffer2))
	}

	sesses[CLIENT_PEER][OUTCOME].Close()
	sesses[CLIENT_PEER][INCOME].Close()
	sesses[SERVER_PEER][OUTCOME].Close()
	sesses[SERVER_PEER][INCOME].Close()

	time.Sleep(time.Second)

	Assert(t, 0, Equal(client.sessionhub.Len()))
	Assert(t, 0, Equal(server.sessionhub.Len()))

	for i := 0; i < len(proxy.vConns); i++ {
		proxy.vConnMutex[i].Lock()
		Assert(t, 0, Equal(len(proxy.vConns[i])))
		proxy.vConnMutex[i].Unlock()
	}

	proxy.Stop()
}

func TestProxyParallel(t *testing.T) {
	listener1, err := net.Listen("tcp", "127.0.0.1:0")
	Assert(t, err, NilVal())
	defer listener1.Close()

	listener2, err := net.Listen("tcp", "127.0.0.1:0")
	Assert(t, err, NilVal())
	defer listener2.Close()

	pool := TestPool

	proxy := NewProxy(pool, TestMaxPacket)

	go proxy.ServeClients(listener1, TestProxyCfg)
	go proxy.ServeServers(listener2, TestProxyCfg)

	time.Sleep(time.Second)

	client, err := DialClient("tcp", listener1.Addr().String(), TestPodConfig)
	Assert(t, err, NilVal())

	server, err := DialServer("tcp", listener2.Addr().String(), TestPodConfig)
	Assert(t, err, NilVal())

L:
	for {
		n := 0
		for i := 0; i < len(proxy.sessionhubs); i++ {
			n += proxy.sessionhubs[i][0].Len()
			n += proxy.sessionhubs[i][1].Len()
			if n >= 2 {
				break L
			}
		}
		time.Sleep(time.Second)
	}

	go func() {
		for {
			sess, err := server.Accept()
			if err != nil {
				return
			}
			go func() {
				for {
					msg, err := sess.Receive()
					if err != nil {
						return
					}
					if err := sess.Send(msg); err != nil {
						return
					}
				}
			}()
		}
	}()

	var wg sync.WaitGroup
	var errors = make([]error, runtime.GOMAXPROCS(-1))
	var errorInfos = make([]string, len(errors))

	for i := 0; i < len(errors); i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()

			var sess *srv.Session
			sess, errors[n] = client.Dial(123)
			if errors[n] != nil {
				errorInfos[n] = "dial"
				return
			}
			defer sess.Close()

			times := 3000 + rand.Intn(3000)
			for i := 0; i < times; i++ {
				buffer1 := make([]byte, 1024)
				for i := 0; i < len(buffer1); i++ {
					buffer1[i] = byte(rand.Intn(256))
				}

				errors[n] = sess.Send(buffer1)
				if errors[n] != nil {
					errorInfos[n] = "send"
					return
				}

				var buffer2 interface{}
				buffer2, errors[n] = sess.Receive()
				if errors[n] != nil {
					errorInfos[n] = "receive"
					return
				}
				Assert(t, buffer1, Equal(buffer2.([]byte)))
			}
		}(i)
	}
	wg.Wait()
	time.Sleep(time.Second)

	var failed bool
	for i := 0; i < len(errors); i++ {
		if !failed && errors[i] != nil {
			failed = true
			println(i, errorInfos[i], errors[i].Error())
		}
	}

	Assert(t, 0, Equal(client.sessionhub.Len()))
	Assert(t, 0, Equal(server.sessionhub.Len()))

	for i := 0; i < len(proxy.vConns); i++ {
		proxy.vConnMutex[i].Lock()
		Assert(t, 0, Equal(len(proxy.vConns[i])))
		proxy.vConnMutex[i].Unlock()
	}

	proxy.Stop()
}

func TestVConnsCloseAndReceive(t *testing.T) {
	lsn1, err := net.Listen("tcp", "127.0.0.1:0")
	Assert(t, err, NilVal())
	defer lsn1.Close()

	lsn2, err := net.Listen("tcp", "127.0.0.1:0")
	Assert(t, err, NilVal())
	defer lsn2.Close()

	pool := TestPool
	proxy := NewProxy(pool, TestMaxPacket)

	go proxy.ServeClients(lsn1, TestProxyCfg)
	go proxy.ServeServers(lsn2, TestProxyCfg)

	time.Sleep(time.Second)

	server, err := DialServer("tcp", lsn2.Addr().String(), TestPodConfig)
	Assert(t, err, NilVal())
	time.Sleep(time.Second)
	go func() {
		for {
			vconn, err := server.Accept()
			if err != nil {
				return
			}
			runtime.Gosched()
			vconn.Close()
			runtime.Gosched()
		}
	}()
	time.Sleep(time.Second)
	payload := make([]byte, 10)
	for i := 0; i < 10000; i++ {
		client, err := DialClient("tcp", lsn1.Addr().String(), TestPodConfig)
		Assert(t, err, NilVal())
		vconn, err := client.Dial(123)
		Assert(t, err, NilVal())
		runtime.Gosched()
		vconn.Send(payload)
		runtime.Gosched()
		client.Close()
	}
	proxy.Stop()
}
