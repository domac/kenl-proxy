package protocol

import (
	"bufio"
	srv "github.com/domac/kenl-proxy/server"
	"io"
	"net"
	"net/http"
	"net/url"
)

type HttpProtocol struct {
}

func Http() *HttpProtocol {
	return &HttpProtocol{}
}

func (h *HttpProtocol) NewCodec(conn io.ReadWriter) (srv.Codec, error) {
	codec := &HttpCodec{
		rw:     conn.(net.Conn),
		reader: bufio.NewReader(conn),
	}
	return codec, nil
}

type HttpCodec struct {
	rw     net.Conn
	reader *bufio.Reader
}

func (hc *HttpCodec) Receive() (msg interface{}, err error) {
	req, err := http.ReadRequest(hc.reader)
	if err != nil {
		if e, ok := err.(net.Error); ok {
			return nil, e
		}
	}
	args, _ := url.ParseQuery(req.URL.RawQuery)
	cmd := args.Get("cmd")
	return cmd, nil
}

func (hc *HttpCodec) Send(msg interface{}) error {
	_, err := hc.rw.Write(msg.([]byte))
	return err
}

func (sc *HttpCodec) Close() error {
	return sc.rw.Close()
}
