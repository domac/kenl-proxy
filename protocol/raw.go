package protocol

import (
	srv "github.com/domac/kenl-proxy/server"
	"io"
	"net"
)

type RawProtocol struct {
	bufSize int
}

func Raw(size int) *RawProtocol {
	return &RawProtocol{
		bufSize: size,
	}
}

func (r *RawProtocol) GetBuffSize() int {
	return r.bufSize
}

func (r *RawProtocol) NewCodec(conn io.ReadWriter) (srv.Codec, error) {
	codec := &RawCodec{
		bufSize: r.bufSize,
		conn:    conn.(net.Conn),
	}
	return codec, nil
}

type RawCodec struct {
	bufSize int
	conn    net.Conn
}

func (rc *RawCodec) Receive() (interface{}, error) {
	buf := make([]byte, rc.bufSize)
	n, err := rc.conn.Read(buf)
	if err != nil {
		return nil, err
	}
	return buf[:n], nil
}

func (rc *RawCodec) Send(msg interface{}) (err error) {
	_, err = rc.conn.Write(msg.([]byte))
	return
}

func (rc *RawCodec) Close() error {
	return rc.conn.(io.ReadWriteCloser).Close()
}
