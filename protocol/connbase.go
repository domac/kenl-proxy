package protocol

import (
	"encoding/binary"
	srv "github.com/domac/kenl-proxy/server"
	"io"
)

type SimpleProtocol struct {
}

func Simple() *SimpleProtocol {
	return &SimpleProtocol{}
}

func (s *SimpleProtocol) NewCodec(conn io.ReadWriter) (srv.Codec, error) {
	codec := &SimpleCodec{
		rw: conn.(io.ReadWriteCloser),
	}
	return codec, nil
}

type SimpleCodec struct {
	rw io.ReadWriteCloser
}

func (sc *SimpleCodec) Receive() (interface{}, error) {
	var head [2]byte
	_, err := io.ReadFull(sc.rw, head[:])
	if err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint16(head[:])
	buf := make([]byte, n)
	io.ReadFull(sc.rw, buf)
	if err != nil {
		return nil, err
	}
	return buf, nil
}

func (sc *SimpleCodec) Send(msg interface{}) error {
	var head [2]byte
	bodylen := len(msg.([]byte))
	binary.BigEndian.PutUint16(head[:], uint16(bodylen))

	_, err := sc.rw.Write(head[:])
	if err != nil {
		return err
	}

	_, err = sc.rw.Write(msg.([]byte))
	if err != nil {
		return err
	}
	return nil
}

func (sc *SimpleCodec) Close() error {
	return sc.rw.Close()
}
