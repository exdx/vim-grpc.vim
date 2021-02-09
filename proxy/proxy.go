package proxy

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"net"
	"sync"

	"github.com/golang/protobuf/jsonpb"
	"github.com/ldelossa/vim-grpc.vim/channel"
	"github.com/ldelossa/vim-grpc.vim/proto"
	pb "github.com/ldelossa/vim-grpc.vim/proto"
)

const (
	Network     = "tcp4"
	DefaultPort = 7999
)

type ProxyOption func(*Proxy) error

// Proxy implements our gRPC client <-> Vim
// multiplexing proxy.
//
// The Proxy implements our client facing Proxy gRPC
// service along with listening on a Vim facing TCP
// socket.
type Proxy struct {
	pb.UnimplementedProxyServer
	sync.RWMutex
	channel *channel.Channel
}

// Listen will create a TCP socket for
// Vim to connect to.
//
// Once a connection is made a channel.Channel
// will be created from the net.TCPConn
//
// Any gRPC requests made while a Channel is not
// initialized will error.
func (p *Proxy) Listen(ctx context.Context) error {
	listener, err := net.ListenTCP(Network, &net.TCPAddr{
		Port: DefaultPort,
	})
	if err != nil {
		return err
	}

	for {
		conn, err := listener.AcceptTCP()
		if err != nil {
			log.Printf("err tcp during connection: %v", err)
			continue
		}
		log.Printf("proxy: received new connect")

		p.Lock()
		p.channel = channel.NewChannel(conn)
		p.Unlock()

		log.Printf("proxy: channel connected")
		// kick off recv side of channel
		go p.channel.Recv(ctx)
		// blocks until ctx is canceled or an underlying
		// tcp error is detected.
		p.channel.Ping(ctx)
		log.Printf("proxy: channel disconnected")
	}
}

func (p *Proxy) channelUp() bool {
	var ok bool
	p.RLock()
	ok = (p.channel != nil)
	p.Unlock()
	return ok
}

func (p *Proxy) GetBufInfo(ctx context.Context, req *proto.GetBufInfoRequest) (*proto.GetBufInfoResponse, error) {
	const (
		RPC = "getBufInfo"
	)

	if ok := p.channelUp(); !ok {
		return nil, fmt.Errorf("vim channel is not initialized")
	}

	e := channel.Envelope{
		RPC:  "getBufInfo",
		Body: []byte{},
	}
	m := jsonpb.Marshaler{
		EmitDefaults: true,
	}
	m.Marshal(bytes.NewBuffer(e.Body), req)

	delivery, err := p.channel.Send(&e)
	if err != nil {
		return nil, err
	}

	e, err = delivery.Wait(ctx)
	if err != nil {
		return nil, err
	}

	resp := &proto.GetBufInfoResponse{}
	err = jsonpb.Unmarshal(bytes.NewReader(e.Body), resp)
	if err != nil {
		return nil, err
	}
	return resp, nil
}
