package channel

import (
	"context"
	"encoding/json"
	"log"
	"net"
	"runtime"
	"sync/atomic"
	"time"
	"unsafe"
)

// Envelope wraps a json-encoded rpc message.
//
// When Vim receives an Envelope it will route
// the message to the appropriate vim-script handler.
//
// When the Channel receives an Envelope it
// will be delivered to the correct mailbox.
//
// The Envelope's Err field must be checked for
// any underlying tcp errors before assuming
// the Body is a valid json response.
type Envelope struct {
	BoxNum uint32          `json:"box_num"`
	RPC    string          `json:"rpc"`
	Body   json.RawMessage `json:"body"`
	Err    error           `json:"error"`
	In     bool            `json:"-"`
}

// Delivery provides a lock-free wait mechanism
// for clients issueing a channel.Send
//
// Delivery will check its Channel's mailbox periodically
// until an envelope is available.
type Delivery struct {
	channel Channel
	boxNum  uint32
}

func (d *Delivery) Wait(ctx context.Context) (env Envelope, err error) {
	for {
		if ctx.Err() != nil {
			return Envelope{}, ctx.Err()
		}

		e := (*Envelope)(*d.channel.mailbox[int(d.boxNum)])
		if e != nil && e.In {
			env = *e
			atomic.SwapPointer(d.channel.mailbox[int(env.BoxNum)], nil)
			return
		}

		runtime.Gosched()
	}
}

// Channel represents a Vim channel in JSON mode.
//
// A Channel maintains a mailbox where incoming RPC messages
// are placed, in the form of Envelope data structures.
//
// Each Delivery polls its assigned mailbox until an Envelope is present.
//
// In the occurence of a underlying TCP error the Channel delivers
// a sentinel Envelope indicating error.
type Channel struct {
	// all channel fields are pointers,
	// Channel(s) may be copied after construction.
	*json.Encoder
	*json.Decoder
	conn *net.TCPConn
	// *unsafe.Pointer will either be nil or *Envelope
	mailbox []*unsafe.Pointer
}

// Ping begins sending synethic rpc as a
// heartbeat with vim.
//
// Ping will close the connetion and return
// if an underlying tpc error is encountered.
//
// A caller may cancel the context to end the
// ping without closing the connection.
func (c Channel) Ping(ctx context.Context) {
	e := &Envelope{
		RPC: "ping",
	}
	t := time.NewTicker(1 * time.Second)
	for {
		select {
		case <-ctx.Done():
			log.Printf("channel: ctx cancled, closing channel: %v", ctx.Err())
			return
		case <-t.C:
			err := c.Encode(e)
			if err != nil {
				log.Printf("channel: err while sending ping, closing channel: %v", err)
				c.conn.Close()
				return
			}
		}
	}
}

func NewChannel(conn *net.TCPConn) *Channel {
	c := &Channel{
		Encoder: json.NewEncoder(conn),
		Decoder: json.NewDecoder(conn),
		conn:    conn,
		mailbox: make([]*unsafe.Pointer, 1024),
	}
	// this initialization is necessary,
	// a memory address must exist for every
	// *unsafe.Pointer in the map.
	// i.e 0x1[0x2] -> 0x2[nil]
	//      ^
	//      |
	//      &p
	for i := 0; i < 1024; i++ {
		p := unsafe.Pointer(nil)
		c.mailbox[i] = &p
	}

	// reserve mailbox 0 for ping for
	// ping responses.
	e := unsafe.Pointer(&Envelope{})
	c.mailbox[0] = &e

	c.Send(&Envelope{})
	d := Delivery{
		channel: *c,
	}
	go func() {
		e, err := d.Wait(context.TODO())
		log.Printf("delivery done: %v %v", e, err)
	}()
	return c
}

// Send delivers an Envelope to Vim.
//
// The Envelope's BoxNum field should be empty, and is ignored
// if not.
//
// The Envelope's In field MUST be false.
//
//
// The returneed Delivery provides a Wait method for blocking
// on the response Envelope.
func (c Channel) Send(e *Envelope) (*Delivery, error) {
	// try to compare and swap a mailbox number
	// until success
	var boxNum uint32
	for ; boxNum < uint32(len(c.mailbox)); boxNum++ {
		e.BoxNum = boxNum
		if ok := atomic.CompareAndSwapPointer(c.mailbox[boxNum], nil, unsafe.Pointer(e)); ok {
			break
		}
	}

	err := c.Encode(e)
	if err != nil {
		log.Printf("channel: error sending, closing channel: %v", err)
		c.conn.Close()
		return nil, err
	}

	return &Delivery{
		channel: c,
		boxNum:  boxNum,
	}, nil
}

// Recv reads off the json.Decoder
// until the ctx is canceled or the underlying
// tcp.conn fails.
//
// When a json message is received the payload
// will be placed in the channel's mailbox.
func (c Channel) Recv(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			log.Printf("channel rcv ctx canceled: %v", ctx.Err())
			return
		}
		e := &Envelope{}
		err := c.Decode(e)
		if err != nil {
			log.Printf("channel: error receiving, closing channel: %v", err)
			c.conn.Close()
			break
		}
		e.In = true
		atomic.SwapPointer(c.mailbox[int(e.BoxNum)], unsafe.Pointer(e))
	}
}
