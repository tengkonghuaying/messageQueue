package server

import (
	"context"
	"encoding/binary"
	"io"
	"log"
	"mq/protocol"
)

type Client struct {
	conn  io.ReadWriteCloser
	name  string
	state int
}

func NewClient(conn io.ReadWriteCloser, name string) *Client {
	return &Client{conn, name, -1}
}

func (c *Client) GetName() string {
	return c.name
}

func (c *Client) GetState() int {
	return c.state
}

func (c *Client) SetState(state int) {
	c.state = state
}

func (c *Client) Read(data []byte) (int, error) {
	return c.conn.Read(data)
}

func (c *Client) Write(data []byte) (int, error) {
	var err error

	err = binary.Write(c.conn, binary.BigEndian, int32(len(data)))
	if err != nil {
		return 0, err
	}

	n, err := c.conn.Write(data)
	if err != nil {
		return 0, err
	}

	return n + 4, nil
}

func (c *Client) Close() {
	log.Printf("CLIENT(%s): closing", c.GetName())
	err := c.conn.Close()
	if err != nil {
		log.Printf("CLIENT(%s): close failed", c.GetName())
		return
	}
}

// Handle 调用IOLoop从客户端读取数据
func (c *Client) Handle(ctx context.Context) {
	defer c.Close()
	proto := &protocol.Protocol{}
	err := proto.IOLoop(ctx, c)
	if err != nil {
		log.Printf("ERROR: client(%s) - %s", c.GetName(), err.Error())
		return
	}
}
