package proto

import (
	"bufio"
	"net"
)

type Client struct {
	c net.Conn
	r *bufio.Reader
}

func Dial(addr string) (*Client, error) {
	c, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	return &Client{
		c: c,
		r: bufio.NewReader(c),
	}, nil
}

func (c *Client) Close() error {
	return c.c.Close()
}

func (c *Client) Do(cmd ...string) (string, error) {
	if err := Write(c.c, cmd); err != nil {
		return "", err
	}
	return Read(c.r)
}

func (c *Client) Read() (string, error) {
	return Read(c.r)
}
