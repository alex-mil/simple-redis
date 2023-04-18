package simpleredisclient

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"sync"
)

type Client struct {
	pool    *sync.Pool
	address string
}

func NewClient(address string, maxConnections int) *Client {
	client := &Client{
		address: address,
		pool: &sync.Pool{
			New: func() interface{} {
				conn, err := net.Dial("tcp", address)
				if err != nil {
					panic(err)
				}
				return conn
			},
		},
	}

	// Warm up the connection pool
	for i := 0; i < maxConnections; i++ {
		client.pool.Put(client.pool.New())
	}

	return client
}

func (c *Client) ExecuteCommand(command string) (string, error) {
	// Get a connection from the pool
	conn := c.pool.Get().(net.Conn)
	defer c.pool.Put(conn)

	// Send the command to the server
	fmt.Fprintln(conn, command)

	// Read the server response
	reader := bufio.NewReader(conn)
	response, err := reader.ReadString('\n')
	if err != nil {
		return "", errors.New("error reading server response")
	}

	return response, nil
}

func (c *Client) Close() {
	for {
		conn, ok := c.pool.Get().(net.Conn)
		if !ok {
			break
		}
		conn.Close()
	}
}