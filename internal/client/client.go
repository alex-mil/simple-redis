package simpleredisclient

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/sony/gobreaker"
)

type Client struct {
	pool    *sync.Pool
	breaker *gobreaker.CircuitBreaker
}

func NewClient(address string, maxConnections int) *Client {
	client := &Client{
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

	client.breaker = gobreaker.NewCircuitBreaker(gobreaker.Settings{
		MaxRequests: 5,
		Interval:    time.Second * 60,
		Timeout:     time.Second * 5,
	})

	return client
}

func (c *Client) Set(key, value string) (string, error) {
	return c.sendCommand("SET", key, value)
}

func (c *Client) Get(key string) (string, error) {
	return c.sendCommand("GET", key)
}

func (c *Client) Del(key string) (string, error) {
	return c.sendCommand("DEL", key)
}

func (c *Client) HSet(key, field string, value interface{}) (string, error) {
	return c.sendCommand("HSET", key, field, fmt.Sprintf("%v", value))
}

func (c *Client) Close() {
	for {
		c.getConnection().Close()
	}
}

// getConnection retrieves a connection from the pool
func (c *Client) getConnection() net.Conn {
	return c.pool.Get().(net.Conn)
}

// putConnection returns a connection to the pool
func (c *Client) putConnection(conn net.Conn) {
	c.pool.Put(conn)
}

// sendCommand sends a command to the simplified Redis server
func (c *Client) sendCommand(args ...string) (string, error) {
	resp, err := c.breaker.Execute(func() (interface{}, error) {
		conn := c.getConnection()
		defer c.putConnection(conn)

		cmd := strings.Join(args, " ") + "\r\n"
		_, err := conn.Write([]byte(cmd))
		if err != nil {
			return "", err
		}

		r := bufio.NewReader(conn)
		resp, err := r.ReadString('\n')
		if err != nil {
			return "", err
		}

		return strings.Trim(resp, "\r\n"), nil
	})

	if err != nil {
		return "", err
	}
	return resp.(string), nil
}
