package main

import (
	"fmt"
	"net"

	"bitbucket.org/alex-mil/simple-redis/internal/store"
	"golang.org/x/net/netutil"
)

func main() {
	storeInstance := store.NewStore()
	
	// Start a TCP server on port 6380
	listener, err := net.Listen("tcp", ":6380")
	if err != nil {
		panic(err)
	}
	defer listener.Close()

	// Limit the number of simultaneous connections
	limitListener := netutil.LimitListener(listener, 1000)

	fmt.Println("Server is listening on port 6380...")

	for {
		conn, err := limitListener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err)
			continue
		}

		go store.HandleConnection(conn, storeInstance)
	}
}
