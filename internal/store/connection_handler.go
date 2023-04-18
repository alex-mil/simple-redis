package store

import (
	"bufio"
	"fmt"
	"net"
	"strings"
)

func HandleConnection(conn net.Conn, store *Store) {
	defer conn.Close()

	// Use bufio.Reader to read client input
	reader := bufio.NewReader(conn)

	for {
		// Read input until a newline character is encountered
		input, err := reader.ReadString('\n')
		if err != nil {
			return
		}

		// Split input into command and arguments
		tokens := strings.Split(strings.TrimSpace(input), " ")
		command := strings.ToUpper(tokens[0])
		args := tokens[1:]

		// Process commands and send responses
		switch command {
		case "SET":
			if len(args) == 2 {
				store.Set(args[0], args[1])
				fmt.Fprintln(conn, "OK")
			} else {
				fmt.Fprintln(conn, "ERR wrong number of arguments")
			}
		case "GET":
			if len(args) == 1 {
				value, found := store.Get(args[0])
				if found {
					fmt.Fprintln(conn, value)
				} else {
					fmt.Fprintln(conn, "ERR key not found")
				}
			} else {
				fmt.Fprintln(conn, "ERR wrong number of arguments")
			}
		case "DEL":
			if len(args) == 1 {
				found := store.Del(args[0])
				if found {
					fmt.Fprintln(conn, "OK")
				} else {
					fmt.Fprintln(conn, "ERR key not found")
				}
			} else {
				fmt.Fprintln(conn, "ERR wrong number of arguments")
			}
		case "HSET":
			if len(args) == 3 {
				store.HSet(args[0], args[1], args[2])
				fmt.Fprintln(conn, "OK")
			} else {
				fmt.Fprintln(conn, "ERR wrong number of arguments")
			}
		default:
			fmt.Fprintln(conn, "ERR unknown command")
		}
	}
}
