package main

import (
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"time"
)

const (
	CONN_HOST = "localhost"
	CONN_PORT = 3333
	CONN_TYPE = "tcp"
	//TIMEOUT   = 10 * time.Second
	TIMEOUT = 0
)

// Listen for incoming connections.
func ListenTCPServer(host string, port int) error {
	addr := fmt.Sprintf("%s:%d", host, port)
	l, err := net.Listen("tcp", addr)
	if err != nil {
		log.Printf("Error listening: ", err.Error())
		return err
	}
	// Close the listener when the application closes.
	defer l.Close()
	log.Printf("Listening on " + addr)
	for {
		// Listen for an incoming connection.
		conn, err := l.Accept()
		if err != nil {
			log.Printf("Error accepting: ", err.Error())
		}
		// Handle connections in a new goroutine.
		go handleRequest(conn, TIMEOUT)
	}
}

// Handles incoming requests.
func handleRequest(conn net.Conn, timeout time.Duration) {
	// Make a buffer to hold incoming data.
	buf := make([]byte, 1024)
	if timeout > 0 {
		conn.SetDeadline(time.Now().Add(timeout))
	}
	// Read the incoming connection into the buffer.
	_, err := conn.Read(buf)
	if err != nil && err != io.EOF {
		log.Printf("Error reading: ", err.Error())
	}
	// Close the connection when you're done with it.
	conn.Close()
}

func main() {
	if ListenTCPServer(CONN_HOST, CONN_PORT) != nil {
		os.Exit(1)
	}

}
