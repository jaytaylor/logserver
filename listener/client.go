package listener

import (
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	"github.com/jaytaylor/logserver"
)

type (
	Client struct {
		conn      net.Conn
		lastEntry time.Time
	}
)

func Dial(host string) (*Client, error) {
	this := &Client{}
	var err error
	if !strings.Contains(host, ":") {
		host = fmt.Sprintf("%v:%v", host, log.DefaultPort)
	}
	this.conn, err = net.Dial("tcp", host)
	if err != nil {
		return this, err
	}
	return this, log.Write(this.conn, "listener", this.lastEntry)
}

func (this *Client) Receive(writer io.Writer) error {
	for {
		entry, err := log.ReadEntry(this.conn)
		if err != nil {
			return err
		}
		this.lastEntry = entry.Time
		_, err = writer.Write(entry.Data)
		if err != nil {
			return err
		}
	}
	return nil
}

func (this *Client) Close() error {
	if this.conn == nil {
		return nil
	}
	return this.conn.Close()
}
