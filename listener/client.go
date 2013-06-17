package listener

import (
	"fmt"
	"io"
	"net"
	"time"

	"github.com/sendhub/log"
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
	this.conn, err = net.Dial("tcp", host+":"+fmt.Sprint(log.Port))
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
