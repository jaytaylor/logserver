package logger

import (
	"fmt"
	"net"
	"time"

	"github.com/sendhub/log"
)

type (
	Client struct {
		conn                 net.Conn
		application, process string
	}
)

func Dial(host, application, process string) (*Client, error) {
	this := &Client{
		application: application,
		process:     process,
	}
	var err error
	this.conn, err = net.Dial("tcp", host+":"+fmt.Sprint(log.Port))
	if err != nil {
		return this, err
	}
	return this, log.Write(this.conn, "logger")
}

func (this *Client) Send(bs []byte) error {
	return log.Entry{
		Time:        time.Now(),
		Application: this.application,
		Process:     this.process,
		Data:        bs,
	}.Write(this.conn)
}

func (this *Client) Close() error {
	if this.conn == nil {
		return nil
	}
	return this.conn.Close()
}
