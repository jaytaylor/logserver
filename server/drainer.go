package server

import (
	"log/syslog"
	"time"

	. "github.com/Sendhub/logserver"
)

type (
	Drainer struct {
		Address  string
		Filter   EntryFilter
		server   *Server
		listener *Listener
	}
)

func (this *Server) StartDrainer(address string, filter EntryFilter) *Drainer {
	c := make(chan Entry)
	listener := &Listener{
		Channel:   c,
		Filter:    filter,
		LastEntry: time.Now(),
	}
	this.AddListener <- listener

	drainer := &Drainer{
		Address:  address,
		Filter:   filter,
		server:   this,
		listener: listener,
	}
	go func() {
		var w *syslog.Writer
		var err error
		for entry := range Throttle(c, 100) {
			for {
				if w == nil {
					w, err = syslog.Dial("tcp", address, syslog.LOG_INFO, "")
					if err != nil {
						w = nil
						time.Sleep(time.Second * 1)
						continue
					}
				}
				_, err = w.Write(entry.Data)
				if err != nil {
					w.Close()
					w = nil
					time.Sleep(time.Second * 1)
					continue
				}
				break
			}
		}
	}()
	return drainer
}

func (this *Drainer) Close() {
	this.server.RemoveListener <- this.listener
	close(this.listener.Channel)
}
