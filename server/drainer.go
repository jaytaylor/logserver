package server

import (
	"log"
	"log/syslog"
	"time"

	. "github.com/jaytaylor/logserver"
)

type (
	Drainer struct {
		Address   string
		Filter    EntryFilter
		server    *Server
		listener  *Listener
		terminate chan bool
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
		Address:   address,
		Filter:    filter,
		server:    this,
		listener:  listener,
		terminate: make(chan bool, 1),
	}
	go func() {
		var w *syslog.Writer
		var err error
		for entry := range Throttle(c, 100) {
			for {
				// If we terminated give up
				select {
				case <-drainer.terminate:
					return
				default:
				}

				// Connect
				if w == nil {
					log.Printf("connecting to syslog://%v\n", address)
					w, err = syslog.Dial("tcp", address, syslog.LOG_INFO, "")
					if err != nil {
						w = nil
						time.Sleep(time.Second * 5)
						continue
					}
				}
				// Send the message
				_, err = w.Write(entry.Line())
				if err != nil {
					w.Close()
					w = nil
					time.Sleep(time.Second * 5)
					continue
				}

				// Successfully sent the message so break
				break
			}
		}
	}()
	return drainer
}

func (this *Drainer) Close() {
	this.server.RemoveListener <- this.listener
	close(this.listener.Channel)
	this.terminate <- true
}
