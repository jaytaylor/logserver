package server

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"regexp"
	"time"

	. "github.com/jaytaylor/logserver"
)

type (
	Listener struct {
		Channel   chan Entry
		LastEntry time.Time
		Filter    EntryFilter
	}
	Server struct {
		AddListener    chan *Listener
		ReceiveEntry   chan Entry
		RemoveListener chan *Listener
		listeners      []*Listener
		history        History
		drains         []*Drainer
	}
	HaProxyLogLine struct {
		Payload *[]byte
	}
)

const (
	haProxyTsLayout = "2/Jan/2006:15:04:05.000"
)

var (
	// NB: See section 8.2.3 of http://haproxy.1wt.eu/download/1.5/doc/configuration.txt to see HAProxy log format details.
	// Example line:
	//     <142>Sep 27 00:15:57 haproxy[28513]: 67.188.214.167:64531 [27/Sep/2013:00:15:43.494] frontend~ test/10.127.57.177-10000 449/0/0/13531/13980 200 13824 - - ---- 6/6/0/1/0 0/0 "GET / HTTP/1.1"
	haProxyLogRe = regexp.MustCompile(`^` +
		`<(?P<logLevel>\d+)>` +
		`[a-zA-Z0-9: ]+ ` +
		`(?P<processName>[a-zA-Z_\.-]+)\[(?P<pid>[^\]]+)\]: ` +
		`(?P<clientIp>[0-9\.]+):(?P<clientPort>[0-9]+) ` +
		`\[(?P<acceptTs>[^\]]+)\] ` +
		`(?P<frontend>[^ ]+) ` +
		`(?:` +
		`(?P<backend>[^\/]+)\/(?P<server>[^ ]+) ` +
		`(?P<tq>-?\d+)\/(?P<tw>-?\d+)\/(?P<tc>-?\d+)\/(?P<tr>-?\d+)\/(?P<tt>\+?\d+) ` +
		`(?P<statusCode>\d+) ` +
		`(?P<bytesRead>\d+) ` +
		`(?P<terminationState>[a-zA-Z -]+) ` +
		`(?P<actConn>\d+)\/(?P<feConn>\d+)\/(?P<beConn>\d+)\/(?P<srvConn>\d+)\/(?P<retries>\+?\d) ` +
		`(?P<srvQueue>\d+)\/(?P<backendQueue>\d+) ` +
		`(?P<headers>\{.*\} *)*` +
		`"(?P<request>(?P<requestMethod>[^ ]+) (?P<requestPath>.*)(?: HTTP\/[0-9\.]+))"?` +
		`|` +
		`(?P<error>[a-zA-Z0-9: ]+)` +
		`)`,
	)
)

// Generic submatch mapper.  Returns nil if no match.
func RegexpSubmatchesToMap(re *regexp.Regexp, input *[]byte) map[string]string {
	submatches := re.FindSubmatch(*input)
	if submatches == nil {
		return nil
	}
	data := map[string]string{}
	for i, key := range haProxyLogRe.SubexpNames() {
		data[key] = string(submatches[i])
		//fmt.Printf("data[%v] = \"%v\"\n", key, data[key])
	}
	return data
}

// Payload must be from an HAProxy log line formatted in accordance with
// section 8.2.3 of http://haproxy.1wt.eu/download/1.5/doc/configuration.txt.
func (this *HaProxyLogLine) ToEntry() (*Entry, error) {
	matches := RegexpSubmatchesToMap(haProxyLogRe, this.Payload)
	if matches == nil || len(matches) == 0 {
		return nil, fmt.Errorf("Payload didn't match haproxy log line regexp, payload=%v", string(*this.Payload))
	}

	app, ok := matches["backend"]
	if !ok || len(app) == 0 {
		return nil, fmt.Errorf("failed to extract app/backend from payload=%v", string(*this.Payload))
	}
	//fmt.Printf("APP=%v\n", app)

	acceptTsStr, ok := matches["acceptTs"]
	if !ok {
		return nil, fmt.Errorf("app=%v: failed to get timestamp from matches=%v/payload=%v", app, matches, string(*this.Payload))
	}
	acceptTs, err := time.Parse(haProxyTsLayout, acceptTsStr)
	if err != nil {
		return nil, fmt.Errorf("app=%v: failed to parse timestamp=%v\n", app, acceptTsStr)
	}

	var data []byte

	if errStr, ok := matches["error"]; ok && len(errStr) > 0 {
		data = []byte(errStr)
	} else {
		method, _ := matches["requestMethod"]
		path, _ := matches["requestPath"]
		clientIp, _ := matches["clientIp"]
		server, _ := matches["server"]
		tConnect, _ := matches["tc"]
		tWait , _ := matches["tw"]
		tQueue, _ := matches["tq"]
		tRequest, _ := matches["tr"]
		tTotal, _ := matches["tt"]
		statusCode, _ := matches["statusCode"]
		bytesRead, _ := matches["bytesRead"]

		data = []byte(fmt.Sprintf(
			"method=%v path=%v client=%v dyno=%v status=%v bytes=%v conn=%vms wait=%vms queue=%vms svc=%vms tot=%vms\n",
			method, path, clientIp, server, statusCode, bytesRead, tConnect, tWait, tQueue, tRequest, tTotal,
		))
	}

	entry := &Entry{
		Time:        acceptTs,
		Application: app,
		Process:     "router",
		Data:        data,
	}
	return entry, nil
}

func Start() (*Server, error) {
	this := &Server{
		ReceiveEntry:   make(chan Entry),
		listeners:      make([]*Listener, 0),
		AddListener:    make(chan *Listener),
		RemoveListener: make(chan *Listener),
		history: History{
			Position: 0,
			Size:     0,
			Capacity: 1000,
			Entries:  make([]Entry, 1000),
		},
	}

	log.Printf("starting log listener tcp+udp servers on port: %v\n", Port)

	// TCP listener.
	tcpLn, err := net.Listen("tcp", ":"+fmt.Sprint(Port))
	if err != nil {
		return this, err
	}
	go func() {
		for {
			conn, err := tcpLn.Accept()
			if err != nil {
				log.Printf("tcp listener error: %v\n", err)
				continue
			}
			log.Printf("[%v] connected\n", conn.RemoteAddr())
			go this.handleTcpConnection(conn)
		}
	}()

	// UDP listener.
	udpAddress, err := net.ResolveUDPAddr("udp", ":"+fmt.Sprint(Port))
	if err != nil {
		return this, err
	}
	udpLn, err := net.ListenUDP("udp", udpAddress)
	if err != nil {
		return this, err
	}
	go func() {
		for {
			payload := make([]byte, 8192)
			numBytesRead, err := udpLn.Read(payload)
			if err != nil {
				fmt.Printf("udp reader error: %v\n", err)
				continue
			}
			go this.handleUdpPayLoad(&payload, numBytesRead)
		}
	}()

	go func() {
		for {
			select {
			// Add a listener to the list.
			case listener := <-this.AddListener:
				this.addListener(listener)
			// Remove a listener.
			case listener := <-this.RemoveListener:
				this.removeListener(listener)
			// Receive a message.
			case entry, ok := <-this.ReceiveEntry:
				if !ok {
					break
				}
				this.receiveEntry(entry)
			}
		}
	}()

	return this, nil
}
func (this *Server) handleTcpConnection(conn net.Conn) {
	defer conn.Close()

	var typ string
	err := Read(conn, &typ)
	if err != nil {
		fmt.Printf("error: %v\n", err)
		return
	}
	switch typ {
	case "logger":
		this.handleLogger(conn)
	}
}
func (this *Server) handleUdpPayLoad(payload *[]byte, numBytesRead int) {
	hapll := HaProxyLogLine{Payload: payload}
	entry, err := hapll.ToEntry()
	if err != nil {
		fmt.Printf("error: handleUdpPayLoad :: %v\n", err)
		return
	}
	this.ReceiveEntry <- *entry
}
func (this *Server) handleLogger(conn net.Conn) {
	reader := bufio.NewReader(conn)
	for {
		entry, err := ReadEntry(reader)
		if err != nil {
			fmt.Printf("error: %v\n", err)
			break
		}
		this.ReceiveEntry <- entry
	}
}
func (this *Server) addListener(listener *Listener) {
	this.listeners = append(this.listeners, listener)
	for entry := range this.history.GetEntriesSince(listener.LastEntry) {
		if !listener.Filter.Include(entry) {
			continue
		}
		listener.Channel <- entry
	}

}
func (this *Server) removeListener(listener *Listener) {
	nls := make([]*Listener, 0, len(this.listeners)-1)
	for _, thisListener := range this.listeners {
		if listener != thisListener {
			nls = append(nls, thisListener)
		}
	}
	this.listeners = nls
}
func (this *Server) receiveEntry(entry Entry) {
	this.history.Add(entry)
	// Push an entry on to the end of the channel,
	//   if the channel is full remove the first entry
	//   and try to push it on the end again.
	for _, listener := range this.listeners {
		if !listener.Filter.Include(entry) {
			continue
		}

		listener.Channel <- entry
	}
}

func (this *Server) StartListener(w io.Writer, filter EntryFilter) error {
	c := make(chan Entry)
	listener := &Listener{
		Channel: c,
		Filter:  filter,
	}
	defer func() {
		this.RemoveListener <- listener
	}()
	this.AddListener <- listener

	for entry := range Throttle(c, 100) {
		_, err := w.Write(entry.Line())
		if err != nil {
			return err
		}
	}
	return nil
}
