package log

import (
	"io"
	"regexp"
	"time"
)

type (
	Entry struct {
		Time        time.Time
		Application string
		Process     string
		Data        []byte
	}
	EntryFilter struct {
		Application string
		Process     string
		Data        *regexp.Regexp
	}
)

func (this *Entry) Line() []byte {
	return append([]byte(this.Time.String()+" "+this.Application+"["+this.Process+"]: "), this.Data...)
}

func (this EntryFilter) Include(entry Entry) bool {
	if !(this.Application == "" || this.Application == entry.Application) {
		return false
	}
	if !(this.Process == "" || this.Process == entry.Process) {
		return false
	}
	if !(this.Data == nil || this.Data.Match(entry.Data)) {
		return false
	}
	return true
}

func ReadEntry(reader io.Reader) (Entry, error) {
	var entry Entry
	return entry, Read(reader, &entry.Time, &entry.Application, &entry.Process, &entry.Data)
}

func (this Entry) Write(writer io.Writer) error {
	return Write(writer, this.Time, this.Application, this.Process, this.Data)
}

// Throttle a channel so that it dequeues quickly
//   and drops old messages
func Throttle(c <-chan Entry, sz int) <-chan Entry {
	buffer := make(chan Entry, sz)
	go func() {
		for entry := range c {
			select {
			case buffer <- entry:
				continue
			default:
			}

			select {
			case <-buffer:
			default:
			}

			select {
			case buffer <- entry:
			default:
			}
		}
		close(buffer)
	}()
	return buffer
}
