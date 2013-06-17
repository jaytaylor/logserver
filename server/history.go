package server

import (
	"time"

	. "github.com/sendhub/log"
)

type (
	History struct {
		Position, Size, Capacity int
		Entries                  []Entry
	}
)

func (this *History) Add(entry Entry) {
	this.Position++
	this.Entries[this.Position%this.Capacity] = entry
	if this.Size < this.Capacity {
		this.Size++
	}
}

func (this *History) GetEntriesSince(time time.Time) <-chan Entry {
	c := make(chan Entry)
	go func() {
		for i := this.Position - this.Size; i <= this.Position+this.Size; i++ {
			entry := this.Entries[i%this.Capacity]
			if !entry.Time.After(time) {
				continue
			}
			c <- entry
		}
		close(c)
	}()
	return c
}
