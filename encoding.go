package log

import (
	"encoding/binary"
	"io"
	"time"
)

func Write(w io.Writer, args ...interface{}) error {
	for _, arg := range args {
		switch t := arg.(type) {
		// Handle bytes
		case []byte:
			err := Write(w, int64(len(t)))
			if err != nil {
				return err
			}
			_, err = w.Write(t)
			if err != nil {
				return err
			}
		// Handle strings
		case string:
			err := Write(w, int64(len(t)))
			if err != nil {
				return err
			}
			_, err = w.Write([]byte(t))
			if err != nil {
				return err
			}
		// Handle times
		case time.Time:
			err := Write(w, t.Unix())
			if err != nil {
				return err
			}
			err = Write(w, int32(t.Nanosecond()))
			if err != nil {
				return err
			}
		default:
			err := binary.Write(w, binary.BigEndian, t)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func Read(r io.Reader, dsts ...interface{}) error {
	for _, dst := range dsts {
		switch t := dst.(type) {
		// Handle byte arrays
		case *[]byte:
			var size int64
			err := Read(r, &size)
			if err != nil {
				return err
			}
			bs := make([]byte, size)
			_, err = io.ReadFull(r, bs)
			if err != nil {
				return err
			}
			*t = bs
		// Handle times
		case *time.Time:
			var sec int64
			err := Read(r, &sec)
			if err != nil {
				return err
			}
			var nsec int32
			err = Read(r, &nsec)
			if err != nil {
				return err
			}
			*t = time.Unix(sec, int64(nsec))
		// Handle strings
		case *string:
			var size int64
			err := Read(r, &size)
			if err != nil {
				return err
			}
			bs := make([]byte, size)
			_, err = io.ReadFull(r, bs)
			if err != nil {
				return err
			}
			*t = string(bs)
		default:
			err := binary.Read(r, binary.BigEndian, dst)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
