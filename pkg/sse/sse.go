// Package sse provides a Server-Sent Events parser and writer.
// It implements the SSE spec (https://html.spec.whatwg.org/multipage/server-sent-events.html)
// using a Go 1.23 iterator for reading and a write function for producing events.
package sse

import (
	"bufio"
	"fmt"
	"io"
	"strings"
)

// Event represents a single SSE event.
type Event struct {
	// LastEventID is the last non-empty event ID encountered, which may not be
	// the ID of the current event. Per the spec, empty IDs and IDs containing
	// U+0000 NULL are ignored.
	LastEventID string
	// Type is the event type from the "event" field. It is empty for unnamed events.
	Type string
	// Data is the event payload from all "data" fields, joined with newlines.
	Data string
}

// ReadConfig controls SSE parsing behaviour.
type ReadConfig struct {
	// MaxEventSize limits the total byte length of a single event line.
	// Default is 64KB if not set.
	MaxEventSize int
}

const defaultMaxEventSize = 64 * 1024

// Read returns an iterator that yields parsed SSE events from r.
// On error, iteration stops and the error is yielded once.
// io.EOF is treated as a clean termination and is not yielded.
func Read(r io.Reader, cfg *ReadConfig) func(func(Event, error) bool) {
	maxSize := defaultMaxEventSize
	if cfg != nil && cfg.MaxEventSize > 0 {
		maxSize = cfg.MaxEventSize
	}

	return func(yield func(Event, error) bool) {
		scanner := bufio.NewScanner(r)
		buf := make([]byte, maxSize)
		scanner.Buffer(buf, maxSize)

		var (
			lastEventID string
			typ         string
			sb          strings.Builder
			dirty       bool
		)

		flush := func() bool {
			if !dirty {
				return true
			}
			data := strings.TrimSuffix(sb.String(), "\n")
			if !yield(Event{LastEventID: lastEventID, Type: typ, Data: data}, nil) {
				return false
			}
			sb.Reset()
			typ = ""
			dirty = false
			return true
		}

		for scanner.Scan() {
			line := scanner.Text()

			if line == "" {
				// Blank line delimits events
				if !flush() {
					return
				}
				continue
			}

			if line[0] == ':' {
				// Comment line — also delimits events
				if !flush() {
					return
				}
				continue
			}

			// Parse field[: value]
			colonIdx := strings.IndexByte(line, ':')
			if colonIdx == -1 {
				continue
			}

			fieldName := line[:colonIdx]
			fieldValue := line[colonIdx+1:]
			if len(fieldValue) > 0 && fieldValue[0] == ' ' {
				fieldValue = fieldValue[1:]
			}

			switch fieldName {
			case "data":
				sb.WriteString(fieldValue)
				sb.WriteByte('\n')
				dirty = true
			case "event":
				typ = fieldValue
				dirty = true
			case "id":
				if strings.IndexByte(fieldValue, 0) == -1 {
					lastEventID = fieldValue
				}
			case "retry":
			}
		}

		if !flush() {
			return
		}

		if err := scanner.Err(); err != nil {
			yield(Event{}, err)
		}
	}
}

// WriteEvent writes an Event to w following the SSE format.
// It handles multi-line data, optional event type, and optional ID.
// A blank line is always written after the event to delimit it.
func WriteEvent(w io.Writer, evt Event) error {
	if evt.LastEventID != "" {
		if _, err := fmt.Fprintf(w, "id: %s\n", evt.LastEventID); err != nil {
			return err
		}
	}
	if evt.Type != "" {
		if _, err := fmt.Fprintf(w, "event: %s\n", evt.Type); err != nil {
			return err
		}
	}
	if evt.Data != "" {
		for _, line := range strings.Split(evt.Data, "\n") {
			if _, err := fmt.Fprintf(w, "data: %s\n", line); err != nil {
				return err
			}
		}
	}
	_, err := fmt.Fprintf(w, "\n")
	return err
}
