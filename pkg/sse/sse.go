// Package sse provides a Server-Sent Events parser and writer.
// It implements the SSE spec (https://html.spec.whatwg.org/multipage/server-sent-events.html).
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

// ReadConfig controls SSE parsing behavior.
type ReadConfig struct {
	// MaxEventSize limits the total byte length of a single event line.
	// Default is 64KB if not set.
	MaxEventSize int
}

const defaultMaxEventSize = 64 * 1024

// sseParser holds the state for parsing an SSE stream.
type sseParser struct {
	lastEventID string
	typ         string
	sb          strings.Builder
	dirty       bool
}

// flush yields the current accumulated event (if any) and resets state.
// Returns false if the yield function signals stop.
func (p *sseParser) flush(yield func(Event, error) bool) bool {
	if !p.dirty {
		return true
	}

	data := strings.TrimSuffix(p.sb.String(), "\n")
	if !yield(Event{LastEventID: p.lastEventID, Type: p.typ, Data: data}, nil) {
		return false
	}

	p.sb.Reset()
	p.typ = ""
	p.dirty = false

	return true
}

// processLine handles a single line from the SSE stream.
// It returns false if the yield function signals stop.
func (p *sseParser) processLine(line string, yield func(Event, error) bool) bool {
	if line == "" {
		// Blank line delimits events.
		return p.flush(yield)
	}

	if line[0] == ':' {
		// Comment line — also delimits events.
		return p.flush(yield)
	}

	// Parse field[: value]
	before, after, ok := strings.Cut(line, ":")
	if !ok {
		return true
	}

	fieldValue := after
	if fieldValue != "" && fieldValue[0] == ' ' {
		fieldValue = fieldValue[1:]
	}

	p.applyField(before, fieldValue)

	return true
}

// applyField updates the parser state based on a parsed SSE field.
func (p *sseParser) applyField(fieldName, fieldValue string) {
	switch fieldName {
	case "data":
		p.sb.WriteString(fieldValue)
		p.sb.WriteByte('\n')
		p.dirty = true
	case "event":
		p.typ = fieldValue
		p.dirty = true
	case "id":
		if strings.IndexByte(fieldValue, 0) == -1 {
			p.lastEventID = fieldValue
		}
	case "retry":
	}
}

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

		p := &sseParser{}

		for scanner.Scan() {
			if !p.processLine(scanner.Text(), yield) {
				return
			}
		}

		if !p.flush(yield) {
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
		for line := range strings.SplitSeq(evt.Data, "\n") {
			if _, err := fmt.Fprintf(w, "data: %s\n", line); err != nil {
				return err
			}
		}
	}

	_, err := fmt.Fprintf(w, "\n")

	return err
}
