package sessionhost

import (
	"bufio"
	"fmt"
	"io"
	"sync"
)

type frameCodec struct {
	reader   *bufio.Scanner
	writer   io.Writer
	writeMu  sync.Mutex
	maxBytes int
}

func newFrameCodec(stream io.ReadWriter, maxBytes int) *frameCodec {
	if maxBytes <= 0 {
		maxBytes = defaultMaxFrameBytes
	}
	initialBufferSize := 64 << 10
	if maxBytes < initialBufferSize {
		initialBufferSize = maxBytes
	}
	scanner := bufio.NewScanner(stream)
	scanner.Buffer(make([]byte, initialBufferSize), maxBytes)
	return &frameCodec{
		reader:   scanner,
		writer:   stream,
		maxBytes: maxBytes,
	}
}

func (c *frameCodec) read() (frame, error) {
	if !c.reader.Scan() {
		if err := c.reader.Err(); err != nil {
			return frame{}, fmt.Errorf("session-host: read frame: %w", err)
		}
		return frame{}, io.EOF
	}
	return decodeFrame(c.reader.Bytes(), c.maxBytes)
}

func (c *frameCodec) write(value frame) error {
	raw, err := encodeFrame(value)
	if err != nil {
		return err
	}
	if len(raw)+1 > c.maxBytes {
		return fmt.Errorf("session-host: frame size %d exceeds limit %d", len(raw)+1, c.maxBytes)
	}

	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if _, err := c.writer.Write(append(raw, '\n')); err != nil {
		return fmt.Errorf("session-host: write frame: %w", err)
	}
	return nil
}
