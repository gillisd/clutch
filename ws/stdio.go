package ws

import (
	"bufio"
	"io"
	"sync"
)

// StdioConn implements Conn over newline-delimited JSON on an io.Reader and io.Writer.
type StdioConn struct {
	scanner *bufio.Scanner
	writer  io.Writer
	once    sync.Once
	done    chan struct{}
}

// NewStdioConn creates a Conn that reads newline-delimited JSON from r and writes to w.
func NewStdioConn(r io.Reader, w io.Writer) *StdioConn {
	s := bufio.NewScanner(r)
	s.Buffer(make([]byte, 64*1024), 10*1024*1024) // 10MB max line for large CDP responses
	return &StdioConn{
		scanner: s,
		writer:  w,
		done:    make(chan struct{}),
	}
}

func (c *StdioConn) ReadMessage() (int, []byte, error) {
	if c.scanner.Scan() {
		return 1, c.scanner.Bytes(), nil
	}
	if err := c.scanner.Err(); err != nil {
		return 0, nil, err
	}
	return 0, nil, io.EOF
}

func (c *StdioConn) WriteMessage(_ int, data []byte) error {
	data = append(data, '\n')
	_, err := c.writer.Write(data)
	return err
}

func (c *StdioConn) Close() error {
	c.once.Do(func() { close(c.done) })
	return nil
}
