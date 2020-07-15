package client

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
)

var ErrProtocol = errors.New("invalid request")

// Read a single command, as-is. Used to parse replies from redis.
// Understands RESP3 proto.
func Read(r *bufio.Reader) (string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}
	if len(line) < 3 {
		return "", ErrProtocol
	}

	// TODO: all other cases.
	switch line[0] {
	default:
		return "", ErrProtocol
	case '+', '-', ':':
		// +: inline string
		// -: errors
		// :: integer
		// Simple line based replies.
		return line, nil
	case '$':
		// bulk strings are: `$5\r\nhello\r\n`
		length, err := strconv.Atoi(line[1 : len(line)-2])
		if err != nil {
			return "", err
		}
		if length < 0 {
			// -1 is a nil response
			return line, nil
		}
		var (
			buf = make([]byte, length+2)
			pos = 0
		)
		for pos < length+2 {
			n, err := r.Read(buf[pos:])
			if err != nil {
				return "", err
			}
			pos += n
		}
		return line + string(buf), nil
	}
}

// Write a command in RESP3 proto. Used to write commands to redis.
// Currently only supports string arrays.
func Write(w io.Writer, cmd []string) error {
	if _, err := fmt.Fprintf(w, "*%d\r\n", len(cmd)); err != nil {
		return err
	}
	for _, c := range cmd {
		if _, err := fmt.Fprintf(w, "$%d\r\n%s\r\n", len(c), c); err != nil {
			return err
		}
	}
	return nil
}
