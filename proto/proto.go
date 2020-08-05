package proto

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

var (
	ErrProtocol   = errors.New("invalid request")
	ErrUnexpected = errors.New("not what you asked for")
)

func readLine(r *bufio.Reader) (string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}
	if len(line) < 3 {
		return "", ErrProtocol
	}
	return line, nil
}

// Read an array, with all elements are the raw redis commands
func ReadArray(b string) ([]string, error) {
	r := bufio.NewReader(strings.NewReader(b))
	line, err := readLine(r)
	if err != nil {
		return nil, err
	}

	switch line[0] {
	default:
		return nil, ErrUnexpected
	case '*':
		length, err := strconv.Atoi(line[1 : len(line)-2])
		if err != nil {
			return nil, err
		}
		var res []string
		for i := 0; i < length; i++ {
			next, err := Read(r)
			if err != nil {
				return nil, err
			}
			res = append(res, next)
		}
		return res, nil
	}
}

func ReadString(b string) (string, error) {
	r := bufio.NewReader(strings.NewReader(b))
	line, err := readLine(r)
	if err != nil {
		return "", err
	}

	switch line[0] {
	default:
		return "", ErrUnexpected
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
		return string(buf[:len(buf)-2]), nil
	}
}

func ReadStrings(b string) ([]string, error) {
	elems, err := ReadArray(b)
	if err != nil {
		return nil, err
	}
	var res []string
	for _, e := range elems {
		s, err := ReadString(e)
		if err != nil {
			return nil, err
		}
		res = append(res, s)
	}
	return res, nil
}

// Read a single command, returning it raw. Used to read replies from redis.
// Understands RESP3 proto.
func Read(r *bufio.Reader) (string, error) {
	line, err := readLine(r)
	if err != nil {
		return "", err
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
	case '*':
		length, err := strconv.Atoi(line[1 : len(line)-2])
		if err != nil {
			return "", err
		}
		for i := 0; i < length; i++ {
			next, err := Read(r)
			if err != nil {
				return "", err
			}
			line += next
		}
		return line, nil
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
