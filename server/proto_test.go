package server

import (
	"bufio"
	"bytes"
	"io"
	"reflect"
	"testing"
)

func TestReadArray(t *testing.T) {
	type cas struct {
		payload string
		err     error
		res     []string
	}
	for i, c := range []cas{
		{
			payload: "*1\r\n$4\r\nPING\r\n",
			res:     []string{"PING"},
		},
		{
			payload: "*2\r\n$4\r\nLLEN\r\n$6\r\nmylist\r\n",
			res:     []string{"LLEN", "mylist"},
		},
		{
			payload: "*2\r\n$4\r\nLLEN\r\n$6\r\nmyl",
			err:     ErrProtocol,
		},
		{
			payload: "PING",
			err:     io.EOF,
		},
		{
			payload: "*0\r\n",
		},
		{
			payload: "*-1\r\n", // not sure this is legal in a request
		},
	} {
		res, err := readArray(bufio.NewReader(bytes.NewBufferString(c.payload)))
		if have, want := err, c.err; have != want {
			t.Errorf("err %d: have %v, want %v", i, have, want)
			continue
		}
		if have, want := res, c.res; !reflect.DeepEqual(have, want) {
			t.Errorf("case %d: have %v, want %v", i, have, want)
		}
	}
}
