package miniredis

import (
	"errors"
	"fmt"
	"net"
	"sync"

	"github.com/bsm/redeo"
)

type miniredis struct {
	sync.Mutex
	closed     chan struct{}
	listen     net.Listener
	info       *redeo.ServerInfo
	stringKeys map[string]string // GET/SET keys
}

var errUnimplemented = errors.New("Unimplemented")

// NewMiniRedis makes a new non-started miniredis object.
func NewMiniRedis() *miniredis {
	return &miniredis{
		closed:     make(chan struct{}),
		stringKeys: make(map[string]string),
	}
}

// Run creates and Start()s a miniredis.
func Run() (*miniredis, error) {
	m := NewMiniRedis()
	return m, m.Start()
}

func (m *miniredis) Close() {
	m.Lock()
	defer m.Unlock()
	if m.listen == nil {
		return
	}
	if m.listen.Close() != nil {
		return
	}
	<-m.closed
	m.listen = nil
}

func (m *miniredis) Start() error {
	m.Lock()
	defer m.Unlock()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		if l, err = net.Listen("tcp6", "[::1]:0"); err != nil {
			return fmt.Errorf("miniredis: failed to listen on a port: %v", err)
		}
	}
	m.listen = l
	srv := redeo.NewServer(&redeo.Config{Addr: "localhost:0"})

	m.info = srv.Info()

	commandPing(m, srv)
	commandGetSet(m, srv)

	go func() {
		e := make(chan error)
		go srv.Serve(e, m.listen)
		<-e
		m.closed <- struct{}{}
	}()
	return nil
}

// Addr returns '127.0.0.1:12345'. Can be given to a Dial()
func (m *miniredis) Addr() string {
	m.Lock()
	defer m.Unlock()
	return m.listen.Addr().String()
}

// TotalCommands returns the number of processed commands.
func (m *miniredis) TotalCommands() int {
	m.Lock()
	defer m.Unlock()
	return int(m.info.TotalProcessed())
}

// ClientsLen returns the number of connected clients.
func (m *miniredis) ClientsLen() int {
	m.Lock()
	defer m.Unlock()
	return m.info.ClientsLen()
}

// Get returns keys added with SET.
func (m *miniredis) Get(k string) string {
	m.Lock()
	defer m.Unlock()
	return m.stringKeys[k]
}

func commandPing(r *miniredis, srv *redeo.Server) {
	srv.HandleFunc("ping", func(out *redeo.Responder, _ *redeo.Request) error {
		out.WriteInlineString("PONG")
		return nil
	})
}

func commandGetSet(m *miniredis, srv *redeo.Server) {
	srv.HandleFunc("SET", func(out *redeo.Responder, r *redeo.Request) error {
		if len(r.Args) < 2 {
			out.WriteErrorString("Usage error")
			return nil
		}
		if len(r.Args) > 2 {
			// EX/PX/NX/XX options.
			return errUnimplemented
		}
		key := r.Args[0]
		value := r.Args[1]
		m.Lock()
		defer m.Unlock()
		m.stringKeys[key] = value
		out.WriteOK()
		return nil
	})

	srv.HandleFunc("GET", func(out *redeo.Responder, r *redeo.Request) error {
		key := r.Args[0]
		m.Lock()
		defer m.Unlock()
		value, ok := m.stringKeys[key]
		if !ok {
			out.WriteNil()
			return nil
		}
		out.WriteString(value)
		return nil
	})
}
