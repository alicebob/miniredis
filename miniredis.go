package miniredis

import (
	"errors"
	"fmt"
	"net"
	"sync"

	"github.com/bsm/redeo"
)

var errUnimplemented = errors.New("unimplemented")

type redisDB struct {
	sync.Mutex
	keys       map[string]string            // Master map of keys with their type
	stringKeys map[string]string            // GET/SET &c. keys
	hashKeys   map[string]map[string]string // MGET/MSET &c. keys
	expire     map[string]int               // EXPIRE values
}

// Miniredis is a Redis server implementation.
type Miniredis struct {
	sync.Mutex
	closed   chan struct{}
	listen   net.Listener
	info     *redeo.ServerInfo
	dbs      map[int]*redisDB
	clientDB int            // DB id used in the direct Get(), Set() &c.
	selectDB map[uint64]int // Current DB per connection id
}

// NewMiniRedis makes a new, non-started, Miniredis object.
func NewMiniRedis() *Miniredis {
	return &Miniredis{
		closed:   make(chan struct{}),
		dbs:      map[int]*redisDB{},
		selectDB: map[uint64]int{},
	}
}

func newRedisDB() redisDB {
	return redisDB{
		keys:       map[string]string{},
		stringKeys: map[string]string{},
		hashKeys:   map[string]map[string]string{},
		expire:     map[string]int{},
	}
}

// Run creates and Start()s a Miniredis.
func Run() (*Miniredis, error) {
	m := NewMiniRedis()
	return m, m.Start()
}

// Start starts a server. It listens on a random port on localhost. See also Addr().
func (m *Miniredis) Start() error {
	m.Lock()
	defer m.Unlock()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		if l, err = net.Listen("tcp6", "[::1]:0"); err != nil {
			return fmt.Errorf("failed to listen on a port: %v", err)
		}
	}
	m.listen = l
	srv := redeo.NewServer(&redeo.Config{Addr: "localhost:0"})

	m.info = srv.Info()

	commandsConnection(m, srv)
	commandsGeneric(m, srv)
	commandsString(m, srv)
	commandsHash(m, srv)

	go func() {
		e := make(chan error)
		go srv.Serve(e, m.listen)
		<-e
		m.closed <- struct{}{}
	}()
	return nil
}

// Close shuts down a Miniredis.
func (m *Miniredis) Close() {
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

// DB returns a DB by ID.
func (m *Miniredis) DB(i int) *redisDB {
	m.Lock()
	defer m.Unlock()
	return m.db(i)
}

// get DB. No locks!
func (m *Miniredis) db(i int) *redisDB {
	if db, ok := m.dbs[i]; ok {
		return db
	}
	db := newRedisDB()
	m.dbs[i] = &db
	return &db
}

// dbFor gets the DB for a connection id.
func (m *Miniredis) dbFor(connID uint64) *redisDB {
	m.Lock()
	defer m.Unlock()
	return m.db(m.selectDB[connID])
}

// Addr returns '127.0.0.1:12345'. Can be given to a Dial()
func (m *Miniredis) Addr() string {
	m.Lock()
	defer m.Unlock()
	return m.listen.Addr().String()
}

// Host returns the host and the (random) port used.
func (m *Miniredis) Host() string {
	m.Lock()
	defer m.Unlock()
	host, _, _ := net.SplitHostPort(m.listen.Addr().String())
	return host
}

// Port returns the (random) port used.
func (m *Miniredis) Port() string {
	m.Lock()
	defer m.Unlock()
	_, port, _ := net.SplitHostPort(m.listen.Addr().String())
	return port
}

// CommandCount returns the number of processed commands.
func (m *Miniredis) CommandCount() int {
	m.Lock()
	defer m.Unlock()
	return int(m.info.TotalProcessed())
}

// CurrentConnectionCount returns the number of currently connected clients.
func (m *Miniredis) CurrentConnectionCount() int {
	m.Lock()
	defer m.Unlock()
	return m.info.ClientsLen()
}

// TotalConnectionCount returns the number of client connections since server start.
func (m *Miniredis) TotalConnectionCount() int {
	m.Lock()
	defer m.Unlock()
	return int(m.info.TotalConnections())
}
