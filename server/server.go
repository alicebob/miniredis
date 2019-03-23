package server

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"sync"
	"unicode"
)

func errUnknownCommand(cmd string, args []string) string {
	s := fmt.Sprintf("ERR unknown command `%s`, with args beginning with: ", cmd)
	if len(args) > 20 {
		args = args[:20]
	}
	for _, a := range args {
		s += fmt.Sprintf("`%s`, ", a)
	}
	return s
}

// Cmd is what Register expects
type Cmd func(c *Peer, cmd string, args []string)

type DisconnectHandler func(c *Peer)

// Server is a simple redis server
type Server struct {
	l         net.Listener
	cmds      map[string]Cmd
	peers     map[net.Conn]struct{}
	mu        sync.Mutex
	wg        sync.WaitGroup
	infoConns int
	infoCmds  int
}

// NewServer makes a server listening on addr. Close with .Close().
func NewServer(addr string) (*Server, error) {
	s := Server{
		cmds:  map[string]Cmd{},
		peers: map[net.Conn]struct{}{},
	}

	l, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	s.l = l

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.serve(l)
	}()
	return &s, nil
}

func (s *Server) serve(l net.Listener) {
	for {
		conn, err := l.Accept()
		if err != nil {
			return
		}
		s.ServeConn(conn)
	}
}

// ServeConn handles a net.Conn. Nice with net.Pipe()
func (s *Server) ServeConn(conn net.Conn) {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer conn.Close()
		s.mu.Lock()
		s.peers[conn] = struct{}{}
		s.infoConns++
		s.mu.Unlock()

		s.servePeer(conn)

		s.mu.Lock()
		delete(s.peers, conn)
		s.mu.Unlock()
	}()
}

// Addr has the net.Addr struct
func (s *Server) Addr() *net.TCPAddr {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.l == nil {
		return nil
	}
	return s.l.Addr().(*net.TCPAddr)
}

// Close a server started with NewServer. It will wait until all clients are
// closed.
func (s *Server) Close() {
	s.mu.Lock()
	if s.l != nil {
		s.l.Close()
	}
	s.l = nil
	for c := range s.peers {
		c.Close()
	}
	s.mu.Unlock()
	s.wg.Wait()
}

// Register a command. It can't have been registered before. Safe to call on a
// running server.
func (s *Server) Register(cmd string, f Cmd) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cmd = strings.ToUpper(cmd)
	if _, ok := s.cmds[cmd]; ok {
		return fmt.Errorf("command already registered: %s", cmd)
	}
	s.cmds[cmd] = f
	return nil
}

func (s *Server) servePeer(c net.Conn) {
	r := bufio.NewReader(c)
	peer := &Peer{
		w: bufio.NewWriter(c),
	}
	defer func() {
		for _, f := range peer.onDisconnect {
			f()
		}
	}()

	for {
		args, err := readArray(r)
		if err != nil {
			return
		}
		s.dispatch(peer, args)
		peer.Flush()
		s.mu.Lock()
		closed := peer.closed
		s.mu.Unlock()
		if closed {
			c.Close()
		}
	}
}

func (s *Server) dispatch(c *Peer, args []string) {
	cmd, args := args[0], args[1:]
	cmdUp := strings.ToUpper(cmd)
	s.mu.Lock()
	cb, ok := s.cmds[cmdUp]
	s.mu.Unlock()
	if !ok {
		c.WriteError(errUnknownCommand(cmd, args))
		return
	}

	s.mu.Lock()
	s.infoCmds++
	s.mu.Unlock()
	cb(c, cmdUp, args)
}

// TotalCommands is total (known) commands since this the server started
func (s *Server) TotalCommands() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.infoCmds
}

// ClientsLen gives the number of connected clients right now
func (s *Server) ClientsLen() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.peers)
}

// TotalConnections give the number of clients connected since the server
// started, including the currently connected ones
func (s *Server) TotalConnections() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.infoConns
}

// Peer is a client connected to the server
type Peer struct {
	w            *bufio.Writer
	closed       bool
	Ctx          interface{} // anything goes, server won't touch this
	onDisconnect []func()    // list of callbacks
	mu           sync.Mutex  // for Block()
}

// Flush the write buffer. Called automatically after every redis command
func (c *Peer) Flush() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.w.Flush()
}

// Close the client connection after the current command is done.
func (c *Peer) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
}

// Register a function to execute on disconnect. There can be multiple
// functions registered.
func (c *Peer) OnDisconnect(f func()) {
	c.onDisconnect = append(c.onDisconnect, f)
}

// issue multiple calls, guarded with a mutex
func (c *Peer) Block(f func(*Writer)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	f(&Writer{c.w})
}

// WriteError writes a redis 'Error'
func (c *Peer) WriteError(e string) {
	c.Block(func(w *Writer) {
		w.WriteError(e)
	})
}

// WriteInline writes a redis inline string
func (c *Peer) WriteInline(s string) {
	c.Block(func(w *Writer) {
		w.WriteInline(s)
	})
}

// WriteOK write the inline string `OK`
func (c *Peer) WriteOK() {
	c.WriteInline("OK")
}

// WriteBulk writes a bulk string
func (c *Peer) WriteBulk(s string) {
	c.Block(func(w *Writer) {
		w.WriteBulk(s)
	})
}

// WriteNull writes a redis Null element
func (c *Peer) WriteNull() {
	c.Block(func(w *Writer) {
		w.WriteNull()
	})
}

// WriteLen starts an array with the given length
func (c *Peer) WriteLen(n int) {
	c.Block(func(w *Writer) {
		w.WriteLen(n)
	})
}

// WriteInt writes an integer
func (c *Peer) WriteInt(i int) {
	c.Block(func(w *Writer) {
		w.WriteInt(i)
	})
}

func toInline(s string) string {
	return strings.Map(func(r rune) rune {
		if unicode.IsSpace(r) {
			return ' '
		}
		return r
	}, s)
}

// A Writer is given to the callback in Block()
type Writer struct {
	w *bufio.Writer
}

// WriteError writes a redis 'Error'
func (w *Writer) WriteError(e string) {
	fmt.Fprintf(w.w, "-%s\r\n", toInline(e))
}

func (w *Writer) WriteLen(n int) {
	fmt.Fprintf(w.w, "*%d\r\n", n)
}

// WriteBulk writes a bulk string
func (w *Writer) WriteBulk(s string) {
	fmt.Fprintf(w.w, "$%d\r\n%s\r\n", len(s), s)
}

// WriteInt writes an integer
func (w *Writer) WriteInt(i int) {
	fmt.Fprintf(w.w, ":%d\r\n", i)
}

// WriteNull writes a redis Null element
func (w *Writer) WriteNull() {
	fmt.Fprintf(w.w, "$-1\r\n")
}

// WriteInline writes a redis inline string
func (w *Writer) WriteInline(s string) {
	fmt.Fprintf(w.w, "+%s\r\n", toInline(s))
}

func (w *Writer) Flush() {
	w.w.Flush()
}
