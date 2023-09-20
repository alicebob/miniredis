package miniredis

import (
	"fmt"
	"strings"

	"github.com/alicebob/miniredis/v2/server"
)

// commandsClient handles client operations.
func commandsClient(m *Miniredis) {
	m.srv.Register("CLIENT", m.cmdClient)
}

// CLIENT
func (m *Miniredis) cmdClient(c *server.Peer, cmd string, args []string) {
	if len(args) == 0 {
		setDirty(c)
		c.WriteError("ERR wrong number of arguments for 'client' command")
		return
	}

	switch strings.ToUpper(args[0]) {
	case "SETNAME":
		m.cmdClientSetName(c, args[1:])
	case "GETNAME":
		m.cmdClientGetName(c, args[1:])
	default:
		setDirty(c)
		c.WriteError(fmt.Sprintf("ERR 'CLIENT %s' not supported", strings.Join(args, " ")))
	}
}

// CLIENT SETNAME
func (m *Miniredis) cmdClientSetName(c *server.Peer, args []string) {
	if len(args) != 1 {
		setDirty(c)
		c.WriteError("ERR wrong number of arguments for 'client setname' command")
		return
	}

	c.ClientName = args[0]
	c.WriteOK()
}

// CLIENT GETNAME
func (m *Miniredis) cmdClientGetName(c *server.Peer, args []string) {
	if len(args) > 0 {
		setDirty(c)
		c.WriteError("ERR wrong number of arguments for 'client getname' command")
		return
	}

	if c.ClientName == "" {
		c.WriteNull()
	} else {
		c.WriteBulk(c.ClientName)
	}
}
