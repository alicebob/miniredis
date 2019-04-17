// Commands from https://redis.io/commands#connection

package miniredis

import (
	"strconv"

	"github.com/alicebob/miniredis/server"
)

func commandsConnection(m *Miniredis) {
	m.srv.Register("AUTH", m.cmdAuth)
	m.srv.Register("ECHO", m.cmdEcho)
	m.srv.Register("PING", m.cmdPing)
	m.srv.Register("SELECT", m.cmdSelect)
	m.srv.Register("SWAPDB", m.cmdSwapdb)
	m.srv.Register("QUIT", m.cmdQuit)
}

// PING
func (m *Miniredis) cmdPing(c *server.Peer, cmd string, args []string) {
	if !m.handleAuth(c) {
		return
	}

	if len(args) > 1 {
		setDirty(c)
		c.WriteError(errWrongNumber(cmd))
		return
	}

	payload := ""
	if len(args) > 0 {
		payload = args[0]
	}

	// PING is allowed in subscribed state
	if sub := getCtx(c).subscriber; sub != nil {
		c.Block(func(c *server.Writer) {
			c.WriteLen(2)
			c.WriteBulk("pong")
			c.WriteBulk(payload)
		})
		return
	}

	if payload == "" {
		c.WriteInline("PONG")
		return
	}
	c.WriteBulk(payload)
}

// AUTH
func (m *Miniredis) cmdAuth(c *server.Peer, cmd string, args []string) {
	if len(args) != 1 {
		setDirty(c)
		c.WriteError(errWrongNumber(cmd))
		return
	}
	if m.checkPubsub(c) {
		return
	}

	pw := args[0]

	m.Lock()
	defer m.Unlock()
	if m.password == "" {
		c.WriteError("ERR Client sent AUTH, but no password is set")
		return
	}
	if m.password != pw {
		c.WriteError("ERR invalid password")
		return
	}

	setAuthenticated(c)
	c.WriteOK()
}

// ECHO
func (m *Miniredis) cmdEcho(c *server.Peer, cmd string, args []string) {
	if len(args) != 1 {
		setDirty(c)
		c.WriteError(errWrongNumber(cmd))
		return
	}
	if !m.handleAuth(c) {
		return
	}
	if m.checkPubsub(c) {
		return
	}

	msg := args[0]
	c.WriteBulk(msg)
}

// SELECT
func (m *Miniredis) cmdSelect(c *server.Peer, cmd string, args []string) {
	if len(args) != 1 {
		setDirty(c)
		c.WriteError(errWrongNumber(cmd))
		return
	}
	if !m.handleAuth(c) {
		return
	}
	if m.checkPubsub(c) {
		return
	}

	id, err := strconv.Atoi(args[0])
	if err != nil {
		id = 0
	}

	m.Lock()
	defer m.Unlock()

	ctx := getCtx(c)
	ctx.selectedDB = id

	c.WriteOK()
}

// SWAPDB
func (m *Miniredis) cmdSwapdb(c *server.Peer, cmd string, args []string) {
	if len(args) != 2 {
		setDirty(c)
		c.WriteError(errWrongNumber(cmd))
		return
	}
	if !m.handleAuth(c) {
		return
	}

	withTx(m, c, func(c *server.Peer, ctx *connCtx) {
		id1, err := strconv.Atoi(args[0])
		if err != nil {
			c.WriteError("ERR invalid first DB index")
			setDirty(c)
			return
		}
		id2, err := strconv.Atoi(args[1])
		if err != nil {
			c.WriteError("ERR invalid second DB index")
			setDirty(c)
			return
		}
		if id1 < 0 || id2 < 0 {
			c.WriteError("ERR DB index is out of range")
			setDirty(c)
			return
		}

		m.swapDB(id1, id2)

		c.WriteOK()
	})
}

// QUIT
func (m *Miniredis) cmdQuit(c *server.Peer, cmd string, args []string) {
	// QUIT isn't transactionfied and accepts any arguments.
	c.WriteOK()
	c.Close()
}
