// Commands from https://redis.io/commands#stream

package miniredis

import (
	"strings"

	"github.com/alicebob/miniredis/v2/server"
)

// commandsStream handles all stream operations.
func commandsStream(m *Miniredis) {
	m.srv.Register("XADD", m.cmdXadd)
	// XRANGE key start end [COUNT count]
	// XREVRANGE key end start [COUNT count]
	m.srv.Register("XLEN", m.cmdXlen)
}

// XADD
func (m *Miniredis) cmdXadd(c *server.Peer, cmd string, args []string) {
	if len(args) < 4 {
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

	key, id, args := args[0], args[1], args[2:]
	var entryID streamEntryID

	if strings.ToLower(id) == "maxlen" {
		setDirty(c)
		c.WriteError("ERR option MAXLEN is not supported")
		return
	}
	if id != "*" {
		var err error
		entryID, err = formatStreamEntryID(id)
		if err != nil {
			setDirty(c)
			c.WriteError(err.Error())
			return
		}
	}

	// args must be composed of field/value pairs.
	if len(args) == 0 || len(args)%2 != 0 {
		setDirty(c)
		c.WriteError(errWrongNumber(cmd))
		return
	}

	entryDict := make(map[string]string)
	for len(args) > 0 {
		entryDict[args[0]] = args[1]
		args = args[2:]
	}

	withTx(m, c, func(c *server.Peer, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		if db.exists(key) && db.t(key) != "stream" {
			c.WriteError(ErrWrongType.Error())
			return
		}

		newID, err := db.streamAdd(key, entryID, entryDict)
		if err != nil {
			c.WriteError(err.Error())
			return
		}

		c.WriteBulk(newID)
	})
}

// XLEN
func (m *Miniredis) cmdXlen(c *server.Peer, cmd string, args []string) {
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

	key := args[0]

	withTx(m, c, func(c *server.Peer, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		t, ok := db.keys[key]
		if !ok {
			// No such key. That's zero length.
			c.WriteInt(0)
			return
		}
		if t != "stream" {
			c.WriteError(msgWrongType)
			return
		}

		c.WriteInt(len(db.streamKeys[key]))
	})
}
