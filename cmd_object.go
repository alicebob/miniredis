package miniredis

import (
	"strings"

	"github.com/alicebob/miniredis/v2/server"
)

// commandsObject handles all object operations.
func commandsObject(m *Miniredis) {
	m.srv.Register("OBJECT", m.cmdObject)
}

// OBJECT
func (m *Miniredis) cmdObject(c *server.Peer, cmd string, args []string) {
	if len(args) != 2 {
		setDirty(c)
		c.WriteError(errWrongNumber(cmd))
		return
	}
	if !m.handleAuth(c) {
		return
	}
	if m.checkPubsub(c, cmd) {
		return
	}

	switch strings.ToLower(args[0]) {
	case "idletime":
		m.cmdObjectIdletime(c, args[1])
	default:
		setDirty(c)
		c.WriteError(server.ErrUnknownCommand(cmd, args))
	}
}

// OBJECT IDLETIME
func (m *Miniredis) cmdObjectIdletime(c *server.Peer, key string) {
	withTx(m, c, func(c *server.Peer, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		t, ok := db.lru[key]
		if !ok {
			c.WriteNull()
			return
		}

		c.WriteInt(int(db.master.effectiveNow().Sub(t).Seconds()))
	})
}
