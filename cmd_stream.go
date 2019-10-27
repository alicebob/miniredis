// Commands from https://redis.io/commands#stream

package miniredis

import (
	"strconv"
	"strings"

	"github.com/alicebob/miniredis/v2/server"
)

// commandsStream handles all stream operations.
func commandsStream(m *Miniredis) {
	m.srv.Register("XADD", m.cmdXadd)
	m.srv.Register("XLEN", m.cmdXlen)
	m.srv.Register("XRANGE", m.makeCmdXrange(false))
	m.srv.Register("XREVRANGE", m.makeCmdXrange(true))
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

	entryDict := make([][2]string, 0, len(args)/2)
	for len(args) > 0 {
		entryDict = append(entryDict, [2]string{args[0], args[1]})
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

// XRANGE and XREVRANGE
func (m *Miniredis) makeCmdXrange(reverse bool) server.Cmd {
	return func(c *server.Peer, cmd string, args []string) {
		if len(args) < 3 {
			setDirty(c)
			c.WriteError(errWrongNumber(cmd))
			return
		}
		if len(args) == 4 || len(args) > 5 {
			setDirty(c)
			c.WriteError(msgSyntaxError)
			return
		}
		if !m.handleAuth(c) {
			return
		}
		if m.checkPubsub(c) {
			return
		}

		key := args[0]

		var start streamEntryID
		start, err := formatStreamRangeBound(args[1], true, reverse)
		if err != nil {
			setDirty(c)
			c.WriteError(err.Error())
			return
		}
		var end streamEntryID
		end, err = formatStreamRangeBound(args[2], false, reverse)
		if err != nil {
			setDirty(c)
			c.WriteError(err.Error())
			return
		}

		count := 0
		if len(args) == 5 {
			if strings.ToLower(args[3]) != "count" {
				setDirty(c)
				c.WriteError(msgSyntaxError)
				return
			}

			count, err = strconv.Atoi(args[4])
			if err != nil {
				setDirty(c)
				c.WriteError(msgInvalidInt)
				return
			}

			if count == 0 {
				c.WriteLen(0)
				return
			}
		}

		withTx(m, c, func(c *server.Peer, ctx *connCtx) {
			db := m.db(ctx.selectedDB)

			if !db.exists(key) {
				c.WriteLen(0)
				return
			}

			if db.t(key) != "stream" {
				c.WriteError(ErrWrongType.Error())
				return
			}

			var entries []streamEntry = db.streamKeys[key]
			if reverse {
				entries = reversedStreamEntries(entries)
			}

			if count == 0 {
				count = len(entries)
			}

			returnedEntries := make([]streamEntry, 0, count)
			returnedItemsCount := 0

			for _, entry := range entries {
				if len(returnedEntries) == count {
					break
				}

				if !reverse {
					// Break if entry ID > end
					if end.Less(entry.id) {
						break
					}

					// Continue if entry ID < start
					if entry.id.Less(start) {
						continue
					}
				} else {
					// Break if entry iD < end
					if entry.id.Less(end) {
						break
					}

					// Continue if entry ID > start.
					if start.Less(entry.id) {
						continue
					}
				}

				returnedEntries = append(returnedEntries, entry)
				returnedItemsCount += 1 + len(entry.values)
			}

			c.WriteLen(len(returnedEntries))
			for _, entry := range returnedEntries {
				c.WriteLen(2)
				c.WriteBulk(entry.id.String())
				c.WriteLen(2 * len(entry.values))
				for _, kv := range entry.values {
					c.WriteBulk(kv[0])
					c.WriteBulk(kv[1])
				}
			}
		})
	}
}
