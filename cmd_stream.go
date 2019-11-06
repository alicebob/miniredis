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

	key, entryID, args := args[0], args[1], args[2:]

	if strings.ToLower(entryID) == "maxlen" {
		setDirty(c)
		c.WriteError("ERR option MAXLEN is not supported")
		return
	}

	// args must be composed of field/value pairs.
	if len(args) == 0 || len(args)%2 != 0 {
		setDirty(c)
		c.WriteError("ERR wrong number of arguments for XADD") // non-default message
		return
	}

	var values []string
	for len(args) > 0 {
		values = append(values, args[0], args[1])
		args = args[2:]
	}

	withTx(m, c, func(c *server.Peer, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		if db.exists(key) && db.t(key) != "stream" {
			c.WriteError(ErrWrongType.Error())
			return
		}

		newID, err := db.streamAdd(key, entryID, values)
		if err != nil {
			switch err {
			case errInvalidEntryID:
				c.WriteError(msgInvalidStreamID)
			case errInvalidStreamValue:
				c.WriteError(msgStreamIDTooSmall)
			default:
				c.WriteError(err.Error())
			}
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

		var (
			key      = args[0]
			startKey = args[1]
			endKey   = args[2]
		)

		countArg := "0"
		if len(args) == 5 {
			if strings.ToLower(args[3]) != "count" {
				setDirty(c)
				c.WriteError(msgSyntaxError)
				return
			}
			countArg = args[4]
		}

		withTx(m, c, func(c *server.Peer, ctx *connCtx) {

			start, err := formatStreamRangeBound(startKey, true, reverse)
			if err != nil {
				c.WriteError(msgInvalidStreamID)
				return
			}
			end, err := formatStreamRangeBound(endKey, false, reverse)
			if err != nil {
				c.WriteError(msgInvalidStreamID)
				return
			}
			count, err := strconv.Atoi(countArg)
			if err != nil {
				c.WriteError(msgInvalidInt)
				return
			}

			db := m.db(ctx.selectedDB)

			if !db.exists(key) {
				c.WriteLen(0)
				return
			}

			if db.t(key) != "stream" {
				c.WriteError(ErrWrongType.Error())
				return
			}

			var entries = db.streamKeys[key]
			if reverse {
				entries = reversedStreamEntries(entries)
			}
			if count == 0 {
				count = len(entries)
			}

			returnedEntries := make([]StreamEntry, 0, count)

			for _, entry := range entries {
				if len(returnedEntries) == count {
					break
				}

				if !reverse {
					// Break if entry ID > end
					if streamCmp(entry.ID, end) == 1 {
						break
					}

					// Continue if entry ID < start
					if streamCmp(entry.ID, start) == -1 {
						continue
					}
				} else {
					// Break if entry iD < end
					if streamCmp(entry.ID, end) == -1 {
						break
					}

					// Continue if entry ID > start.
					if streamCmp(entry.ID, start) == 1 {
						continue
					}
				}

				returnedEntries = append(returnedEntries, entry)
			}

			c.WriteLen(len(returnedEntries))
			for _, entry := range returnedEntries {
				c.WriteLen(2)
				c.WriteBulk(entry.ID)
				c.WriteLen(len(entry.Values))
				for _, v := range entry.Values {
					c.WriteBulk(v)
				}
			}
		})
	}
}
