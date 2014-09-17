// Commands from http://redis.io/commands#set

package miniredis

import (
	"github.com/bsm/redeo"
)

// commandsSet handles all set value operations.
func commandsSet(m *Miniredis, srv *redeo.Server) {
	srv.HandleFunc("SADD", m.cmdSadd)
	// SCARD key
	// SDIFF key [key ...]
	// SDIFFSTORE destination key [key ...]
	// SINTER key [key ...]
	// SINTERSTORE destination key [key ...]
	// SISMEMBER key member
	srv.HandleFunc("SMEMBERS", m.cmdSmembers)
	// SMOVE source destination member
	// SPOP key
	// SRANDMEMBER key [count]
	// SREM key member [member ...]
	// SUNION key [key ...]
	// SUNIONSTORE destination key [key ...]
	// SSCAN key cursor [MATCH pattern] [COUNT count]
}

// SADD
func (m *Miniredis) cmdSadd(out *redeo.Responder, r *redeo.Request) error {
	if len(r.Args) < 2 {
		setDirty(r.Client())
		out.WriteErrorString("ERR wrong number of arguments for 'sadd' command")
		return nil
	}

	key := r.Args[0]
	elems := r.Args[1:]

	return withTx(m, out, r, func(out *redeo.Responder, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		added, err := db.setadd(key, elems...)
		if err != nil {
			out.WriteErrorString(err.Error())
			return
		}

		out.WriteInt(added)
	})
}

// SMEMBERS
func (m *Miniredis) cmdSmembers(out *redeo.Responder, r *redeo.Request) error {
	if len(r.Args) != 1 {
		setDirty(r.Client())
		out.WriteErrorString("ERR wrong number of arguments for 'smembers' command")
		return nil
	}

	key := r.Args[0]

	return withTx(m, out, r, func(out *redeo.Responder, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		members, err := db.members(key)
		if err != nil {
			if err == ErrKeyNotFound {
				out.WriteBulkLen(0)
				return
			}
			out.WriteErrorString(err.Error())
			return
		}

		out.WriteBulkLen(len(members))
		for _, elem := range members {
			out.WriteString(elem)
		}
	})
}
