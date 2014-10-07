// Commands from http://redis.io/commands#set

package miniredis

import (
	"math/rand"
	"strconv"

	"github.com/bsm/redeo"
)

// commandsSet handles all set value operations.
func commandsSet(m *Miniredis, srv *redeo.Server) {
	srv.HandleFunc("SADD", m.cmdSadd)
	srv.HandleFunc("SCARD", m.cmdScard)
	srv.HandleFunc("SDIFF", m.cmdSdiff)
	srv.HandleFunc("SDIFFSTORE", m.cmdSdiffstore)
	srv.HandleFunc("SINTER", m.cmdSinter)
	srv.HandleFunc("SINTERSTORE", m.cmdSinterstore)
	srv.HandleFunc("SISMEMBER", m.cmdSismember)
	srv.HandleFunc("SMEMBERS", m.cmdSmembers)
	srv.HandleFunc("SMOVE", m.cmdSmove)
	srv.HandleFunc("SPOP", m.cmdSpop)
	srv.HandleFunc("SRANDMEMBER", m.cmdSrandmember)
	srv.HandleFunc("SREM", m.cmdSrem)
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

		if db.exists(key) && db.t(key) != "set" {
			out.WriteErrorString(ErrWrongType.Error())
			return
		}

		added := db.setadd(key, elems...)
		out.WriteInt(added)
	})
}

// SCARD
func (m *Miniredis) cmdScard(out *redeo.Responder, r *redeo.Request) error {
	if len(r.Args) != 1 {
		setDirty(r.Client())
		out.WriteErrorString("ERR wrong number of arguments for 'scard' command")
		return nil
	}

	key := r.Args[0]

	return withTx(m, out, r, func(out *redeo.Responder, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		if !db.exists(key) {
			out.WriteZero()
			return
		}

		if db.t(key) != "set" {
			out.WriteErrorString(ErrWrongType.Error())
			return
		}

		members := db.members(key)
		out.WriteInt(len(members))
	})
}

// SDIFF
func (m *Miniredis) cmdSdiff(out *redeo.Responder, r *redeo.Request) error {
	if len(r.Args) < 1 {
		setDirty(r.Client())
		out.WriteErrorString("ERR wrong number of arguments for 'sdiff' command")
		return nil
	}

	keys := r.Args

	return withTx(m, out, r, func(out *redeo.Responder, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		set, err := db.setDiff(keys)
		if err != nil {
			out.WriteErrorString(err.Error())
			return
		}

		out.WriteBulkLen(len(set))
		for k := range set {
			out.WriteString(k)
		}
	})
}

// SDIFFSTORE
func (m *Miniredis) cmdSdiffstore(out *redeo.Responder, r *redeo.Request) error {
	if len(r.Args) < 2 {
		setDirty(r.Client())
		out.WriteErrorString("ERR wrong number of arguments for 'sdiffstore' command")
		return nil
	}

	dest := r.Args[0]
	keys := r.Args[1:]

	return withTx(m, out, r, func(out *redeo.Responder, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		set, err := db.setDiff(keys)
		if err != nil {
			out.WriteErrorString(err.Error())
			return
		}

		db.del(dest, true)
		db.setset(dest, set)
		out.WriteInt(len(set))
	})
}

// SINTER
func (m *Miniredis) cmdSinter(out *redeo.Responder, r *redeo.Request) error {
	if len(r.Args) < 1 {
		setDirty(r.Client())
		out.WriteErrorString("ERR wrong number of arguments for 'sinter' command")
		return nil
	}

	keys := r.Args

	return withTx(m, out, r, func(out *redeo.Responder, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		set, err := db.setInter(keys)
		if err != nil {
			out.WriteErrorString(err.Error())
			return
		}

		out.WriteBulkLen(len(set))
		for k := range set {
			out.WriteString(k)
		}
	})
}

// SINTERSTORE
func (m *Miniredis) cmdSinterstore(out *redeo.Responder, r *redeo.Request) error {
	if len(r.Args) < 2 {
		setDirty(r.Client())
		out.WriteErrorString("ERR wrong number of arguments for 'sinterstore' command")
		return nil
	}

	dest := r.Args[0]
	keys := r.Args[1:]

	return withTx(m, out, r, func(out *redeo.Responder, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		set, err := db.setInter(keys)
		if err != nil {
			out.WriteErrorString(err.Error())
			return
		}

		db.del(dest, true)
		db.setset(dest, set)
		out.WriteInt(len(set))
	})
}

// SISMEMBER
func (m *Miniredis) cmdSismember(out *redeo.Responder, r *redeo.Request) error {
	if len(r.Args) != 2 {
		setDirty(r.Client())
		out.WriteErrorString("ERR wrong number of arguments for 'sismember' command")
		return nil
	}

	key := r.Args[0]
	value := r.Args[1]

	return withTx(m, out, r, func(out *redeo.Responder, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		if !db.exists(key) {
			out.WriteZero()
			return
		}

		if db.t(key) != "set" {
			out.WriteErrorString(ErrWrongType.Error())
			return
		}

		if db.isMember(key, value) {
			out.WriteOne()
			return
		}
		out.WriteZero()
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

		if !db.exists(key) {
			out.WriteBulkLen(0)
			return
		}

		if db.t(key) != "set" {
			out.WriteErrorString(ErrWrongType.Error())
			return
		}

		members := db.members(key)

		out.WriteBulkLen(len(members))
		for _, elem := range members {
			out.WriteString(elem)
		}
	})
}

// SMOVE
func (m *Miniredis) cmdSmove(out *redeo.Responder, r *redeo.Request) error {
	if len(r.Args) != 3 {
		setDirty(r.Client())
		out.WriteErrorString("ERR wrong number of arguments for 'smove' command")
		return nil
	}

	src := r.Args[0]
	dst := r.Args[1]
	member := r.Args[2]

	return withTx(m, out, r, func(out *redeo.Responder, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		if !db.exists(src) {
			out.WriteInt(0)
			return
		}

		if db.t(src) != "set" {
			out.WriteErrorString(ErrWrongType.Error())
			return
		}

		if db.exists(dst) && db.t(dst) != "set" {
			out.WriteErrorString(ErrWrongType.Error())
			return
		}

		if !db.isMember(src, member) {
			out.WriteInt(0)
			return
		}
		db.setrem(src, member)
		db.setadd(dst, member)
		out.WriteInt(1)
	})
}

// SPOP
func (m *Miniredis) cmdSpop(out *redeo.Responder, r *redeo.Request) error {
	if len(r.Args) != 1 {
		setDirty(r.Client())
		out.WriteErrorString("ERR wrong number of arguments for 'spop' command")
		return nil
	}

	key := r.Args[0]

	return withTx(m, out, r, func(out *redeo.Responder, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		if !db.exists(key) {
			out.WriteNil()
			return
		}

		if db.t(key) != "set" {
			out.WriteErrorString(ErrWrongType.Error())
			return
		}

		members := db.members(key)
		member := members[rand.Intn(len(members))]
		db.setrem(key, member)
		out.WriteString(member)
	})
}

// SRANDMEMBER
func (m *Miniredis) cmdSrandmember(out *redeo.Responder, r *redeo.Request) error {
	if len(r.Args) < 1 {
		setDirty(r.Client())
		out.WriteErrorString("ERR wrong number of arguments for 'srandmember' command")
		return nil
	}
	if len(r.Args) > 2 {
		setDirty(r.Client())
		out.WriteErrorString(msgSyntaxError)
		return nil
	}

	key := r.Args[0]
	count := 0
	withCount := false
	if len(r.Args) == 2 {
		var err error
		count, err = strconv.Atoi(r.Args[1])
		if err != nil {
			setDirty(r.Client())
			out.WriteErrorString(msgInvalidInt)
			return nil
		}
		withCount = true
	}

	return withTx(m, out, r, func(out *redeo.Responder, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		if !db.exists(key) {
			out.WriteNil()
			return
		}

		if db.t(key) != "set" {
			out.WriteErrorString(ErrWrongType.Error())
			return
		}

		members := db.members(key)
		if count < 0 {
			// Non-unique elements is allowed with negative count.
			out.WriteBulkLen(-count)
			for count != 0 {
				member := members[rand.Intn(len(members))]
				out.WriteString(member)
				count++
			}
			return
		}

		// Must be unique elements.
		shuffle(members)
		if count > len(members) {
			count = len(members)
		}
		if !withCount {
			out.WriteString(members[0])
			return
		}
		out.WriteBulkLen(count)
		for i := range make([]struct{}, count) {
			out.WriteString(members[i])
		}
	})
}

// SREM
func (m *Miniredis) cmdSrem(out *redeo.Responder, r *redeo.Request) error {
	if len(r.Args) < 2 {
		setDirty(r.Client())
		out.WriteErrorString("ERR wrong number of arguments for 'srem' command")
		return nil
	}

	key := r.Args[0]
	fields := r.Args[1:]

	return withTx(m, out, r, func(out *redeo.Responder, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		if !db.exists(key) {
			out.WriteInt(0)
			return
		}

		if db.t(key) != "set" {
			out.WriteErrorString(ErrWrongType.Error())
			return
		}

		out.WriteInt(db.setrem(key, fields...))
	})
}

// shuffle shuffles a string. Kinda.
func shuffle(m []string) {
	for _ = range m {
		i := rand.Intn(len(m))
		j := rand.Intn(len(m))
		m[i], m[j] = m[j], m[i]
	}
}
