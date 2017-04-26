// Commands from http://redis.io/commands#set

package miniredis

import (
	"math/rand"
	"strconv"
	"strings"

	"github.com/bsm/redeo"
	"github.com/bsm/redeo/resp"
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
	srv.HandleFunc("SUNION", m.cmdSunion)
	srv.HandleFunc("SUNIONSTORE", m.cmdSunionstore)
	srv.HandleFunc("SSCAN", m.cmdSscan)
}

// SADD
func (m *Miniredis) cmdSadd(out resp.ResponseWriter, r *resp.Command) {
	if r.ArgN() < 2 {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgNumberOfArgs(r.Name))
		return
	}
	if !m.handleAuth(redeo.GetClient(r.Context()), out) {
		return
	}

	key := r.Arg(0).String()
	elems := asString(r.Args()[1:])

	withTx(m, out, r, func(out resp.ResponseWriter, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		if db.exists(key) && db.t(key) != "set" {
			out.AppendError(ErrWrongType.Error())
			return
		}

		added := db.setAdd(key, elems...)
		out.AppendInt(int64(added))
	})
}

// SCARD
func (m *Miniredis) cmdScard(out resp.ResponseWriter, r *resp.Command) {
	if r.ArgN() != 1 {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgNumberOfArgs(r.Name))
		return
	}
	if !m.handleAuth(redeo.GetClient(r.Context()), out) {
		return
	}

	key := r.Arg(0).String()

	withTx(m, out, r, func(out resp.ResponseWriter, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		if !db.exists(key) {
			out.AppendInt(0)
			return
		}

		if db.t(key) != "set" {
			out.AppendError(ErrWrongType.Error())
			return
		}

		members := db.setMembers(key)
		out.AppendInt(int64(len(members)))
	})
}

// SDIFF
func (m *Miniredis) cmdSdiff(out resp.ResponseWriter, r *resp.Command) {
	if r.ArgN() < 1 {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgNumberOfArgs(r.Name))
		return
	}
	if !m.handleAuth(redeo.GetClient(r.Context()), out) {
		return
	}

	keys := asString(r.Args())

	withTx(m, out, r, func(out resp.ResponseWriter, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		set, err := db.setDiff(keys)
		if err != nil {
			out.AppendError(err.Error())
			return
		}

		out.AppendArrayLen(len(set))
		for k := range set {
			out.AppendBulkString(k)
		}
	})
}

// SDIFFSTORE
func (m *Miniredis) cmdSdiffstore(out resp.ResponseWriter, r *resp.Command) {
	if r.ArgN() < 2 {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgNumberOfArgs(r.Name))
		return
	}
	if !m.handleAuth(redeo.GetClient(r.Context()), out) {
		return
	}

	dest := r.Arg(0).String()
	keys := asString(r.Args()[1:])

	withTx(m, out, r, func(out resp.ResponseWriter, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		set, err := db.setDiff(keys)
		if err != nil {
			out.AppendError(err.Error())
			return
		}

		db.del(dest, true)
		db.setSet(dest, set)
		out.AppendInt(int64(len(set)))
	})
}

// SINTER
func (m *Miniredis) cmdSinter(out resp.ResponseWriter, r *resp.Command) {
	if r.ArgN() < 1 {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgNumberOfArgs(r.Name))
		return
	}
	if !m.handleAuth(redeo.GetClient(r.Context()), out) {
		return
	}

	keys := asString(r.Args())

	withTx(m, out, r, func(out resp.ResponseWriter, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		set, err := db.setInter(keys)
		if err != nil {
			out.AppendError(err.Error())
			return
		}

		out.AppendArrayLen(len(set))
		for k := range set {
			out.AppendBulkString(k)
		}
	})
}

// SINTERSTORE
func (m *Miniredis) cmdSinterstore(out resp.ResponseWriter, r *resp.Command) {
	if r.ArgN() < 2 {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgNumberOfArgs(r.Name))
		return
	}
	if !m.handleAuth(redeo.GetClient(r.Context()), out) {
		return
	}

	dest := r.Arg(0).String()
	keys := asString(r.Args()[1:])

	withTx(m, out, r, func(out resp.ResponseWriter, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		set, err := db.setInter(keys)
		if err != nil {
			out.AppendError(err.Error())
			return
		}

		db.del(dest, true)
		db.setSet(dest, set)
		out.AppendInt(int64(len(set)))
	})
}

// SISMEMBER
func (m *Miniredis) cmdSismember(out resp.ResponseWriter, r *resp.Command) {
	if r.ArgN() != 2 {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgNumberOfArgs(r.Name))
		return
	}
	if !m.handleAuth(redeo.GetClient(r.Context()), out) {
		return
	}

	key := r.Arg(0).String()
	value := r.Arg(1).String()

	withTx(m, out, r, func(out resp.ResponseWriter, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		if !db.exists(key) {
			out.AppendInt(0)
			return
		}

		if db.t(key) != "set" {
			out.AppendError(ErrWrongType.Error())
			return
		}

		if db.setIsMember(key, value) {
			out.AppendInt(1)
			return
		}
		out.AppendInt(0)
	})
}

// SMEMBERS
func (m *Miniredis) cmdSmembers(out resp.ResponseWriter, r *resp.Command) {
	if r.ArgN() != 1 {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgNumberOfArgs(r.Name))
		return
	}
	if !m.handleAuth(redeo.GetClient(r.Context()), out) {
		return
	}

	key := r.Arg(0).String()

	withTx(m, out, r, func(out resp.ResponseWriter, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		if !db.exists(key) {
			out.AppendArrayLen(0)
			return
		}

		if db.t(key) != "set" {
			out.AppendError(ErrWrongType.Error())
			return
		}

		members := db.setMembers(key)

		out.AppendArrayLen(len(members))
		for _, elem := range members {
			out.AppendBulkString(elem)
		}
	})
}

// SMOVE
func (m *Miniredis) cmdSmove(out resp.ResponseWriter, r *resp.Command) {
	if r.ArgN() != 3 {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgNumberOfArgs(r.Name))
		return
	}
	if !m.handleAuth(redeo.GetClient(r.Context()), out) {
		return
	}

	src := r.Arg(0).String()
	dst := r.Arg(1).String()
	member := r.Arg(2).String()

	withTx(m, out, r, func(out resp.ResponseWriter, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		if !db.exists(src) {
			out.AppendInt(0)
			return
		}

		if db.t(src) != "set" {
			out.AppendError(ErrWrongType.Error())
			return
		}

		if db.exists(dst) && db.t(dst) != "set" {
			out.AppendError(ErrWrongType.Error())
			return
		}

		if !db.setIsMember(src, member) {
			out.AppendInt(0)
			return
		}
		db.setRem(src, member)
		db.setAdd(dst, member)
		out.AppendInt(1)
	})
}

// SPOP
func (m *Miniredis) cmdSpop(out resp.ResponseWriter, r *resp.Command) {
	if r.ArgN() == 0 {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgNumberOfArgs(r.Name))
		return
	}
	if !m.handleAuth(redeo.GetClient(r.Context()), out) {
		return
	}

	args := asString(r.Args())
	key := args[0]
	args = args[1:]

	withTx(m, out, r, func(out resp.ResponseWriter, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		withCount := false
		count := 1
		if len(args) > 0 {
			v, err := strconv.Atoi(args[0])
			if err != nil {
				setDirty(redeo.GetClient(r.Context()))
				out.AppendError(msgInvalidInt)
				return
			}
			count = v
			withCount = true
			args = args[1:]
		}
		if len(args) > 0 {
			setDirty(redeo.GetClient(r.Context()))
			out.AppendError(msgInvalidInt)
			return
		}

		if !db.exists(key) {
			if !withCount {
				out.AppendNil()
				return
			}
			out.AppendArrayLen(0)
			return
		}

		if db.t(key) != "set" {
			out.AppendError(ErrWrongType.Error())
			return
		}

		var deleted []string
		for i := 0; i < count; i++ {
			members := db.setMembers(key)
			if len(members) == 0 {
				break
			}
			member := members[rand.Intn(len(members))]
			db.setRem(key, member)
			deleted = append(deleted, member)
		}
		// without `count` return a single value...
		if !withCount {
			if len(deleted) == 0 {
				out.AppendNil()
				return
			}
			out.AppendBulkString(deleted[0])
			return
		}
		// ... with `count` return a list
		out.AppendArrayLen(len(deleted))
		for _, v := range deleted {
			out.AppendBulkString(v)
		}
	})
}

// SRANDMEMBER
func (m *Miniredis) cmdSrandmember(out resp.ResponseWriter, r *resp.Command) {
	if r.ArgN() < 1 {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgNumberOfArgs(r.Name))
		return
	}
	if r.ArgN() > 2 {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgSyntaxError)
		return
	}
	if !m.handleAuth(redeo.GetClient(r.Context()), out) {
		return
	}

	key := r.Arg(0).String()
	count := 0
	withCount := false
	if r.ArgN() == 2 {
		var err error
		count, err = strconv.Atoi(r.Arg(1).String())
		if err != nil {
			setDirty(redeo.GetClient(r.Context()))
			out.AppendError(msgInvalidInt)
			return
		}
		withCount = true
	}

	withTx(m, out, r, func(out resp.ResponseWriter, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		if !db.exists(key) {
			out.AppendNil()
			return
		}

		if db.t(key) != "set" {
			out.AppendError(ErrWrongType.Error())
			return
		}

		members := db.setMembers(key)
		if count < 0 {
			// Non-unique elements is allowed with negative count.
			out.AppendArrayLen(-count)
			for count != 0 {
				member := members[rand.Intn(len(members))]
				out.AppendBulkString(member)
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
			out.AppendBulkString(members[0])
			return
		}
		out.AppendArrayLen(count)
		for i := range make([]struct{}, count) {
			out.AppendBulkString(members[i])
		}
	})
}

// SREM
func (m *Miniredis) cmdSrem(out resp.ResponseWriter, r *resp.Command) {
	if r.ArgN() < 2 {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgNumberOfArgs(r.Name))
		return
	}
	if !m.handleAuth(redeo.GetClient(r.Context()), out) {
		return
	}

	key := r.Arg(0).String()
	fields := asString(r.Args()[1:])

	withTx(m, out, r, func(out resp.ResponseWriter, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		if !db.exists(key) {
			out.AppendInt(0)
			return
		}

		if db.t(key) != "set" {
			out.AppendError(ErrWrongType.Error())
			return
		}

		out.AppendInt(int64(db.setRem(key, fields...)))
	})
}

// SUNION
func (m *Miniredis) cmdSunion(out resp.ResponseWriter, r *resp.Command) {
	if r.ArgN() < 1 {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgNumberOfArgs(r.Name))
		return
	}
	if !m.handleAuth(redeo.GetClient(r.Context()), out) {
		return
	}

	keys := asString(r.Args())

	withTx(m, out, r, func(out resp.ResponseWriter, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		set, err := db.setUnion(keys)
		if err != nil {
			out.AppendError(err.Error())
			return
		}

		out.AppendArrayLen(len(set))
		for k := range set {
			out.AppendBulkString(k)
		}
	})
}

// SUNIONSTORE
func (m *Miniredis) cmdSunionstore(out resp.ResponseWriter, r *resp.Command) {
	if r.ArgN() < 2 {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgNumberOfArgs(r.Name))
		return
	}
	if !m.handleAuth(redeo.GetClient(r.Context()), out) {
		return
	}

	dest := r.Arg(0).String()
	keys := asString(r.Args()[1:])

	withTx(m, out, r, func(out resp.ResponseWriter, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		set, err := db.setUnion(keys)
		if err != nil {
			out.AppendError(err.Error())
			return
		}

		db.del(dest, true)
		db.setSet(dest, set)
		out.AppendInt(int64(len(set)))
	})
}

// SSCAN
func (m *Miniredis) cmdSscan(out resp.ResponseWriter, r *resp.Command) {
	if r.ArgN() < 2 {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgNumberOfArgs(r.Name))
		return
	}
	if !m.handleAuth(redeo.GetClient(r.Context()), out) {
		return
	}

	key := r.Arg(0).String()
	cursor, err := strconv.Atoi(r.Arg(1).String())
	if err != nil {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgInvalidCursor)
		return
	}
	// MATCH and COUNT options
	var withMatch bool
	var match string
	args := asString(r.Args()[2:])
	for len(args) > 0 {
		if strings.ToLower(args[0]) == "count" {
			if len(args) < 2 {
				setDirty(redeo.GetClient(r.Context()))
				out.AppendError(msgSyntaxError)
				return
			}
			_, err := strconv.Atoi(args[1])
			if err != nil {
				setDirty(redeo.GetClient(r.Context()))
				out.AppendError(msgInvalidInt)
				return
			}
			// We do nothing with count.
			args = args[2:]
			continue
		}
		if strings.ToLower(args[0]) == "match" {
			if len(args) < 2 {
				setDirty(redeo.GetClient(r.Context()))
				out.AppendError(msgSyntaxError)
				return
			}
			withMatch = true
			match = args[1]
			args = args[2:]
			continue
		}
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgSyntaxError)
		return
	}

	withTx(m, out, r, func(out resp.ResponseWriter, ctx *connCtx) {
		db := m.db(ctx.selectedDB)
		// We return _all_ (matched) keys every time.

		if cursor != 0 {
			// Invalid cursor.
			out.AppendArrayLen(2)
			out.AppendBulkString("0") // no next cursor
			out.AppendArrayLen(0)     // no elements
			return
		}
		if db.exists(key) && db.t(key) != "set" {
			out.AppendError(ErrWrongType.Error())
			return
		}

		members := db.setMembers(key)
		if withMatch {
			members = matchKeys(members, match)
		}

		out.AppendArrayLen(2)
		out.AppendBulkString("0") // no next cursor
		out.AppendArrayLen(len(members))
		for _, k := range members {
			out.AppendBulkString(k)
		}
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
