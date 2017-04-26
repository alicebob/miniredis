// Commands from http://redis.io/commands#sorted_set

package miniredis

import (
	"errors"
	"sort"
	"strconv"
	"strings"

	"github.com/bsm/redeo"
	"github.com/bsm/redeo/resp"
)

var (
	errInvalidRangeItem = errors.New(msgInvalidRangeItem)
)

// commandsSortedSet handles all sorted set operations.
func commandsSortedSet(m *Miniredis, srv *redeo.Server) {
	srv.HandleFunc("ZADD", m.cmdZadd)
	srv.HandleFunc("ZCARD", m.cmdZcard)
	srv.HandleFunc("ZCOUNT", m.cmdZcount)
	srv.HandleFunc("ZINCRBY", m.cmdZincrby)
	srv.HandleFunc("ZINTERSTORE", m.cmdZinterstore)
	srv.HandleFunc("ZLEXCOUNT", m.cmdZlexcount)
	srv.HandleFunc("ZRANGE", m.makeCmdZrange(false))
	srv.HandleFunc("ZRANGEBYLEX", m.cmdZrangebylex)
	srv.HandleFunc("ZRANGEBYSCORE", m.makeCmdZrangebyscore(false))
	srv.HandleFunc("ZRANK", m.makeCmdZrank(false))
	srv.HandleFunc("ZREM", m.cmdZrem)
	srv.HandleFunc("ZREMRANGEBYLEX", m.cmdZremrangebylex)
	srv.HandleFunc("ZREMRANGEBYRANK", m.cmdZremrangebyrank)
	srv.HandleFunc("ZREMRANGEBYSCORE", m.cmdZremrangebyscore)
	srv.HandleFunc("ZREVRANGE", m.makeCmdZrange(true))
	srv.HandleFunc("ZREVRANGEBYSCORE", m.makeCmdZrangebyscore(true))
	srv.HandleFunc("ZREVRANK", m.makeCmdZrank(true))
	srv.HandleFunc("ZSCORE", m.cmdZscore)
	srv.HandleFunc("ZUNIONSTORE", m.cmdZunionstore)
	srv.HandleFunc("ZSCAN", m.cmdZscan)
}

// ZADD
func (m *Miniredis) cmdZadd(out resp.ResponseWriter, r *resp.Command) {
	if r.ArgN() < 3 {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgNumberOfArgs(r.Name))
		return
	}
	if !m.handleAuth(redeo.GetClient(r.Context()), out) {
		return
	}

	key := r.Arg(0).String()
	args := asString(r.Args()[1:])
	var (
		nx    = false
		xx    = false
		ch    = false
		elems = map[string]float64{}
	)

	for len(args) > 0 {
		switch strings.ToUpper(args[0]) {
		case "NX":
			nx = true
			args = args[1:]
			continue
		case "XX":
			xx = true
			args = args[1:]
			continue
		case "CH":
			ch = true
			args = args[1:]
			continue
		default:
			if len(args) < 2 {
				setDirty(redeo.GetClient(r.Context()))
				out.AppendError(msgSyntaxError)
				return
			}
			score, err := strconv.ParseFloat(args[0], 64)
			if err != nil {
				setDirty(redeo.GetClient(r.Context()))
				out.AppendError(msgInvalidFloat)
				return
			}
			elems[args[1]] = score
			args = args[2:]
		}
	}

	if xx && nx {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgXXandNX)
		return
	}

	withTx(m, out, r, func(out resp.ResponseWriter, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		if db.exists(key) && db.t(key) != "zset" {
			out.AppendError(ErrWrongType.Error())
			return
		}

		res := 0
		for member, score := range elems {
			if nx && db.ssetExists(key, member) {
				continue
			}
			if xx && !db.ssetExists(key, member) {
				continue
			}
			old := db.ssetScore(key, member)
			if db.ssetAdd(key, score, member) {
				res++
			} else {
				if ch && old != score {
					// if 'CH' is specified, only count changed keys
					res++
				}
			}
		}
		out.AppendInt(int64(res))
	})
}

// ZCARD
func (m *Miniredis) cmdZcard(out resp.ResponseWriter, r *resp.Command) {
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

		if db.t(key) != "zset" {
			out.AppendError(ErrWrongType.Error())
			return
		}

		out.AppendInt(int64(db.ssetCard(key)))
	})
}

// ZCOUNT
func (m *Miniredis) cmdZcount(out resp.ResponseWriter, r *resp.Command) {
	if r.ArgN() != 3 {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgNumberOfArgs(r.Name))
		return
	}
	if !m.handleAuth(redeo.GetClient(r.Context()), out) {
		return
	}

	key := r.Arg(0).String()
	min, minIncl, err := parseFloatRange(r.Arg(1).String())
	if err != nil {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgInvalidMinMax)
		return
	}
	max, maxIncl, err := parseFloatRange(r.Arg(2).String())
	if err != nil {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgInvalidMinMax)
		return
	}

	withTx(m, out, r, func(out resp.ResponseWriter, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		if !db.exists(key) {
			out.AppendInt(0)
			return
		}

		if db.t(key) != "zset" {
			out.AppendError(ErrWrongType.Error())
			return
		}

		members := db.ssetElements(key)
		members = withSSRange(members, min, minIncl, max, maxIncl)
		out.AppendInt(int64(len(members)))
	})
}

// ZINCRBY
func (m *Miniredis) cmdZincrby(out resp.ResponseWriter, r *resp.Command) {
	if r.ArgN() != 3 {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgNumberOfArgs(r.Name))
		return
	}
	if !m.handleAuth(redeo.GetClient(r.Context()), out) {
		return
	}

	key := r.Arg(0).String()
	delta, err := r.Arg(1).Float()
	if err != nil {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgInvalidFloat)
		return
	}
	member := r.Arg(2).String()

	withTx(m, out, r, func(out resp.ResponseWriter, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		if db.exists(key) && db.t(key) != "zset" {
			out.AppendError(msgWrongType)
			return
		}
		newScore := db.ssetIncrby(key, member, delta)
		out.AppendBulkString(formatFloat(newScore))
	})
}

// ZINTERSTORE
func (m *Miniredis) cmdZinterstore(out resp.ResponseWriter, r *resp.Command) {
	if r.ArgN() < 3 {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgNumberOfArgs(r.Name))
		return
	}
	if !m.handleAuth(redeo.GetClient(r.Context()), out) {
		return
	}

	destination := r.Arg(0).String()
	numKeys, err := strconv.Atoi(r.Arg(1).String())
	if err != nil {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgInvalidInt)
		return
	}
	args := asString(r.Args()[2:])
	if len(args) < numKeys {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgSyntaxError)
		return
	}
	if numKeys <= 0 {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError("ERR at least 1 input key is needed for ZUNIONSTORE/ZINTERSTORE")
		return
	}
	keys := args[:numKeys]
	args = args[numKeys:]

	withWeights := false
	weights := []float64{}
	aggregate := "sum"
	for len(args) > 0 {
		if strings.ToLower(args[0]) == "weights" {
			if len(args) < numKeys+1 {
				setDirty(redeo.GetClient(r.Context()))
				out.AppendError(msgSyntaxError)
				return
			}
			for i := 0; i < numKeys; i++ {
				f, err := strconv.ParseFloat(args[i+1], 64)
				if err != nil {
					setDirty(redeo.GetClient(r.Context()))
					out.AppendError("ERR weight value is not a float")
					return
				}
				weights = append(weights, f)
			}
			withWeights = true
			args = args[numKeys+1:]
			continue
		}
		if strings.ToLower(args[0]) == "aggregate" {
			if len(args) < 2 {
				setDirty(redeo.GetClient(r.Context()))
				out.AppendError(msgSyntaxError)
				return
			}
			aggregate = strings.ToLower(args[1])
			switch aggregate {
			default:
				setDirty(redeo.GetClient(r.Context()))
				out.AppendError(msgSyntaxError)
				return
			case "sum", "min", "max":
			}
			args = args[2:]
			continue
		}
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgSyntaxError)
		return
	}

	withTx(m, out, r, func(out resp.ResponseWriter, ctx *connCtx) {
		db := m.db(ctx.selectedDB)
		db.del(destination, true)

		// We collect everything and remove all keys which turned out not to be
		// present in every set.
		sset := map[string]float64{}
		counts := map[string]int{}
		for i, key := range keys {
			if !db.exists(key) {
				continue
			}
			if db.t(key) != "zset" {
				out.AppendError(msgWrongType)
				return
			}
			for _, el := range db.ssetElements(key) {
				score := el.score
				if withWeights {
					score *= weights[i]
				}
				counts[el.member]++
				old, ok := sset[el.member]
				if !ok {
					sset[el.member] = score
					continue
				}
				switch aggregate {
				default:
					panic("Invalid aggregate")
				case "sum":
					sset[el.member] += score
				case "min":
					if score < old {
						sset[el.member] = score
					}
				case "max":
					if score > old {
						sset[el.member] = score
					}
				}
			}
		}
		for key, count := range counts {
			if count != numKeys {
				delete(sset, key)
			}
		}
		db.ssetSet(destination, sset)
		out.AppendInt(int64(len(sset)))
	})
}

// ZLEXCOUNT
func (m *Miniredis) cmdZlexcount(out resp.ResponseWriter, r *resp.Command) {
	if r.ArgN() != 3 {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgNumberOfArgs(r.Name))
		return
	}
	if !m.handleAuth(redeo.GetClient(r.Context()), out) {
		return
	}

	key := r.Arg(0).String()
	min, minIncl, err := parseLexrange(r.Arg(1).String())
	if err != nil {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(err.Error())
		return
	}
	max, maxIncl, err := parseLexrange(r.Arg(2).String())
	if err != nil {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(err.Error())
		return
	}
	withTx(m, out, r, func(out resp.ResponseWriter, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		if !db.exists(key) {
			out.AppendInt(0)
			return
		}

		if db.t(key) != "zset" {
			out.AppendError(ErrWrongType.Error())
			return
		}

		members := db.ssetMembers(key)
		// Just key sort. If scores are not the same we don't care.
		sort.Strings(members)
		members = withLexRange(members, min, minIncl, max, maxIncl)

		out.AppendInt(int64(len(members)))
	})
}

// ZRANGE and ZREVRANGE
func (m *Miniredis) makeCmdZrange(reverse bool) redeo.HandlerFunc {
	return func(out resp.ResponseWriter, r *resp.Command) {
		if r.ArgN() < 3 {
			setDirty(redeo.GetClient(r.Context()))
			out.AppendError(msgNumberOfArgs(r.Name))
			return
		}
		if !m.handleAuth(redeo.GetClient(r.Context()), out) {
			return
		}

		key := r.Arg(0).String()
		start, err := strconv.Atoi(r.Arg(1).String())
		if err != nil {
			setDirty(redeo.GetClient(r.Context()))
			out.AppendError(msgInvalidInt)
			return
		}
		end, err := strconv.Atoi(r.Arg(2).String())
		if err != nil {
			setDirty(redeo.GetClient(r.Context()))
			out.AppendError(msgInvalidInt)
			return
		}

		withScores := false
		if r.ArgN() > 4 {
			out.AppendError(msgSyntaxError)
			return
		}
		if r.ArgN() == 4 {
			if strings.ToLower(r.Arg(3).String()) != "withscores" {
				setDirty(redeo.GetClient(r.Context()))
				out.AppendError(msgSyntaxError)
				return
			}
			withScores = true
		}

		withTx(m, out, r, func(out resp.ResponseWriter, ctx *connCtx) {
			db := m.db(ctx.selectedDB)

			if !db.exists(key) {
				out.AppendArrayLen(0)
				return
			}

			if db.t(key) != "zset" {
				out.AppendError(ErrWrongType.Error())
				return
			}

			members := db.ssetMembers(key)
			if reverse {
				reverseSlice(members)
			}
			rs, re := redisRange(len(members), start, end, false)
			if withScores {
				out.AppendArrayLen((re - rs) * 2)
			} else {
				out.AppendArrayLen(re - rs)
			}
			for _, el := range members[rs:re] {
				out.AppendBulkString(el)
				if withScores {
					out.AppendBulkString(formatFloat(db.ssetScore(key, el)))
				}
			}
		})
	}
}

// ZRANGEBYLEX
func (m *Miniredis) cmdZrangebylex(out resp.ResponseWriter, r *resp.Command) {
	if r.ArgN() < 3 {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgNumberOfArgs(r.Name))
		return
	}
	if !m.handleAuth(redeo.GetClient(r.Context()), out) {
		return
	}

	key := r.Arg(0).String()
	min, minIncl, err := parseLexrange(r.Arg(1).String())
	if err != nil {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(err.Error())
		return
	}
	max, maxIncl, err := parseLexrange(r.Arg(2).String())
	if err != nil {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(err.Error())
		return
	}

	args := asString(r.Args()[3:])
	withLimit := false
	limitStart := 0
	limitEnd := 0
	for len(args) > 0 {
		if strings.ToLower(args[0]) == "limit" {
			withLimit = true
			args = args[1:]
			if len(args) < 2 {
				out.AppendError(msgSyntaxError)
				return
			}
			limitStart, err = strconv.Atoi(args[0])
			if err != nil {
				setDirty(redeo.GetClient(r.Context()))
				out.AppendError(msgInvalidInt)
				return
			}
			limitEnd, err = strconv.Atoi(args[1])
			if err != nil {
				setDirty(redeo.GetClient(r.Context()))
				out.AppendError(msgInvalidInt)
				return
			}
			args = args[2:]
			continue
		}
		// Syntax error
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgSyntaxError)
		return
	}

	withTx(m, out, r, func(out resp.ResponseWriter, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		if !db.exists(key) {
			out.AppendArrayLen(0)
			return
		}

		if db.t(key) != "zset" {
			out.AppendError(ErrWrongType.Error())
			return
		}

		members := db.ssetMembers(key)
		// Just key sort. If scores are not the same we don't care.
		sort.Strings(members)
		members = withLexRange(members, min, minIncl, max, maxIncl)

		// Apply LIMIT ranges. That's <start> <elements>. Unlike RANGE.
		if withLimit {
			if limitStart < 0 {
				members = nil
			} else {
				if limitStart < len(members) {
					members = members[limitStart:]
				} else {
					// out of range
					members = nil
				}
				if limitEnd >= 0 {
					if len(members) > limitEnd {
						members = members[:limitEnd]
					}
				}
			}
		}

		out.AppendArrayLen(len(members))
		for _, el := range members {
			out.AppendBulkString(el)
		}
	})
}

// ZRANGEBYSCORE and ZREVRANGEBYSCORE
func (m *Miniredis) makeCmdZrangebyscore(reverse bool) redeo.HandlerFunc {
	return func(out resp.ResponseWriter, r *resp.Command) {
		if r.ArgN() < 3 {
			setDirty(redeo.GetClient(r.Context()))
			out.AppendError(msgNumberOfArgs(r.Name))
			return
		}
		if !m.handleAuth(redeo.GetClient(r.Context()), out) {
			return
		}

		key := r.Arg(0).String()
		min, minIncl, err := parseFloatRange(r.Arg(1).String())
		if err != nil {
			setDirty(redeo.GetClient(r.Context()))
			out.AppendError(msgInvalidMinMax)
			return
		}
		max, maxIncl, err := parseFloatRange(r.Arg(2).String())
		if err != nil {
			setDirty(redeo.GetClient(r.Context()))
			out.AppendError(msgInvalidMinMax)
			return
		}

		args := asString(r.Args()[3:])
		withScores := false
		withLimit := false
		limitStart := 0
		limitEnd := 0
		for len(args) > 0 {
			if strings.ToLower(args[0]) == "limit" {
				withLimit = true
				args = args[1:]
				if len(args) < 2 {
					out.AppendError(msgSyntaxError)
					return
				}
				limitStart, err = strconv.Atoi(args[0])
				if err != nil {
					setDirty(redeo.GetClient(r.Context()))
					out.AppendError(msgInvalidInt)
					return
				}
				limitEnd, err = strconv.Atoi(args[1])
				if err != nil {
					setDirty(redeo.GetClient(r.Context()))
					out.AppendError(msgInvalidInt)
					return
				}
				args = args[2:]
				continue
			}
			if strings.ToLower(args[0]) == "withscores" {
				withScores = true
				args = args[1:]
				continue
			}
			// Syntax error
			setDirty(redeo.GetClient(r.Context()))
			out.AppendError(msgSyntaxError)
			return
		}

		withTx(m, out, r, func(out resp.ResponseWriter, ctx *connCtx) {
			db := m.db(ctx.selectedDB)

			if !db.exists(key) {
				out.AppendArrayLen(0)
				return
			}

			if db.t(key) != "zset" {
				out.AppendError(ErrWrongType.Error())
				return
			}

			members := db.ssetElements(key)
			if reverse {
				min, max = max, min
				minIncl, maxIncl = maxIncl, minIncl
			}
			members = withSSRange(members, min, minIncl, max, maxIncl)
			if reverse {
				reverseElems(members)
			}

			// Apply LIMIT ranges. That's <start> <elements>. Unlike RANGE.
			if withLimit {
				if limitStart < 0 {
					members = ssElems{}
				} else {
					if limitStart < len(members) {
						members = members[limitStart:]
					} else {
						// out of range
						members = ssElems{}
					}
					if limitEnd >= 0 {
						if len(members) > limitEnd {
							members = members[:limitEnd]
						}
					}
				}
			}

			if withScores {
				out.AppendArrayLen(len(members) * 2)
			} else {
				out.AppendArrayLen(len(members))
			}
			for _, el := range members {
				out.AppendBulkString(el.member)
				if withScores {
					out.AppendBulkString(formatFloat(el.score))
				}
			}
		})
	}
}

// ZRANK and ZREVRANK
func (m *Miniredis) makeCmdZrank(reverse bool) redeo.HandlerFunc {
	return func(out resp.ResponseWriter, r *resp.Command) {
		if r.ArgN() != 2 {
			setDirty(redeo.GetClient(r.Context()))
			out.AppendError(msgNumberOfArgs(r.Name))
			return
		}
		if !m.handleAuth(redeo.GetClient(r.Context()), out) {
			return
		}

		key := r.Arg(0).String()
		member := r.Arg(1).String()

		withTx(m, out, r, func(out resp.ResponseWriter, ctx *connCtx) {
			db := m.db(ctx.selectedDB)

			if !db.exists(key) {
				out.AppendNil()
				return
			}

			if db.t(key) != "zset" {
				out.AppendError(ErrWrongType.Error())
				return
			}

			direction := asc
			if reverse {
				direction = desc
			}
			rank, ok := db.ssetRank(key, member, direction)
			if !ok {
				out.AppendNil()
				return
			}
			out.AppendInt(int64(rank))
		})
	}
}

// ZREM
func (m *Miniredis) cmdZrem(out resp.ResponseWriter, r *resp.Command) {
	if r.ArgN() < 2 {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgNumberOfArgs(r.Name))
		return
	}
	if !m.handleAuth(redeo.GetClient(r.Context()), out) {
		return
	}

	key := r.Arg(0).String()
	members := asString(r.Args()[1:])

	withTx(m, out, r, func(out resp.ResponseWriter, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		if !db.exists(key) {
			out.AppendInt(0)
			return
		}

		if db.t(key) != "zset" {
			out.AppendError(ErrWrongType.Error())
			return
		}

		deleted := 0
		for _, member := range members {
			if db.ssetRem(key, member) {
				deleted++
			}
		}
		out.AppendInt(int64(deleted))
	})
}

// ZREMRANGEBYLEX
func (m *Miniredis) cmdZremrangebylex(out resp.ResponseWriter, r *resp.Command) {
	if r.ArgN() != 3 {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgNumberOfArgs(r.Name))
		return
	}
	if !m.handleAuth(redeo.GetClient(r.Context()), out) {
		return
	}

	key := r.Arg(0).String()
	min, minIncl, err := parseLexrange(r.Arg(1).String())
	if err != nil {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(err.Error())
		return
	}
	max, maxIncl, err := parseLexrange(r.Arg(2).String())
	if err != nil {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(err.Error())
		return
	}

	withTx(m, out, r, func(out resp.ResponseWriter, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		if !db.exists(key) {
			out.AppendInt(0)
			return
		}

		if db.t(key) != "zset" {
			out.AppendError(ErrWrongType.Error())
			return
		}

		members := db.ssetMembers(key)
		// Just key sort. If scores are not the same we don't care.
		sort.Strings(members)
		members = withLexRange(members, min, minIncl, max, maxIncl)

		for _, el := range members {
			db.ssetRem(key, el)
		}
		out.AppendInt(int64(len(members)))
	})
}

// ZREMRANGEBYRANK
func (m *Miniredis) cmdZremrangebyrank(out resp.ResponseWriter, r *resp.Command) {
	if r.ArgN() != 3 {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgNumberOfArgs(r.Name))
		return
	}
	if !m.handleAuth(redeo.GetClient(r.Context()), out) {
		return
	}

	key := r.Arg(0).String()
	start, err := strconv.Atoi(r.Arg(1).String())
	if err != nil {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgInvalidInt)
		return
	}
	end, err := strconv.Atoi(r.Arg(2).String())
	if err != nil {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgInvalidInt)
		return
	}

	withTx(m, out, r, func(out resp.ResponseWriter, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		if !db.exists(key) {
			out.AppendInt(0)
			return
		}

		if db.t(key) != "zset" {
			out.AppendError(ErrWrongType.Error())
			return
		}

		members := db.ssetMembers(key)
		rs, re := redisRange(len(members), start, end, false)
		for _, el := range members[rs:re] {
			db.ssetRem(key, el)
		}
		out.AppendInt(int64(re - rs))
	})
}

// ZREMRANGEBYSCORE
func (m *Miniredis) cmdZremrangebyscore(out resp.ResponseWriter, r *resp.Command) {
	if r.ArgN() != 3 {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgNumberOfArgs(r.Name))
		return
	}
	if !m.handleAuth(redeo.GetClient(r.Context()), out) {
		return
	}

	key := r.Arg(0).String()
	min, minIncl, err := parseFloatRange(r.Arg(1).String())
	if err != nil {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgInvalidMinMax)
		return
	}
	max, maxIncl, err := parseFloatRange(r.Arg(2).String())
	if err != nil {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgInvalidMinMax)
		return
	}

	withTx(m, out, r, func(out resp.ResponseWriter, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		if !db.exists(key) {
			out.AppendInt(0)
			return
		}

		if db.t(key) != "zset" {
			out.AppendError(ErrWrongType.Error())
			return
		}

		members := db.ssetElements(key)
		members = withSSRange(members, min, minIncl, max, maxIncl)

		for _, el := range members {
			db.ssetRem(key, el.member)
		}
		out.AppendInt(int64(len(members)))
	})
}

// ZSCORE
func (m *Miniredis) cmdZscore(out resp.ResponseWriter, r *resp.Command) {
	if r.ArgN() != 2 {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgNumberOfArgs(r.Name))
		return
	}
	if !m.handleAuth(redeo.GetClient(r.Context()), out) {
		return
	}

	key := r.Arg(0).String()
	member := r.Arg(1).String()

	withTx(m, out, r, func(out resp.ResponseWriter, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		if !db.exists(key) {
			out.AppendNil()
			return
		}

		if db.t(key) != "zset" {
			out.AppendError(ErrWrongType.Error())
			return
		}

		if !db.ssetExists(key, member) {
			out.AppendNil()
			return
		}

		out.AppendBulkString(formatFloat(db.ssetScore(key, member)))
	})
}

func reverseSlice(o []string) {
	for i := range make([]struct{}, len(o)/2) {
		other := len(o) - 1 - i
		o[i], o[other] = o[other], o[i]
	}
}

func reverseElems(o ssElems) {
	for i := range make([]struct{}, len(o)/2) {
		other := len(o) - 1 - i
		o[i], o[other] = o[other], o[i]
	}
}

// parseFloatRange handles ZRANGEBYSCORE floats. They are inclusive unless the
// string starts with '('
func parseFloatRange(s string) (float64, bool, error) {
	if len(s) == 0 {
		return 0, false, nil
	}
	inclusive := true
	if s[0] == '(' {
		s = s[1:]
		inclusive = false
	}
	f, err := strconv.ParseFloat(s, 64)
	return f, inclusive, err
}

// parseLexrange handles ZRANGEBYLEX ranges. They start with '[', '(', or are
// '+' or '-'.
// Returns range, inclusive, error.
// On '+' or '-' that's just returned.
func parseLexrange(s string) (string, bool, error) {
	if len(s) == 0 {
		return "", false, errInvalidRangeItem
	}
	if s == "+" || s == "-" {
		return s, false, nil
	}
	switch s[0] {
	case '(':
		return s[1:], false, nil
	case '[':
		return s[1:], true, nil
	default:
		return "", false, errInvalidRangeItem
	}
}

// withSSRange limits a list of sorted set elements by the ZRANGEBYSCORE range
// logic.
func withSSRange(members ssElems, min float64, minIncl bool, max float64, maxIncl bool) ssElems {
	gt := func(a, b float64) bool { return a > b }
	gteq := func(a, b float64) bool { return a >= b }

	mincmp := gt
	if minIncl {
		mincmp = gteq
	}
	for i, m := range members {
		if mincmp(m.score, min) {
			members = members[i:]
			goto checkmax
		}
	}
	// all elements were smaller
	return nil

checkmax:
	maxcmp := gteq
	if maxIncl {
		maxcmp = gt
	}
	for i, m := range members {
		if maxcmp(m.score, max) {
			members = members[:i]
			break
		}
	}

	return members
}

// withLexRange limits a list of sorted set elements.
func withLexRange(members []string, min string, minIncl bool, max string, maxIncl bool) []string {
	if max == "-" || min == "+" {
		return nil
	}
	if min != "-" {
		if minIncl {
			for i, m := range members {
				if m >= min {
					members = members[i:]
					break
				}
			}
		} else {
			// Excluding min
			for i, m := range members {
				if m > min {
					members = members[i:]
					break
				}
			}
		}
	}
	if max != "+" {
		if maxIncl {
			for i, m := range members {
				if m > max {
					members = members[:i]
					break
				}
			}
		} else {
			// Excluding max
			for i, m := range members {
				if m >= max {
					members = members[:i]
					break
				}
			}
		}
	}
	return members
}

// ZUNIONSTORE
func (m *Miniredis) cmdZunionstore(out resp.ResponseWriter, r *resp.Command) {
	if r.ArgN() < 3 {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgNumberOfArgs(r.Name))
		return
	}
	if !m.handleAuth(redeo.GetClient(r.Context()), out) {
		return
	}

	destination := r.Arg(0).String()
	numKeys, err := strconv.Atoi(r.Arg(1).String())
	if err != nil {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgInvalidInt)
		return
	}
	args := asString(r.Args()[2:])
	if len(args) < numKeys {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgSyntaxError)
		return
	}
	if numKeys <= 0 {
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError("ERR at least 1 input key is needed for ZUNIONSTORE/ZINTERSTORE")
		return
	}
	keys := args[:numKeys]
	args = args[numKeys:]

	withWeights := false
	weights := []float64{}
	aggregate := "sum"
	for len(args) > 0 {
		if strings.ToLower(args[0]) == "weights" {
			if len(args) < numKeys+1 {
				setDirty(redeo.GetClient(r.Context()))
				out.AppendError(msgSyntaxError)
				return
			}
			for i := 0; i < numKeys; i++ {
				f, err := strconv.ParseFloat(args[i+1], 64)
				if err != nil {
					setDirty(redeo.GetClient(r.Context()))
					out.AppendError("ERR weight value is not a float")
					return
				}
				weights = append(weights, f)
			}
			withWeights = true
			args = args[numKeys+1:]
			continue
		}
		if strings.ToLower(args[0]) == "aggregate" {
			if len(args) < 2 {
				setDirty(redeo.GetClient(r.Context()))
				out.AppendError(msgSyntaxError)
				return
			}
			aggregate = strings.ToLower(args[1])
			switch aggregate {
			default:
				setDirty(redeo.GetClient(r.Context()))
				out.AppendError(msgSyntaxError)
				return
			case "sum", "min", "max":
			}
			args = args[2:]
			continue
		}
		setDirty(redeo.GetClient(r.Context()))
		out.AppendError(msgSyntaxError)
		return
	}

	withTx(m, out, r, func(out resp.ResponseWriter, ctx *connCtx) {
		db := m.db(ctx.selectedDB)
		db.del(destination, true)

		sset := sortedSet{}
		for i, key := range keys {
			if !db.exists(key) {
				continue
			}
			if db.t(key) != "zset" {
				out.AppendError(msgWrongType)
				return
			}
			for _, el := range db.ssetElements(key) {
				score := el.score
				if withWeights {
					score *= weights[i]
				}
				old, ok := sset[el.member]
				if !ok {
					sset[el.member] = score
					continue
				}
				switch aggregate {
				default:
					panic("Invalid aggregate")
				case "sum":
					sset[el.member] += score
				case "min":
					if score < old {
						sset[el.member] = score
					}
				case "max":
					if score > old {
						sset[el.member] = score
					}
				}
			}
		}
		db.ssetSet(destination, sset)
		out.AppendInt(int64(sset.card()))
	})
}

// ZSCAN
func (m *Miniredis) cmdZscan(out resp.ResponseWriter, r *resp.Command) {
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
	var (
		withMatch bool
		match     string
		args      = asString(r.Args()[2:])
	)
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
		if db.exists(key) && db.t(key) != "zset" {
			out.AppendError(ErrWrongType.Error())
			return
		}

		members := db.ssetMembers(key)
		if withMatch {
			members = matchKeys(members, match)
		}

		out.AppendArrayLen(2)
		out.AppendBulkString("0") // no next cursor
		// HSCAN gives key, values.
		out.AppendArrayLen(len(members) * 2)
		for _, k := range members {
			out.AppendBulkString(k)
			out.AppendBulkString(formatFloat(db.ssetScore(key, k)))
		}
	})
}
