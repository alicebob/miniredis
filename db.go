package miniredis

import (
	"sort"
	"strconv"
)

func (db *RedisDB) exists(k string) bool {
	_, ok := db.keys[k]
	return ok
}

// t gives the type of a key, or ""
func (db *RedisDB) t(k string) string {
	return db.keys[k]
}

// get returns the string key or "" on error/nonexists.
func (db *RedisDB) get(k string) string {
	if t, ok := db.keys[k]; !ok || t != "string" {
		return ""
	}
	return db.stringKeys[k]
}

// force set() a key. Does not touch expire.
func (db *RedisDB) set(k, v string) {
	db.del(k, false)
	db.keys[k] = "string"
	db.stringKeys[k] = v
	db.keyVersion[k]++
}

// change int key value
func (db *RedisDB) incr(k string, delta int) (int, error) {
	v := 0
	if sv, ok := db.stringKeys[k]; ok {
		var err error
		v, err = strconv.Atoi(sv)
		if err != nil {
			return 0, ErrIntValueError
		}
	}
	v += delta
	db.set(k, strconv.Itoa(v))
	return v, nil
}

// change float key value
func (db *RedisDB) incrfloat(k string, delta float64) (float64, error) {
	v := 0.0
	if sv, ok := db.stringKeys[k]; ok {
		var err error
		v, err = strconv.ParseFloat(sv, 64)
		if err != nil {
			return 0, ErrFloatValueError
		}
	}
	v += delta
	db.set(k, formatFloat(v))
	return v, nil
}

// move something to another db. Will return ok. Or not.
func (db *RedisDB) move(key string, to *RedisDB) bool {
	if _, ok := to.keys[key]; ok {
		return false
	}

	t, ok := db.keys[key]
	if !ok {
		return false
	}
	to.keys[key] = db.keys[key]
	switch t {
	case "string":
		to.stringKeys[key] = db.stringKeys[key]
	case "hash":
		to.hashKeys[key] = db.hashKeys[key]
	case "list":
		to.listKeys[key] = db.listKeys[key]
	case "set":
		to.setKeys[key] = db.setKeys[key]
	case "zset":
		to.sortedsetKeys[key] = db.sortedsetKeys[key]
	default:
		panic("unhandled key type")
	}
	to.keyVersion[key]++
	db.del(key, true)
	return true
}

func (db *RedisDB) rename(from, to string) error {
	t, ok := db.keys[from]
	if !ok {
		return ErrKeyNotFound
	}
	switch t {
	case "string":
		db.stringKeys[to] = db.stringKeys[from]
	case "hash":
		db.hashKeys[to] = db.hashKeys[from]
	case "list":
		db.listKeys[to] = db.listKeys[from]
	case "set":
		db.setKeys[to] = db.setKeys[from]
	case "zset":
		db.sortedsetKeys[to] = db.sortedsetKeys[from]
	default:
		panic("missing case")
	}
	db.keys[to] = db.keys[from]
	db.keyVersion[to]++

	db.del(from, true)
	return nil
}

func (db *RedisDB) lpush(k, v string) (int, error) {
	if t, ok := db.keys[k]; ok && t != "list" {
		return 0, ErrWrongType
	}
	l, ok := db.listKeys[k]
	if !ok {
		db.keys[k] = "list"
		l = []string{}
	}
	l = append([]string{v}, l...)
	db.listKeys[k] = l
	db.keyVersion[k]++
	return len(l), nil
}

func (db *RedisDB) lpop(k string) string {
	l := db.listKeys[k]
	el := l[0]
	l = l[1:]
	if len(l) == 0 {
		db.del(k, true)
	} else {
		db.listKeys[k] = l
	}
	db.keyVersion[k]++
	return el
}

func (db *RedisDB) push(k string, v ...string) (int, error) {
	if t, ok := db.keys[k]; ok && t != "list" {
		return 0, ErrWrongType
	}
	l, ok := db.listKeys[k]
	if !ok {
		db.keys[k] = "list"
		l = []string{}
	}
	l = append(l, v...)
	db.listKeys[k] = l
	db.keyVersion[k]++
	return len(l), nil
}

func (db *RedisDB) pop(k string) (string, error) {
	if t, ok := db.keys[k]; ok && t != "list" {
		return "", ErrWrongType
	}
	l, ok := db.listKeys[k]
	if !ok || len(l) < 1 {
		return "", ErrKeyNotFound
	}
	el := l[len(l)-1]
	l = l[:len(l)-1]
	if len(l) == 0 {
		db.del(k, true)
	} else {
		db.listKeys[k] = l
		db.keyVersion[k]++
	}
	return el, nil
}

// setadd adds members to a set. Returns nr of new keys.
func (db *RedisDB) setadd(k string, elems ...string) int {
	s, ok := db.setKeys[k]
	if !ok {
		s = setKey{}
		db.keys[k] = "set"
	}
	added := 0
	for _, e := range elems {
		if _, ok := s[e]; !ok {
			added++
		}
		s[e] = struct{}{}
	}
	db.setKeys[k] = s
	db.keyVersion[k]++
	return added
}

// setrem removes members from a set. Returns nr of deleted keys.
func (db *RedisDB) setrem(k string, fields ...string) int {
	s, ok := db.setKeys[k]
	if !ok {
		return 0
	}
	removed := 0
	for _, f := range fields {
		if _, ok := s[f]; ok {
			removed++
			delete(s, f)
		}
	}
	db.setKeys[k] = s
	db.keyVersion[k]++
	return removed
}

// All members of a set.
func (db *RedisDB) members(k string) []string {
	set := db.setKeys[k]
	members := make([]string, 0, len(set))
	for k := range set {
		members = append(members, k)
	}
	sort.Strings(members)
	return members
}

// Is a SET value present?
func (db *RedisDB) isMember(k, v string) bool {
	set, ok := db.setKeys[k]
	if !ok {
		return false
	}
	_, ok = set[v]
	return ok
}

// hkeys returns all keys ('fields') for a hash key.
func (db *RedisDB) hkeys(k string) []string {
	v := db.hashKeys[k]
	r := make([]string, 0, len(v))
	for k := range v {
		r = append(r, k)
	}
	return r
}

func (db *RedisDB) del(k string, delTTL bool) {
	if !db.exists(k) {
		return
	}
	t := db.t(k)
	delete(db.keys, k)
	db.keyVersion[k]++
	if delTTL {
		delete(db.expire, k)
	}
	switch t {
	case "string":
		delete(db.stringKeys, k)
	case "hash":
		delete(db.hashKeys, k)
	case "list":
		delete(db.listKeys, k)
	case "set":
		delete(db.setKeys, k)
	case "zset":
		delete(db.sortedsetKeys, k)
	default:
		panic("Unknown key type: " + t)
	}
}

// hset returns whether the key already existed
func (db *RedisDB) hset(k, f, v string) bool {
	if t, ok := db.keys[k]; ok && t != "hash" {
		db.del(k, true)
	}
	db.keys[k] = "hash"
	if _, ok := db.hashKeys[k]; !ok {
		db.hashKeys[k] = map[string]string{}
	}
	_, ok := db.hashKeys[k][f]
	db.hashKeys[k][f] = v
	db.keyVersion[k]++
	return ok
}

// change int key value
func (db *RedisDB) hincr(key, field string, delta int) (int, error) {
	v := 0
	if h, ok := db.hashKeys[key]; ok {
		if f, ok := h[field]; ok {
			var err error
			v, err = strconv.Atoi(f)
			if err != nil {
				return 0, ErrIntValueError
			}
		}
	}
	v += delta
	db.hset(key, field, strconv.Itoa(v))
	return v, nil
}

// change float key value
func (db *RedisDB) hincrfloat(key, field string, delta float64) (float64, error) {
	v := 0.0
	if h, ok := db.hashKeys[key]; ok {
		if f, ok := h[field]; ok {
			var err error
			v, err = strconv.ParseFloat(f, 64)
			if err != nil {
				return 0, ErrFloatValueError
			}
		}
	}
	v += delta
	db.hset(key, field, formatFloat(v))
	return v, nil
}

// sortedSet set returns a sortedSet as map
func (db *RedisDB) sortedSet(key string) map[string]float64 {
	ss := db.sortedsetKeys[key]
	return map[string]float64(ss)
}

// Add member to a sorted set. Returns whether this was a new member.
func (db *RedisDB) zadd(key string, score float64, member string) bool {
	ss, ok := db.sortedsetKeys[key]
	if !ok {
		ss = newSortedSet()
		db.keys[key] = "zset"
	}
	_, ok = ss[member]
	ss[member] = score
	db.sortedsetKeys[key] = ss
	db.keyVersion[key]++
	return !ok
}

// All members from a sorted set, ordered by score.
func (db *RedisDB) zmembers(key string) []string {
	ss, ok := db.sortedsetKeys[key]
	if !ok {
		return nil
	}
	elems := ss.byScore(asc)
	members := make([]string, 0, len(elems))
	for _, e := range elems {
		members = append(members, e.member)
	}
	return members
}

// All members+scores from a sorted set, ordered by score.
func (db *RedisDB) zelements(key string) ssElems {
	ss, ok := db.sortedsetKeys[key]
	if !ok {
		return nil
	}
	return ss.byScore(asc)
}

// sorted set cardinality
func (db *RedisDB) zcard(key string) int {
	ss := db.sortedsetKeys[key]
	return ss.card()
}

// sorted set rank
func (db *RedisDB) zrank(key, member string, d direction) (int, bool) {
	ss := db.sortedsetKeys[key]
	return ss.rankByScore(member, d)
}

// sorted set score
func (db *RedisDB) zscore(key, member string) float64 {
	ss := db.sortedsetKeys[key]
	return ss[member]
}

// sorted set key delete
func (db *RedisDB) zrem(key, member string) bool {
	ss := db.sortedsetKeys[key]
	_, ok := ss[member]
	delete(ss, member)
	if len(ss) == 0 {
		// Delete key on removal of last member
		db.del(key, true)
	}
	return ok
}

// zexists tells if a member exists in a sorted set
func (db *RedisDB) zexists(key, member string) bool {
	ss := db.sortedsetKeys[key]
	_, ok := ss[member]
	return ok
}
