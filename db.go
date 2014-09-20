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

func (db *RedisDB) lpop(k string) (string, error) {
	if t, ok := db.keys[k]; ok && t != "list" {
		return "", ErrWrongType
	}
	l, ok := db.listKeys[k]
	if !ok || len(l) < 1 {
		return "", ErrKeyNotFound
	}
	el := l[0]
	l = l[1:]
	if len(l) == 0 {
		db.del(k, true)
	} else {
		db.listKeys[k] = l
		db.keyVersion[k]++
	}
	return el, nil
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
