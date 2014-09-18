package miniredis

import (
	"sort"
)

func (db *RedisDB) exists(k string) bool {
	_, ok := db.keys[k]
	return ok
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
func (db *RedisDB) setadd(k string, elems ...string) (int, error) {
	if t, ok := db.keys[k]; ok && t != "set" {
		return 0, ErrWrongType
	}

	s, ok := db.setKeys[k]
	if !ok {
		s = setKey{}
	}
	added := 0
	for _, e := range elems {
		if _, ok := s[e]; !ok {
			added++
		}
		s[e] = struct{}{}
	}
	db.keys[k] = "set"
	db.setKeys[k] = s
	db.keyVersion[k]++
	return added, nil
}

// All members of a set.
func (db *RedisDB) members(k string) ([]string, error) {
	if t, ok := db.keys[k]; ok && t != "set" {
		return nil, ErrWrongType
	}
	set, ok := db.setKeys[k]
	if !ok || len(set) < 1 {
		return nil, ErrKeyNotFound
	}
	members := make([]string, 0, len(set))
	for k := range set {
		members = append(members, k)
	}
	sort.Strings(members)
	return members, nil
}

// Is a value present?
func (db *RedisDB) isMember(k, v string) (bool, error) {
	if t, ok := db.keys[k]; ok && t != "set" {
		return false, ErrWrongType
	}
	set, ok := db.setKeys[k]
	if !ok {
		return false, ErrKeyNotFound
	}
	_, ok = set[v]
	return ok, nil
}
