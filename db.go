package miniredis

import (
	"errors"
)

var (
	errWrongType = errors.New(msgWrongType)
)

// internal, non-locked lpush.
func (db *RedisDB) lpush(k, v string) (int, error) {
	if t, ok := db.keys[k]; ok && t != "list" {
		return 0, errWrongType
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
