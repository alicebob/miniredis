package miniredis

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
