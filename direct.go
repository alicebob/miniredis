package miniredis

// Commands to modify and query our databases directly.

import (
	"errors"
	"github.com/alicebob/miniredis/server"
	"regexp"
	"sync"
	"time"
)

var (
	// ErrKeyNotFound is returned when a key doesn't exist.
	ErrKeyNotFound = errors.New(msgKeyNotFound)
	// ErrWrongType when a key is not the right type.
	ErrWrongType = errors.New(msgWrongType)
	// ErrIntValueError can returned by INCRBY
	ErrIntValueError = errors.New(msgInvalidInt)
	// ErrFloatValueError can returned by INCRBYFLOAT
	ErrFloatValueError = errors.New(msgInvalidFloat)
)

// Select sets the DB id for all direct commands.
func (m *Miniredis) Select(i int) {
	m.Lock()
	defer m.Unlock()
	m.selectedDB = i
}

// Keys returns all keys from the selected database, sorted.
func (m *Miniredis) Keys() []string {
	return m.DB(m.selectedDB).Keys()
}

// Keys returns all keys, sorted.
func (db *RedisDB) Keys() []string {
	db.master.Lock()
	defer db.master.Unlock()
	return db.allKeys()
}

// FlushAll removes all keys from all databases.
func (m *Miniredis) FlushAll() {
	m.Lock()
	defer m.Unlock()
	m.flushAll()
}

func (m *Miniredis) flushAll() {
	for _, db := range m.dbs {
		db.flush()
	}
}

// FlushDB removes all keys from the selected database.
func (m *Miniredis) FlushDB() {
	m.DB(m.selectedDB).FlushDB()
}

// FlushDB removes all keys.
func (db *RedisDB) FlushDB() {
	db.master.Lock()
	defer db.master.Unlock()
	db.flush()
}

// Get returns string keys added with SET.
func (m *Miniredis) Get(k string) (string, error) {
	return m.DB(m.selectedDB).Get(k)
}

// Get returns a string key.
func (db *RedisDB) Get(k string) (string, error) {
	db.master.Lock()
	defer db.master.Unlock()
	if !db.exists(k) {
		return "", ErrKeyNotFound
	}
	if db.t(k) != "string" {
		return "", ErrWrongType
	}
	return db.stringGet(k), nil
}

// Set sets a string key. Removes expire.
func (m *Miniredis) Set(k, v string) error {
	return m.DB(m.selectedDB).Set(k, v)
}

// Set sets a string key. Removes expire.
// Unlike redis the key can't be an existing non-string key.
func (db *RedisDB) Set(k, v string) error {
	db.master.Lock()
	defer db.master.Unlock()

	if db.exists(k) && db.t(k) != "string" {
		return ErrWrongType
	}
	db.del(k, true) // Remove expire
	db.stringSet(k, v)
	return nil
}

// Incr changes a int string value by delta.
func (m *Miniredis) Incr(k string, delta int) (int, error) {
	return m.DB(m.selectedDB).Incr(k, delta)
}

// Incr changes a int string value by delta.
func (db *RedisDB) Incr(k string, delta int) (int, error) {
	db.master.Lock()
	defer db.master.Unlock()

	if db.exists(k) && db.t(k) != "string" {
		return 0, ErrWrongType
	}

	return db.stringIncr(k, delta)
}

// Incrfloat changes a float string value by delta.
func (m *Miniredis) Incrfloat(k string, delta float64) (float64, error) {
	return m.DB(m.selectedDB).Incrfloat(k, delta)
}

// Incrfloat changes a float string value by delta.
func (db *RedisDB) Incrfloat(k string, delta float64) (float64, error) {
	db.master.Lock()
	defer db.master.Unlock()

	if db.exists(k) && db.t(k) != "string" {
		return 0, ErrWrongType
	}

	return db.stringIncrfloat(k, delta)
}

// List returns the list k, or an error if it's not there or something else.
// This is the same as the Redis command `LRANGE 0 -1`, but you can do your own
// range-ing.
func (m *Miniredis) List(k string) ([]string, error) {
	return m.DB(m.selectedDB).List(k)
}

// List returns the list k, or an error if it's not there or something else.
// This is the same as the Redis command `LRANGE 0 -1`, but you can do your own
// range-ing.
func (db *RedisDB) List(k string) ([]string, error) {
	db.master.Lock()
	defer db.master.Unlock()

	if !db.exists(k) {
		return nil, ErrKeyNotFound
	}
	if db.t(k) != "list" {
		return nil, ErrWrongType
	}
	return db.listKeys[k], nil
}

// Lpush is an unshift. Returns the new length.
func (m *Miniredis) Lpush(k, v string) (int, error) {
	return m.DB(m.selectedDB).Lpush(k, v)
}

// Lpush is an unshift. Returns the new length.
func (db *RedisDB) Lpush(k, v string) (int, error) {
	db.master.Lock()
	defer db.master.Unlock()

	if db.exists(k) && db.t(k) != "list" {
		return 0, ErrWrongType
	}
	return db.listLpush(k, v), nil
}

// Lpop is a shift. Returns the popped element.
func (m *Miniredis) Lpop(k string) (string, error) {
	return m.DB(m.selectedDB).Lpop(k)
}

// Lpop is a shift. Returns the popped element.
func (db *RedisDB) Lpop(k string) (string, error) {
	db.master.Lock()
	defer db.master.Unlock()

	if !db.exists(k) {
		return "", ErrKeyNotFound
	}
	if db.t(k) != "list" {
		return "", ErrWrongType
	}
	return db.listLpop(k), nil
}

// Push add element at the end. Is called RPUSH in redis. Returns the new length.
func (m *Miniredis) Push(k string, v ...string) (int, error) {
	return m.DB(m.selectedDB).Push(k, v...)
}

// Push add element at the end. Is called RPUSH in redis. Returns the new length.
func (db *RedisDB) Push(k string, v ...string) (int, error) {
	db.master.Lock()
	defer db.master.Unlock()

	if db.exists(k) && db.t(k) != "list" {
		return 0, ErrWrongType
	}
	return db.listPush(k, v...), nil
}

// Pop removes and returns the last element. Is called RPOP in Redis.
func (m *Miniredis) Pop(k string) (string, error) {
	return m.DB(m.selectedDB).Pop(k)
}

// Pop removes and returns the last element. Is called RPOP in Redis.
func (db *RedisDB) Pop(k string) (string, error) {
	db.master.Lock()
	defer db.master.Unlock()

	if !db.exists(k) {
		return "", ErrKeyNotFound
	}
	if db.t(k) != "list" {
		return "", ErrWrongType
	}

	return db.listPop(k), nil
}

// SetAdd adds keys to a set. Returns the number of new keys.
func (m *Miniredis) SetAdd(k string, elems ...string) (int, error) {
	return m.DB(m.selectedDB).SetAdd(k, elems...)
}

// SetAdd adds keys to a set. Returns the number of new keys.
func (db *RedisDB) SetAdd(k string, elems ...string) (int, error) {
	db.master.Lock()
	defer db.master.Unlock()
	if db.exists(k) && db.t(k) != "set" {
		return 0, ErrWrongType
	}
	return db.setAdd(k, elems...), nil
}

// Members gives all set keys. Sorted.
func (m *Miniredis) Members(k string) ([]string, error) {
	return m.DB(m.selectedDB).Members(k)
}

// Members gives all set keys. Sorted.
func (db *RedisDB) Members(k string) ([]string, error) {
	db.master.Lock()
	defer db.master.Unlock()
	if !db.exists(k) {
		return nil, ErrKeyNotFound
	}
	if db.t(k) != "set" {
		return nil, ErrWrongType
	}
	return db.setMembers(k), nil
}

// IsMember tells if value is in the set.
func (m *Miniredis) IsMember(k, v string) (bool, error) {
	return m.DB(m.selectedDB).IsMember(k, v)
}

// IsMember tells if value is in the set.
func (db *RedisDB) IsMember(k, v string) (bool, error) {
	db.master.Lock()
	defer db.master.Unlock()
	if !db.exists(k) {
		return false, ErrKeyNotFound
	}
	if db.t(k) != "set" {
		return false, ErrWrongType
	}
	return db.setIsMember(k, v), nil
}

// HKeys returns all (sorted) keys ('fields') for a hash key.
func (m *Miniredis) HKeys(k string) ([]string, error) {
	return m.DB(m.selectedDB).HKeys(k)
}

// HKeys returns all (sorted) keys ('fields') for a hash key.
func (db *RedisDB) HKeys(key string) ([]string, error) {
	db.master.Lock()
	defer db.master.Unlock()
	if !db.exists(key) {
		return nil, ErrKeyNotFound
	}
	if db.t(key) != "hash" {
		return nil, ErrWrongType
	}
	return db.hashFields(key), nil
}

// Del deletes a key and any expiration value. Returns whether there was a key.
func (m *Miniredis) Del(k string) bool {
	return m.DB(m.selectedDB).Del(k)
}

// Del deletes a key and any expiration value. Returns whether there was a key.
func (db *RedisDB) Del(k string) bool {
	db.master.Lock()
	defer db.master.Unlock()
	if !db.exists(k) {
		return false
	}
	db.del(k, true)
	return true
}

// TTL is the left over time to live. As set via EXPIRE, PEXPIRE, EXPIREAT,
// PEXPIREAT.
// 0 if not set.
func (m *Miniredis) TTL(k string) time.Duration {
	return m.DB(m.selectedDB).TTL(k)
}

// TTL is the left over time to live. As set via EXPIRE, PEXPIRE, EXPIREAT,
// PEXPIREAT.
// 0 if not set.
func (db *RedisDB) TTL(k string) time.Duration {
	db.master.Lock()
	defer db.master.Unlock()
	return db.ttl[k]
}

// SetTTL sets the TTL of a key.
func (m *Miniredis) SetTTL(k string, ttl time.Duration) {
	m.DB(m.selectedDB).SetTTL(k, ttl)
}

// SetTTL sets the time to live of a key.
func (db *RedisDB) SetTTL(k string, ttl time.Duration) {
	db.master.Lock()
	defer db.master.Unlock()
	db.ttl[k] = ttl
	db.keyVersion[k]++
}

// Type gives the type of a key, or ""
func (m *Miniredis) Type(k string) string {
	return m.DB(m.selectedDB).Type(k)
}

// Type gives the type of a key, or ""
func (db *RedisDB) Type(k string) string {
	db.master.Lock()
	defer db.master.Unlock()
	return db.t(k)
}

// Exists tells whether a key exists.
func (m *Miniredis) Exists(k string) bool {
	return m.DB(m.selectedDB).Exists(k)
}

// Exists tells whether a key exists.
func (db *RedisDB) Exists(k string) bool {
	db.master.Lock()
	defer db.master.Unlock()
	return db.exists(k)
}

// HGet returns hash keys added with HSET.
// This will return an empty string if the key is not set. Redis would return
// a nil.
// Returns empty string when the key is of a different type.
func (m *Miniredis) HGet(k, f string) string {
	return m.DB(m.selectedDB).HGet(k, f)
}

// HGet returns hash keys added with HSET.
// Returns empty string when the key is of a different type.
func (db *RedisDB) HGet(k, f string) string {
	db.master.Lock()
	defer db.master.Unlock()
	h, ok := db.hashKeys[k]
	if !ok {
		return ""
	}
	return h[f]
}

// HSet sets a hash key.
// If there is another key by the same name it will be gone.
func (m *Miniredis) HSet(k, f, v string) {
	m.DB(m.selectedDB).HSet(k, f, v)
}

// HSet sets a hash key.
// If there is another key by the same name it will be gone.
func (db *RedisDB) HSet(k, f, v string) {
	db.master.Lock()
	defer db.master.Unlock()
	db.hashSet(k, f, v)
}

// HDel deletes a hash key.
func (m *Miniredis) HDel(k, f string) {
	m.DB(m.selectedDB).HDel(k, f)
}

// HDel deletes a hash key.
func (db *RedisDB) HDel(k, f string) {
	db.master.Lock()
	defer db.master.Unlock()
	db.hdel(k, f)
}

func (db *RedisDB) hdel(k, f string) {
	if _, ok := db.hashKeys[k]; !ok {
		return
	}
	delete(db.hashKeys[k], f)
	db.keyVersion[k]++
}

// HIncr increases a key/field by delta (int).
func (m *Miniredis) HIncr(k, f string, delta int) (int, error) {
	return m.DB(m.selectedDB).HIncr(k, f, delta)
}

// HIncr increases a key/field by delta (int).
func (db *RedisDB) HIncr(k, f string, delta int) (int, error) {
	db.master.Lock()
	defer db.master.Unlock()
	return db.hashIncr(k, f, delta)
}

// HIncrfloat increases a key/field by delta (float).
func (m *Miniredis) HIncrfloat(k, f string, delta float64) (float64, error) {
	return m.DB(m.selectedDB).HIncrfloat(k, f, delta)
}

// HIncrfloat increases a key/field by delta (float).
func (db *RedisDB) HIncrfloat(k, f string, delta float64) (float64, error) {
	db.master.Lock()
	defer db.master.Unlock()
	return db.hashIncrfloat(k, f, delta)
}

// SRem removes fields from a set. Returns number of deleted fields.
func (m *Miniredis) SRem(k string, fields ...string) (int, error) {
	return m.DB(m.selectedDB).SRem(k, fields...)
}

// SRem removes fields from a set. Returns number of deleted fields.
func (db *RedisDB) SRem(k string, fields ...string) (int, error) {
	db.master.Lock()
	defer db.master.Unlock()
	if !db.exists(k) {
		return 0, ErrKeyNotFound
	}
	if db.t(k) != "set" {
		return 0, ErrWrongType
	}
	return db.setRem(k, fields...), nil
}

// ZAdd adds a score,member to a sorted set.
func (m *Miniredis) ZAdd(k string, score float64, member string) (bool, error) {
	return m.DB(m.selectedDB).ZAdd(k, score, member)
}

// ZAdd adds a score,member to a sorted set.
func (db *RedisDB) ZAdd(k string, score float64, member string) (bool, error) {
	db.master.Lock()
	defer db.master.Unlock()
	if db.exists(k) && db.t(k) != "zset" {
		return false, ErrWrongType
	}
	return db.ssetAdd(k, score, member), nil
}

// ZMembers returns all members by score
func (m *Miniredis) ZMembers(k string) ([]string, error) {
	return m.DB(m.selectedDB).ZMembers(k)
}

// ZMembers returns all members by score
func (db *RedisDB) ZMembers(k string) ([]string, error) {
	db.master.Lock()
	defer db.master.Unlock()
	if !db.exists(k) {
		return nil, ErrKeyNotFound
	}
	if db.t(k) != "zset" {
		return nil, ErrWrongType
	}
	return db.ssetMembers(k), nil
}

// SortedSet returns a raw string->float64 map.
func (m *Miniredis) SortedSet(k string) (map[string]float64, error) {
	return m.DB(m.selectedDB).SortedSet(k)
}

// SortedSet returns a raw string->float64 map.
func (db *RedisDB) SortedSet(k string) (map[string]float64, error) {
	db.master.Lock()
	defer db.master.Unlock()
	if !db.exists(k) {
		return nil, ErrKeyNotFound
	}
	if db.t(k) != "zset" {
		return nil, ErrWrongType
	}
	return db.sortedSet(k), nil
}

// ZRem deletes a member. Returns whether the was a key.
func (m *Miniredis) ZRem(k, member string) (bool, error) {
	return m.DB(m.selectedDB).ZRem(k, member)
}

// ZRem deletes a member. Returns whether the was a key.
func (db *RedisDB) ZRem(k, member string) (bool, error) {
	db.master.Lock()
	defer db.master.Unlock()
	if !db.exists(k) {
		return false, ErrKeyNotFound
	}
	if db.t(k) != "zset" {
		return false, ErrWrongType
	}
	return db.ssetRem(k, member), nil
}

// ZScore gives the score of a sorted set member.
func (m *Miniredis) ZScore(k, member string) (float64, error) {
	return m.DB(m.selectedDB).ZScore(k, member)
}

// ZScore gives the score of a sorted set member.
func (db *RedisDB) ZScore(k, member string) (float64, error) {
	db.master.Lock()
	defer db.master.Unlock()
	if !db.exists(k) {
		return 0, ErrKeyNotFound
	}
	if db.t(k) != "zset" {
		return 0, ErrWrongType
	}
	return db.ssetScore(k, member), nil
}

type Message struct {
	Channel, Message string
}

type messageQueue struct {
	sync.Mutex
	messages       []Message
	hasNewMessages chan struct{}
}

func (q *messageQueue) Enqueue(message Message) {
	q.Lock()
	defer q.Unlock()

	q.messages = append(q.messages, message)

	select {
	case q.hasNewMessages <- struct{}{}:
		break
	default:
		break
	}
}

type Subscriber struct {
	Messages chan Message
	close    chan struct{}
	db       *RedisDB
	channels map[string]struct{}
	patterns map[*regexp.Regexp]struct{}
	queue    messageQueue
}

func (s *Subscriber) Close() error {
	close(s.close)

	s.db.master.Lock()
	defer s.db.master.Unlock()

	for channel := range s.channels {
		subscribers := s.db.directlySubscribedChannels[channel]
		delete(subscribers, s)

		if len(subscribers) < 1 {
			delete(s.db.directlySubscribedChannels, channel)
		}
	}

	for pattern := range s.patterns {
		subscribers := s.db.directlySubscribedPatterns[pattern]
		delete(subscribers, s)

		if len(subscribers) < 1 {
			delete(s.db.directlySubscribedPatterns, pattern)
		}
	}

	return nil
}

func (s *Subscriber) Subscribe(channels ...string) {
	s.db.master.Lock()
	defer s.db.master.Unlock()

	for _, channel := range channels {
		s.channels[channel] = struct{}{}

		var peers map[*Subscriber]struct{}
		var hasPeers bool

		if peers, hasPeers = s.db.directlySubscribedChannels[channel]; !hasPeers {
			peers = map[*Subscriber]struct{}{}
			s.db.directlySubscribedChannels[channel] = peers
		}

		peers[s] = struct{}{}
	}
}

func (s *Subscriber) Unsubscribe(channels ...string) {
	s.db.master.Lock()
	defer s.db.master.Unlock()

	for _, channel := range channels {
		if _, hasChannel := s.channels[channel]; hasChannel {
			delete(s.channels, channel)

			peers := s.db.directlySubscribedChannels[channel]
			delete(peers, s)

			if len(peers) < 1 {
				delete(s.db.directlySubscribedChannels, channel)
			}
		}
	}
}

func (s *Subscriber) PSubscribe(patterns ...*regexp.Regexp) {
	s.db.master.Lock()
	defer s.db.master.Unlock()

	decompiledDSPs := s.db.master.decompiledDirectlySubscribedPatterns

	for _, pattern := range patterns {
		decompiled := pattern.String()

		if decompiledDSP, hasDDSP := decompiledDSPs[decompiled]; hasDDSP {
			pattern = decompiledDSP
		} else {
			decompiledDSPs[decompiled] = pattern
		}

		s.patterns[pattern] = struct{}{}

		var peers map[*Subscriber]struct{}
		var hasPeers bool

		if peers, hasPeers = s.db.directlySubscribedPatterns[pattern]; !hasPeers {
			peers = map[*Subscriber]struct{}{}
			s.db.directlySubscribedPatterns[pattern] = peers
		}

		peers[s] = struct{}{}
	}
}

func (s *Subscriber) PUnsubscribe(patterns ...*regexp.Regexp) {
	s.db.master.Lock()
	defer s.db.master.Unlock()

	decompiledDSPs := s.db.master.decompiledDirectlySubscribedPatterns

	for _, pattern := range patterns {
		if decompiledDSP, hasDDSP := decompiledDSPs[pattern.String()]; hasDDSP {
			pattern = decompiledDSP
		}

		if _, hasChannel := s.patterns[pattern]; hasChannel {
			delete(s.patterns, pattern)

			peers := s.db.directlySubscribedPatterns[pattern]
			delete(peers, s)

			if len(peers) < 1 {
				delete(s.db.directlySubscribedPatterns, pattern)
			}
		}
	}
}

func (s *Subscriber) streamMessages() {
	defer close(s.Messages)

	for {
		select {
		case <-s.queue.hasNewMessages:
			s.queue.Lock()

			select {
			case <-s.queue.hasNewMessages:
				break
			default:
				break
			}

			messages := s.queue.messages
			s.queue.messages = []Message{}

			s.queue.Unlock()

			for _, message := range messages {
				select {
				case s.Messages <- message:
					break
				case <-s.close:
					return
				}
			}
		case <-s.close:
			return
		}
	}
}

func (m *Miniredis) NewSubscriber() *Subscriber {
	return m.DB(m.selectedDB).NewSubscriber()
}

func (db *RedisDB) NewSubscriber() *Subscriber {
	s := &Subscriber{
		Messages: make(chan Message),
		close:    make(chan struct{}),
		db:       db,
		channels: map[string]struct{}{},
		patterns: map[*regexp.Regexp]struct{}{},
		queue: messageQueue{
			messages:       []Message{},
			hasNewMessages: make(chan struct{}, 1),
		},
	}

	go s.streamMessages()

	return s
}

func (m *Miniredis) Publish(channel, message string) int {
	return m.DB(m.selectedDB).Publish(channel, message)
}

func (db *RedisDB) Publish(channel, message string) int {
	db.master.Lock()
	defer db.master.Unlock()

	return db.publishMessage(channel, message)
}

func (m *Miniredis) PubSubChannels(pattern *regexp.Regexp) map[string]struct{} {
	return m.DB(m.selectedDB).PubSubChannels(pattern)
}

func (db *RedisDB) PubSubChannels(pattern *regexp.Regexp) map[string]struct{} {
	db.master.Lock()
	defer db.master.Unlock()

	return db.pubSubChannelsNoLock(pattern)
}

func (m *Miniredis) PubSubNumSub(channels ...string) map[string]int {
	return m.DB(m.selectedDB).PubSubNumSub(channels...)
}

func (db *RedisDB) PubSubNumSub(channels ...string) map[string]int {
	db.master.Lock()
	defer db.master.Unlock()

	return db.pubSubNumSubNoLock(channels...)
}

func (m *Miniredis) PubSubNumPat() int {
	return m.DB(m.selectedDB).PubSubNumPat()
}

func (db *RedisDB) PubSubNumPat() int {
	db.master.Lock()
	defer db.master.Unlock()

	return db.pubSubNumPatNoLock()
}

func (db *RedisDB) pubSubChannelsNoLock(pattern *regexp.Regexp) map[string]struct{} {
	channels := map[string]struct{}{}

	if pattern == nil {
		for channel := range db.subscribedChannels {
			channels[channel] = struct{}{}
		}

		for channel := range db.directlySubscribedChannels {
			channels[channel] = struct{}{}
		}
	} else {
		for channel := range db.subscribedChannels {
			if pattern.MatchString(channel) {
				channels[channel] = struct{}{}
			}
		}

		for channel := range db.directlySubscribedChannels {
			if pattern.MatchString(channel) {
				channels[channel] = struct{}{}
			}
		}
	}

	return channels
}

func (db *RedisDB) pubSubNumSubNoLock(channels ...string) map[string]int {
	numSub := map[string]int{}

	for _, channel := range channels {
		numSub[channel] = len(db.subscribedChannels[channel]) + len(db.directlySubscribedChannels[channel])
	}

	return numSub
}

func (db *RedisDB) pubSubNumPatNoLock() (numPat int) {
	for _, peers := range db.subscribedPatterns {
		numPat += len(peers)
	}

	for _, subscribers := range db.directlySubscribedPatterns {
		numPat += len(subscribers)
	}

	return
}

func (db *RedisDB) publishMessage(channel, message string) int {
	count := 0

	var allPeers map[*server.Peer]struct{} = nil

	if peers, hasPeers := db.subscribedChannels[channel]; hasPeers {
		allPeers = make(map[*server.Peer]struct{}, len(peers))

		for peer := range peers {
			allPeers[peer] = struct{}{}
		}
	}

	for pattern, peers := range db.subscribedPatterns {
		if db.master.channelPatterns[pattern].MatchString(channel) {
			if allPeers == nil {
				allPeers = make(map[*server.Peer]struct{}, len(peers))
			}

			for peer := range peers {
				allPeers[peer] = struct{}{}
			}
		}
	}

	if allPeers != nil {
		count += len(allPeers)
		go publishMessages(allPeers, channel, message)
	}

	var allSubscribers map[*Subscriber]struct{} = nil

	if subscribers, hasSubscribers := db.directlySubscribedChannels[channel]; hasSubscribers {
		allSubscribers = make(map[*Subscriber]struct{}, len(subscribers))

		for subscriber := range subscribers {
			allSubscribers[subscriber] = struct{}{}
		}
	}

	for pattern, subscribers := range db.directlySubscribedPatterns {
		if pattern.MatchString(channel) {
			if allSubscribers == nil {
				allSubscribers = make(map[*Subscriber]struct{}, len(subscribers))
			}

			for subscriber := range subscribers {
				allSubscribers[subscriber] = struct{}{}
			}
		}
	}

	if allSubscribers != nil {
		count += len(allSubscribers)
		go publishMessagesToOurselves(allSubscribers, channel, message)
	}

	return count
}

func publishMessages(peers map[*server.Peer]struct{}, channel, message string) {
	for peer := range peers {
		go publishMessage(peer, channel, message)
	}
}

func publishMessage(peer *server.Peer, channel, message string) {
	peer.MsgQueue.Enqueue(&queuedPubSubMessage{channel, message})
}

func publishMessagesToOurselves(subscribers map[*Subscriber]struct{}, channel, message string) {
	for subscriber := range subscribers {
		go publishMessageToOurselves(subscriber, channel, message)
	}
}

func publishMessageToOurselves(subscriber *Subscriber, channel, message string) {
	subscriber.queue.Enqueue(Message{channel, message})
}
