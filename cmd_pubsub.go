// Commands from https://redis.io/commands#pubsub

package miniredis

import (
	"github.com/alicebob/miniredis/server"
	"regexp"
)

// commandsPubsub handles all PUB/SUB operations.
func commandsPubsub(m *Miniredis) {
	m.srv.Register("SUBSCRIBE", m.cmdSubscribe)
	m.srv.Register("UNSUBSCRIBE", m.cmdUnsubscribe)
	m.srv.Register("PSUBSCRIBE", m.cmdPSubscribe)
	m.srv.Register("PUNSUBSCRIBE", m.cmdPUnsubscribe)
}

// SUBSCRIBE
func (m *Miniredis) cmdSubscribe(c *server.Peer, cmd string, args []string) {
	if len(args) < 1 {
		setDirty(c)
		c.WriteError(errWrongNumber(cmd))
		return
	}
	if !m.handleAuth(c) {
		return
	}

	m.Lock()

	var cache peerCache
	var hasCache bool

	if cache, hasCache = m.peers[c]; !hasCache {
		cache = peerCache{subscriptions: map[int]peerSubscriptions{}}
		m.peers[c] = cache
	}

	dbIdx := getCtx(c).selectedDB

	var dbSubs peerSubscriptions
	var hasDbSubs bool

	if dbSubs, hasDbSubs = cache.subscriptions[dbIdx]; !hasDbSubs {
		dbSubs = peerSubscriptions{channels: map[string]struct{}{}, patterns: map[string]struct{}{}}
		cache.subscriptions[dbIdx] = dbSubs
	}

	subscribedChannels := m.db(dbIdx).subscribedChannels

	for _, channel := range args {
		var peers map[*server.Peer]struct{}
		var hasPeers bool

		if peers, hasPeers = subscribedChannels[channel]; !hasPeers {
			peers = map[*server.Peer]struct{}{}
			subscribedChannels[channel] = peers
		}

		peers[c] = struct{}{}

		dbSubs.channels[channel] = struct{}{}
	}

	m.Unlock()

	c.WriteOK()
}

// UNSUBSCRIBE
func (m *Miniredis) cmdUnsubscribe(c *server.Peer, cmd string, args []string) {
	if !m.handleAuth(c) {
		return
	}

	m.Lock()

	if cache, hasCache := m.peers[c]; hasCache {
		dbIdx := getCtx(c).selectedDB

		if dbSubs, hasDbSubs := cache.subscriptions[dbIdx]; hasDbSubs {
			subscribedChannels := m.db(dbIdx).subscribedChannels

			var channels []string

			if len(args) > 0 {
				channels = args
			} else {
				channels = make([]string, len(dbSubs.channels))
				i := 0

				for channel := range dbSubs.channels {
					channels[i] = channel
					i++
				}
			}

			for _, channel := range channels {
				if peers, hasPeers := subscribedChannels[channel]; hasPeers {
					delete(peers, c)
					delete(dbSubs.channels, channel)

					if len(peers) < 1 {
						delete(subscribedChannels, channel)
					}

					if len(dbSubs.channels) < 1 && len(dbSubs.patterns) < 1 {
						delete(cache.subscriptions, dbIdx)

						if len(cache.subscriptions) < 1 {
							delete(m.peers, c)
						}
					}
				}
			}
		}
	}

	m.Unlock()

	c.WriteOK()
}

// PSUBSCRIBE
func (m *Miniredis) cmdPSubscribe(c *server.Peer, cmd string, args []string) {
	if len(args) < 1 {
		setDirty(c)
		c.WriteError(errWrongNumber(cmd))
		return
	}
	if !m.handleAuth(c) {
		return
	}

	m.Lock()

	var cache peerCache
	var hasCache bool

	if cache, hasCache = m.peers[c]; !hasCache {
		cache = peerCache{subscriptions: map[int]peerSubscriptions{}}
		m.peers[c] = cache
	}

	dbIdx := getCtx(c).selectedDB

	var dbSubs peerSubscriptions
	var hasDbSubs bool

	if dbSubs, hasDbSubs = cache.subscriptions[dbIdx]; !hasDbSubs {
		dbSubs = peerSubscriptions{channels: map[string]struct{}{}, patterns: map[string]struct{}{}}
		cache.subscriptions[dbIdx] = dbSubs
	}

	subscribedPatterns := m.db(dbIdx).subscribedPatterns

	for _, pattern := range args {
		var peers map[*server.Peer]struct{}
		var hasPeers bool

		if peers, hasPeers = subscribedPatterns[pattern]; !hasPeers {
			peers = map[*server.Peer]struct{}{}
			subscribedPatterns[pattern] = peers
		}

		peers[c] = struct{}{}

		dbSubs.patterns[pattern] = struct{}{}

		if _, hasRgx := m.channelPatterns[pattern]; !hasRgx {
			m.channelPatterns[pattern] = compileChannelPattern(pattern)
		}
	}

	m.Unlock()

	c.WriteOK()
}

func compileChannelPattern(pattern string) *regexp.Regexp {
	const readingLiteral uint8 = 0
	const afterEscape uint8 = 1
	const inClass uint8 = 2

	rgx := []rune{'\\', 'A'}
	state := readingLiteral
	literals := []rune{}
	klass := map[rune]struct{}{}

	for _, c := range pattern {
		switch state {
		case readingLiteral:
			switch c {
			case '\\':
				state = afterEscape
			case '?':
				rgx = append(rgx, append([]rune(regexp.QuoteMeta(string(literals))), '.')...)
				literals = []rune{}
			case '*':
				rgx = append(rgx, append([]rune(regexp.QuoteMeta(string(literals))), '.', '*')...)
				literals = []rune{}
			case '[':
				rgx = append(rgx, []rune(regexp.QuoteMeta(string(literals)))...)
				literals = []rune{}
				state = inClass
			default:
				literals = append(literals, c)
			}
		case afterEscape:
			literals = append(literals, c)
			state = readingLiteral
		case inClass:
			if c == ']' {
				expr := []rune{'['}

				if _, hasDash := klass['-']; hasDash {
					delete(klass, '-')
					expr = append(expr, '-')
				}

				flatClass := make([]rune, len(klass))
				i := 0

				for c := range klass {
					flatClass[i] = c
					i++
				}

				klass = map[rune]struct{}{}
				rgx = append(rgx, append(append(expr, []rune(regexp.QuoteMeta(string(flatClass)))...), ']')...)
				state = readingLiteral
			} else {
				klass[c] = struct{}{}
			}
		}
	}

	switch state {
	case afterEscape:
		rgx = append(rgx, '\\', '\\')
	case inClass:
		if len(klass) < 0 {
			rgx = append(rgx, '\\', '[')
		} else {
			expr := []rune{'['}

			if _, hasDash := klass['-']; hasDash {
				delete(klass, '-')
				expr = append(expr, '-')
			}

			flatClass := make([]rune, len(klass))
			i := 0

			for c := range klass {
				flatClass[i] = c
				i++
			}

			rgx = append(rgx, append(append(expr, []rune(regexp.QuoteMeta(string(flatClass)))...), ']')...)
		}
	}

	return regexp.MustCompile(string(append(rgx, '\\', 'z')))
}

// PUNSUBSCRIBE
func (m *Miniredis) cmdPUnsubscribe(c *server.Peer, cmd string, args []string) {
	if !m.handleAuth(c) {
		return
	}

	m.Lock()

	if cache, hasCache := m.peers[c]; hasCache {
		dbIdx := getCtx(c).selectedDB

		if dbSubs, hasDbSubs := cache.subscriptions[dbIdx]; hasDbSubs {
			subscribedPatterns := m.db(dbIdx).subscribedPatterns

			var patterns []string

			if len(args) > 0 {
				patterns = args
			} else {
				patterns = make([]string, len(dbSubs.patterns))
				i := 0

				for pattern := range dbSubs.patterns {
					patterns[i] = pattern
					i++
				}
			}

			for _, pattern := range patterns {
				if peers, hasPeers := subscribedPatterns[pattern]; hasPeers {
					delete(peers, c)
					delete(dbSubs.patterns, pattern)

					if len(peers) < 1 {
						delete(subscribedPatterns, pattern)
					}

					if len(dbSubs.patterns) < 1 && len(dbSubs.channels) < 1 {
						delete(cache.subscriptions, dbIdx)

						if len(cache.subscriptions) < 1 {
							delete(m.peers, c)
						}
					}
				}
			}
		}
	}

	m.Unlock()

	c.WriteOK()
}
