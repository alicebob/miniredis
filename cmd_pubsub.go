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
	m.srv.Register("PUBLISH", m.cmdPublish)
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

	subscriptionsAmounts := make([]int, len(args))

	withTx(m, c, func(c *server.Peer, ctx *connCtx) {
		var cache peerCache
		var hasCache bool

		if cache, hasCache = m.peers[c]; !hasCache {
			cache = peerCache{subscriptions: map[int]peerSubscriptions{}}
			m.peers[c] = cache
		}

		var dbSubs peerSubscriptions
		var hasDbSubs bool

		if dbSubs, hasDbSubs = cache.subscriptions[ctx.selectedDB]; !hasDbSubs {
			dbSubs = peerSubscriptions{channels: map[string]struct{}{}, patterns: map[string]struct{}{}}
			cache.subscriptions[ctx.selectedDB] = dbSubs
		}

		subscribedChannels := m.db(ctx.selectedDB).subscribedChannels

		for i, channel := range args {
			var peers map[*server.Peer]struct{}
			var hasPeers bool

			if peers, hasPeers = subscribedChannels[channel]; !hasPeers {
				peers = map[*server.Peer]struct{}{}
				subscribedChannels[channel] = peers
			}

			peers[c] = struct{}{}

			dbSubs.channels[channel] = struct{}{}

			subscriptionsAmounts[i] = m.getSubscriptionsAmount(c, ctx)
		}

		for i, channel := range args {
			c.WriteLen(3)
			c.WriteBulk("subscribe")
			c.WriteBulk(channel)
			c.WriteInt(subscriptionsAmounts[i])
		}
	})
}

func (m *Miniredis) getSubscriptionsAmount(c *server.Peer, ctx *connCtx) (total int) {
	if cache, hasCache := m.peers[c]; hasCache {
		if dbSubs, hasDbSubs := cache.subscriptions[ctx.selectedDB]; hasDbSubs {
			total = len(dbSubs.channels) + len(dbSubs.patterns)
		}
	}

	return
}

// UNSUBSCRIBE
func (m *Miniredis) cmdUnsubscribe(c *server.Peer, cmd string, args []string) {
	if !m.handleAuth(c) {
		return
	}

	var channels []string = nil
	var subscriptionsAmounts []int = nil

	withTx(m, c, func(c *server.Peer, ctx *connCtx) {
		if cache, hasCache := m.peers[c]; hasCache {
			if dbSubs, hasDbSubs := cache.subscriptions[ctx.selectedDB]; hasDbSubs {
				subscribedChannels := m.db(ctx.selectedDB).subscribedChannels

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

				subscriptionsAmounts = make([]int, len(channels))

				for i, channel := range channels {
					if peers, hasPeers := subscribedChannels[channel]; hasPeers {
						delete(peers, c)
						delete(dbSubs.channels, channel)

						if len(peers) < 1 {
							delete(subscribedChannels, channel)
						}

						if len(dbSubs.channels) < 1 && len(dbSubs.patterns) < 1 {
							delete(cache.subscriptions, ctx.selectedDB)

							if len(cache.subscriptions) < 1 {
								delete(m.peers, c)
							}
						}
					}

					subscriptionsAmounts[i] = m.getSubscriptionsAmount(c, ctx)
				}
			}
		}

		var subscriptionsAmount int

		if channels == nil {
			subscriptionsAmount = m.getSubscriptionsAmount(c, ctx)
		}

		if channels == nil {
			for _, channel := range args {
				c.WriteLen(3)
				c.WriteBulk("unsubscribe")
				c.WriteBulk(channel)
				c.WriteInt(subscriptionsAmount)
			}
		} else {
			for i, channel := range channels {
				c.WriteLen(3)
				c.WriteBulk("unsubscribe")
				c.WriteBulk(channel)
				c.WriteInt(subscriptionsAmounts[i])
			}
		}
	})
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

	subscriptionsAmounts := make([]int, len(args))

	withTx(m, c, func(c *server.Peer, ctx *connCtx) {
		var cache peerCache
		var hasCache bool

		if cache, hasCache = m.peers[c]; !hasCache {
			cache = peerCache{subscriptions: map[int]peerSubscriptions{}}
			m.peers[c] = cache
		}

		var dbSubs peerSubscriptions
		var hasDbSubs bool

		if dbSubs, hasDbSubs = cache.subscriptions[ctx.selectedDB]; !hasDbSubs {
			dbSubs = peerSubscriptions{channels: map[string]struct{}{}, patterns: map[string]struct{}{}}
			cache.subscriptions[ctx.selectedDB] = dbSubs
		}

		subscribedPatterns := m.db(ctx.selectedDB).subscribedPatterns

		for i, pattern := range args {
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

			subscriptionsAmounts[i] = m.getSubscriptionsAmount(c, ctx)
		}

		for i, pattern := range args {
			c.WriteLen(3)
			c.WriteBulk("psubscribe")
			c.WriteBulk(pattern)
			c.WriteInt(subscriptionsAmounts[i])
		}
	})
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

	var patterns []string = nil
	var subscriptionsAmounts []int = nil

	withTx(m, c, func(c *server.Peer, ctx *connCtx) {
		if cache, hasCache := m.peers[c]; hasCache {
			if dbSubs, hasDbSubs := cache.subscriptions[ctx.selectedDB]; hasDbSubs {
				subscribedPatterns := m.db(ctx.selectedDB).subscribedPatterns

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

				subscriptionsAmounts = make([]int, len(patterns))

				for i, pattern := range patterns {
					if peers, hasPeers := subscribedPatterns[pattern]; hasPeers {
						delete(peers, c)
						delete(dbSubs.patterns, pattern)

						if len(peers) < 1 {
							delete(subscribedPatterns, pattern)
						}

						if len(dbSubs.patterns) < 1 && len(dbSubs.channels) < 1 {
							delete(cache.subscriptions, ctx.selectedDB)

							if len(cache.subscriptions) < 1 {
								delete(m.peers, c)
							}
						}
					}

					subscriptionsAmounts[i] = m.getSubscriptionsAmount(c, ctx)
				}
			}
		}

		var subscriptionsAmount int

		if patterns == nil {
			subscriptionsAmount = m.getSubscriptionsAmount(c, ctx)
		}

		if patterns == nil {
			for _, pattern := range args {
				c.WriteLen(3)
				c.WriteBulk("punsubscribe")
				c.WriteBulk(pattern)
				c.WriteInt(subscriptionsAmount)
			}
		} else {
			for i, pattern := range patterns {
				c.WriteLen(3)
				c.WriteBulk("punsubscribe")
				c.WriteBulk(pattern)
				c.WriteInt(subscriptionsAmounts[i])
			}
		}
	})
}

// PUBLISH
func (m *Miniredis) cmdPublish(c *server.Peer, cmd string, args []string) {
	if len(args) != 2 {
		setDirty(c)
		c.WriteError(errWrongNumber(cmd))
		return
	}
	if !m.handleAuth(c) {
		return
	}

	channel := args[0]
	message := args[1]

	withTx(m, c, func(c *server.Peer, ctx *connCtx) {
		db := m.db(ctx.selectedDB)
		var allPeers map[*server.Peer]struct{} = nil

		if peers, hasPeers := db.subscribedChannels[channel]; hasPeers {
			allPeers = make(map[*server.Peer]struct{}, len(peers))

			for peer := range peers {
				allPeers[peer] = struct{}{}
			}
		}

		for pattern, peers := range db.subscribedPatterns {
			if m.channelPatterns[pattern].MatchString(channel) {
				if allPeers == nil {
					allPeers = make(map[*server.Peer]struct{}, len(peers))
				}

				for peer := range peers {
					allPeers[peer] = struct{}{}
				}
			}
		}

		if allPeers == nil {
			c.WriteInt(0)
		} else {
			c.WriteInt(len(allPeers))

			for peer := range allPeers {
				peer.MsgQueue.Enqueue(channel, message)
			}
		}
	})
}
