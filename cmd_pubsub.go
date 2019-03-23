// Commands from https://redis.io/commands#pubsub

package miniredis

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/alicebob/miniredis/server"
)

// commandsPubsub handles all PUB/SUB operations.
func commandsPubsub(m *Miniredis) {
	m.srv.Register("SUBSCRIBE", m.cmdSubscribe)
	m.srv.Register("UNSUBSCRIBE", m.cmdUnsubscribe)
	m.srv.Register("PSUBSCRIBE", m.cmdPsubscribe)
	m.srv.Register("PUNSUBSCRIBE", m.cmdPunsubscribe)
	m.srv.Register("PUBLISH", m.cmdPublish)
	m.srv.Register("PUBSUB", m.cmdPubSub)
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

	withTx(m, c, func(c *server.Peer, ctx *connCtx) {
		sub := m.subscribedState(c)
		for _, channel := range args {
			n := sub.Subscribe(channel)
			c.Block(func(c *server.Writer) {
				c.WriteLen(3)
				c.WriteBulk("subscribe")
				c.WriteBulk(channel)
				c.WriteInt(n)
			})
		}
	})
}

// UNSUBSCRIBE
func (m *Miniredis) cmdUnsubscribe(c *server.Peer, cmd string, args []string) {
	if !m.handleAuth(c) {
		return
	}

	channels := args

	withTx(m, c, func(c *server.Peer, ctx *connCtx) {
		sub := m.subscribedState(c)

		if len(channels) == 0 {
			channels = sub.Channels()
		}

		// there is no de-duplication
		for _, channel := range channels {
			n := sub.Unsubscribe(channel)
			c.Block(func(c *server.Writer) {
				c.WriteLen(3)
				c.WriteBulk("unsubscribe")
				c.WriteBulk(channel)
				c.WriteInt(n)
			})
		}

		if sub.Count() == 0 {
			endSubscriber(m, c)
		}
	})
}

// PSUBSCRIBE
func (m *Miniredis) cmdPsubscribe(c *server.Peer, cmd string, args []string) {
	if len(args) < 1 {
		setDirty(c)
		c.WriteError(errWrongNumber(cmd))
		return
	}
	if !m.handleAuth(c) {
		return
	}

	withTx(m, c, func(c *server.Peer, ctx *connCtx) {
		sub := m.subscribedState(c)
		for _, pat := range args {
			n := sub.Psubscribe(pat)
			c.Block(func(c *server.Writer) {
				c.WriteLen(3)
				c.WriteBulk("psubscribe")
				c.WriteBulk(pat)
				c.WriteInt(n)
			})
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
				expr = append(append(expr, []rune(regexp.QuoteMeta(string(flatClass)))...), ']')

				if len(expr) < 3 {
					rgx = append(rgx, 'x', '\\', 'b', 'y')
				} else {
					rgx = append(rgx, expr...)
				}

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

			expr = append(append(expr, []rune(regexp.QuoteMeta(string(flatClass)))...), ']')

			if len(expr) < 3 {
				rgx = append(rgx, 'x', '\\', 'b', 'y')
			} else {
				rgx = append(rgx, expr...)
			}
		}
	}

	return regexp.MustCompile(string(append(rgx, '\\', 'z')))
}

// PUNSUBSCRIBE
func (m *Miniredis) cmdPunsubscribe(c *server.Peer, cmd string, args []string) {
	if !m.handleAuth(c) {
		return
	}

	patterns := args

	withTx(m, c, func(c *server.Peer, ctx *connCtx) {
		sub := m.subscribedState(c)

		if len(patterns) == 0 {
			patterns = sub.Patterns()
		}

		// there is no de-duplication
		for _, pat := range patterns {
			n := sub.Punsubscribe(pat)
			c.Block(func(c *server.Writer) {
				c.WriteLen(3)
				c.WriteBulk("punsubscribe")
				c.WriteBulk(pat)
				c.WriteInt(n)
			})
		}

		if sub.Count() == 0 {
			endSubscriber(m, c)
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
	if m.checkPubsub(c) {
		return
	}

	channel, mesg := args[0], args[1]

	withTx(m, c, func(c *server.Peer, ctx *connCtx) {
		c.WriteInt(m.publish(channel, mesg))
	})
}

// PUBSUB
func (m *Miniredis) cmdPubSub(c *server.Peer, cmd string, args []string) {
	if len(args) < 1 {
		setDirty(c)
		c.WriteError(errWrongNumber(cmd))
		return
	}

	if m.checkPubsub(c) {
		return
	}

	subcommand := strings.ToUpper(args[0])
	subargs := args[1:]
	var argsOk bool

	switch subcommand {
	case "CHANNELS":
		argsOk = len(subargs) < 2
	case "NUMSUB":
		argsOk = true
	case "NUMPAT":
		argsOk = len(subargs) == 0
	default:
		argsOk = false
	}

	if !argsOk {
		setDirty(c)
		c.WriteError(fmt.Sprintf(msgFPubsubUsage, subcommand))
		return
	}

	if !m.handleAuth(c) {
		return
	}

	withTx(m, c, func(c *server.Peer, ctx *connCtx) {
		switch subcommand {
		case "CHANNELS":
			pat := ""
			if len(subargs) == 1 {
				pat = subargs[0]
			}

			channels := activeChannels(m.allSubscribers(), pat)

			c.WriteLen(len(channels))
			for _, channel := range channels {
				c.WriteBulk(channel)
			}

		case "NUMSUB":
			subs := m.allSubscribers()
			c.WriteLen(len(subargs) * 2)
			for _, channel := range subargs {
				c.WriteBulk(channel)
				c.WriteInt(countSubs(subs, channel))
			}
		case "NUMPAT":
			c.WriteInt(countPsubs(m.allSubscribers()))
		}
	})
}
