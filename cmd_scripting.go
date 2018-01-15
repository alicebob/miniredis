package miniredis

import (
	"crypto/sha1"
	"encoding/hex"
	"io"
	"strconv"
	"strings"

	lua "github.com/yuin/gopher-lua"
	"github.com/yuin/gopher-lua/parse"

	"github.com/alicebob/miniredis/server"
)

func commandsScripting(m *Miniredis) {
	m.srv.Register("EVAL", m.cmdEval)
	m.srv.Register("EVALSHA", m.cmdEvalsha)
	m.srv.Register("SCRIPT", m.cmdScript)
}

func (m *Miniredis) runLuaScript(c *server.Peer, script string, args []string) {
	l := lua.NewState()
	defer l.Close()

	conn := m.redigo()
	defer conn.Close()

	// set global variable KEYS
	keysTable := l.NewTable()
	keysS, args := args[0], args[1:]
	keysLen, err := strconv.Atoi(keysS)
	if err != nil {
		c.WriteError(msgInvalidInt)
		return
	}
	if keysLen < 0 {
		c.WriteError(msgNegativeKeysNumber)
		return
	}
	if keysLen > len(args) {
		c.WriteError(msgInvalidKeysNumber)
		return
	}
	keys, args := args[:keysLen], args[keysLen:]
	for i, k := range keys {
		l.RawSet(keysTable, lua.LNumber(i+1), lua.LString(k))
	}
	l.SetGlobal("KEYS", keysTable)

	argvTable := l.NewTable()
	for i, a := range args {
		l.RawSet(argvTable, lua.LNumber(i+1), lua.LString(a))
	}
	l.SetGlobal("ARGV", argvTable)

	redisFuncs := mkLuaFuncs(conn)
	// Register command handlers
	l.Push(l.NewFunction(func(l *lua.LState) int {
		mod := l.RegisterModule("redis", redisFuncs).(*lua.LTable)
		l.Push(mod)
		return 1
	}))

	l.Push(lua.LString("redis"))
	l.Call(1, 0)

	if err := l.DoString(script); err != nil {
		c.WriteError(errLuaParseError(err))
		return
	}

	if l.GetTop() > 0 {
		luaToRedis(l, c, l.Get(1))
	} else {
		c.WriteNull()
	}
}

func (m *Miniredis) cmdEval(c *server.Peer, cmd string, args []string) {
	if len(args) < 2 {
		setDirty(c)
		c.WriteError(errWrongNumber(cmd))
		return
	}
	if !m.handleAuth(c) {
		return
	}
	script, args := args[0], args[1:]
	m.runLuaScript(c, script, args)
}

func (m *Miniredis) cmdEvalsha(c *server.Peer, cmd string, args []string) {
	if len(args) < 2 {
		setDirty(c)
		c.WriteError(errWrongNumber(cmd))
		return
	}
	if !m.handleAuth(c) {
		return
	}

	sha, args := args[0], args[1:]
	m.Lock()
	script, ok := m.scripts[sha]
	m.Unlock()
	if !ok {
		c.WriteError(msgNoScriptFound)
		return
	}
	m.runLuaScript(c, script, args)
}

func (m *Miniredis) cmdScript(c *server.Peer, cmd string, args []string) {
	if len(args) < 1 {
		setDirty(c)
		c.WriteError(errWrongNumber(cmd))
		return
	}
	if !m.handleAuth(c) {
		return
	}

	subcmd, args := args[0], args[1:]
	switch strings.ToLower(subcmd) {
	case "load":
		if len(args) != 1 {
			setDirty(c)
			c.WriteError(msgScriptUsage)
			return
		}
		script := args[0]
		if _, err := parse.Parse(strings.NewReader(script), "user_script"); err != nil {
			c.WriteError(errLuaParseError(err))
			return
		}
		sha := scriptSha(script)
		m.Lock()
		m.scripts[sha] = script
		m.Unlock()
		c.WriteBulk(sha)

	case "exists":
		m.Lock()
		defer m.Unlock()
		c.WriteLen(len(args))
		for _, arg := range args {
			if _, ok := m.scripts[arg]; ok {
				c.WriteInt(1)
			} else {
				c.WriteInt(0)
			}
		}

	case "flush":
		if len(args) != 0 {
			setDirty(c)
			c.WriteError(msgScriptUsage)
			return
		}

		m.Lock()
		defer m.Unlock()
		m.scripts = map[string]string{}
		c.WriteOK()

	default:
		c.WriteError(msgScriptUsage)
	}
}

func scriptSha(s string) string {
	h := sha1.New()
	io.WriteString(h, s)
	return hex.EncodeToString(h.Sum(nil))
}
