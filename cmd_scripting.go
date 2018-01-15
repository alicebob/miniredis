package miniredis

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/garyburd/redigo/redis"
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

	// create a redis client for redis.call
	conn, err := redis.Dial("tcp", m.srv.Addr().String())
	if err != nil {
		c.WriteError(fmt.Sprintf("ERR Redis error: %v", err.Error()))
		return
	}
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

	// Register call function to lua VM
	redisFuncs := map[string]lua.LGFunction{
		"call": func(l *lua.LState) int {
			top := l.GetTop()

			cmd := lua.LVAsString(l.Get(1))
			args := make([]interface{}, top-1)
			for i := 2; i <= top; i++ {
				arg := l.Get(i)

				dataType := arg.Type()
				switch dataType {
				case lua.LTBool:
					args[i-2] = lua.LVAsBool(arg)
				case lua.LTNumber:
					value, _ := strconv.ParseFloat(lua.LVAsString(arg), 64)
					args[i-2] = value
				case lua.LTString:
					args[i-2] = lua.LVAsString(arg)
				case lua.LTNil:
				case lua.LTFunction:
				case lua.LTUserData:
				case lua.LTThread:
				case lua.LTTable:
				case lua.LTChannel:
				default:
					args[i-2] = nil
				}
			}
			res, err := conn.Do(cmd, args...)
			if err != nil {
				l.Push(lua.LNil)
				return 1
			}

			if res == nil {
				l.Push(lua.LNil)
			} else {
				switch r := res.(type) {
				case int64:
					l.Push(lua.LNumber(r))
				case []uint8:
					l.Push(lua.LString(string(r)))
				case []interface{}:
					l.Push(m.redisToLua(l, r))
				case string:
					l.Push(lua.LString(r))
				default:
					// TODO: oops?
					l.Push(lua.LString(res.(string)))
				}
			}

			return 1 // Notify that we pushed one value to the stack
		},
	}

	redisFuncs["pcall"] = redisFuncs["call"]

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
		m.luaToRedis(l, c, l.Get(1))
	} else {
		c.WriteNull()
	}
}

func (m *Miniredis) redisToLua(l *lua.LState, res []interface{}) *lua.LTable {
	rettb := l.NewTable()
	for _, e := range res {
		var v lua.LValue
		if e == nil {
			v = lua.LValue(nil)
		} else {
			switch et := e.(type) {
			case int64:
				v = lua.LNumber(et)
			case []uint8:
				v = lua.LString(string(et))
			case []interface{}:
				v = m.redisToLua(l, et)
			case string:
				v = lua.LString(et)
			default:
				// TODO: oops?
				v = lua.LString(e.(string))
			}
		}
		l.RawSet(rettb, lua.LNumber(rettb.Len()+1), v)
	}
	return rettb
}

func (m *Miniredis) luaToRedis(l *lua.LState, c *server.Peer, value lua.LValue) {
	if value == nil {
		c.WriteNull()
		return
	}

	switch value.Type() {
	case lua.LTNil:
		c.WriteNull()
	case lua.LTBool:
		if lua.LVAsBool(value) {
			c.WriteInt(1)
		} else {
			c.WriteInt(0)
		}
	case lua.LTNumber:
		c.WriteInt(int(lua.LVAsNumber(value)))
	case lua.LTString:
		c.WriteBulk(lua.LVAsString(value))
	case lua.LTTable:
		result := []lua.LValue{}
		for j := 1; true; j++ {
			val := l.GetTable(value, lua.LNumber(j))
			if val == nil {
				result = append(result, val)
				continue
			}

			if val.Type() == lua.LTNil {
				break
			}

			result = append(result, val)
		}

		c.WriteLen(len(result))
		for _, r := range result {
			m.luaToRedis(l, c, r)
		}
	default:
		c.WriteInline(lua.LVAsString(value))
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
