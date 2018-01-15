package miniredis

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"strings"

	"github.com/garyburd/redigo/redis"
	lua "github.com/yuin/gopher-lua"

	"github.com/alicebob/miniredis/server"
)

func commandsScripting(m *Miniredis) {
	m.srv.Register("EVAL", m.cmdEval)
	m.srv.Register("EVALSHA", m.cmdEvalsha)
	m.srv.Register("SCRIPT", m.cmdScript)
}

func byteToString(bs []uint8) string {
	b := make([]byte, len(bs))
	for i, v := range bs {
		b[i] = byte(v)
	}
	return string(b)
}

func (m *Miniredis) runLuaScript(c *server.Peer, script string, args []string) error {
	L := lua.NewState()
	defer L.Close()

	// create a redis client for redis.call
	conn, err := redis.Dial("tcp", m.srv.Addr().String())
	if err != nil {
		return err
	}
	defer conn.Close()

	// set global variable KEYS
	keysTable := L.NewTable()
	keysLen, err := strconv.Atoi(args[1])
	if err != nil {
		c.WriteError(err.Error())
		return err
	}
	for i := 0; i < keysLen; i++ {
		L.RawSet(keysTable, lua.LNumber(i+1), lua.LString(args[i+2]))
	}
	L.SetGlobal("KEYS", keysTable)

	// set global variable ARGV
	argvTable := L.NewTable()
	argvLen := len(args) - 2 - keysLen
	for i := 0; i < argvLen; i++ {
		L.RawSet(argvTable, lua.LNumber(i+1), lua.LString(args[i+2+keysLen]))
	}
	L.SetGlobal("ARGV", argvTable)

	// Register call function to lua VM
	redisFuncs := map[string]lua.LGFunction{
		"call": func(L *lua.LState) int {
			top := L.GetTop()

			cmd := lua.LVAsString(L.Get(1))
			args := make([]interface{}, top-1)
			for i := 2; i <= top; i++ {
				arg := L.Get(i)

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
				L.Push(lua.LNil)
				return 1
			}

			pushCount := 0
			resType := reflect.TypeOf(res)

			if resType == nil {
				L.Push(lua.LNil)
				pushCount++
			} else {
				if resType.String() == "int64" {
					L.Push(lua.LNumber(res.(int64)))
					pushCount++
				} else if resType.String() == "[]uint8" {
					L.Push(lua.LString(byteToString(res.([]uint8))))
					pushCount++
				} else if resType.String() == "[]interface {}" {
					L.Push(m.redisToLua(L, res))
					pushCount++
				} else {
					L.Push(lua.LString(res.(string)))
					pushCount++
				}
			}

			return pushCount // Notify that we pushed one value to the stack
		},
	}

	redisFuncs["pcall"] = redisFuncs["call"]

	// Register command handlers
	L.Push(L.NewFunction(func(L *lua.LState) int {
		mod := L.RegisterModule("redis", redisFuncs).(*lua.LTable)
		L.Push(mod)
		return 1
	}))

	L.Push(lua.LString("redis"))
	L.Call(1, 0)

	if err := L.DoString(script); err != nil {
		c.WriteError(err.Error())
		return err
	}

	if L.GetTop() > 0 {
		m.luaToRedis(L, c, L.Get(1))
	} else {
		c.WriteNull()
	}

	return nil
}

func (m *Miniredis) redisToLua(L *lua.LState, res interface{}) *lua.LTable {
	rettb := L.NewTable()
	for _, e := range res.([]interface{}) {
		if e == nil {
			L.RawSet(rettb, lua.LNumber(rettb.Len()+1), lua.LValue(nil))
			continue
		}

		if reflect.TypeOf(e).String() == "int64" {
			L.RawSet(rettb, lua.LNumber(rettb.Len()+1), lua.LNumber(e.(int64)))
		} else if reflect.TypeOf(e).String() == "[]uint8" {
			L.RawSet(rettb, lua.LNumber(rettb.Len()+1), lua.LString(byteToString(e.([]uint8))))
		} else if reflect.TypeOf(e).String() == "[]interface {}" {
			L.RawSet(rettb, lua.LNumber(rettb.Len()+1), m.redisToLua(L, e))
		} else {
			L.RawSet(rettb, lua.LNumber(rettb.Len()+1), lua.LString(e.(string)))
		}
	}

	return rettb
}

func (m *Miniredis) luaToRedis(L *lua.LState, c *server.Peer, value lua.LValue) {
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
		c.WriteInline(lua.LVAsString(value))
	case lua.LTTable:
		result := []lua.LValue{}
		for j := 1; true; j++ {
			val := L.GetTable(value, lua.LNumber(j))
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
			m.luaToRedis(L, c, r)
		}
	default:
		c.WriteInline(lua.LVAsString(value))
	}
}

func (m *Miniredis) cmdEval(c *server.Peer, cmd string, args []string) {
	if len(args) < 1 {
		setDirty(c)
		c.WriteError(errWrongNumber(cmd))
		return
	}
	if !m.handleAuth(c) {
		return
	}

	err := m.runLuaScript(c, args[0], args)
	if err != nil {
		c.WriteError(err.Error())
	}
}

func (m *Miniredis) cmdEvalsha(c *server.Peer, cmd string, args []string) {
	if len(args) < 1 {
		setDirty(c)
		c.WriteError(errWrongNumber(cmd))
		return
	}
	if !m.handleAuth(c) {
		return
	}

	sha := args[0]
	m.Lock()
	script, ok := m.scripts[sha]
	m.Unlock()
	if !ok {
		c.WriteError(fmt.Sprintf("Invalid SHA %v", sha))
	}
	err := m.runLuaScript(c, script, args)
	if err != nil {
		c.WriteError(err.Error())
	}
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

	m.Lock()
	defer m.Unlock()

	switch strings.Trim(strings.ToLower(args[0]), " \t") {
	case "load":
		if len(args) < 2 {
			setDirty(c)
			c.WriteError(errWrongNumber(cmd))
			return
		}

		var shaList []string
		for _, arg := range args[1:] {
			sha := scriptSha(arg)
			m.scripts[sha] = arg
			shaList = append(shaList, sha)
		}

		c.WriteLen(len(shaList))
		for _, sha := range shaList {
			c.WriteBulk(sha)
		}
	case "exists":
		if len(args) < 2 {
			setDirty(c)
			c.WriteError(errWrongNumber(cmd))
			return
		}

		c.WriteLen(len(args) - 1)
		for _, arg := range args[1:] {
			if _, ok := m.scripts[arg]; ok {
				c.WriteInt(1)
			} else {
				c.WriteInt(0)
			}
		}
	case "flush":
		m.scripts = map[string]string{}
		c.WriteOK()
	default:
		c.WriteError("Not implemented yet")
	}
}

func scriptSha(s string) string {
	h := sha1.New()
	io.WriteString(h, s)
	return hex.EncodeToString(h.Sum(nil))
}
