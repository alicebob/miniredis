package miniredis

import (
	"strconv"

	redigo "github.com/garyburd/redigo/redis"
	lua "github.com/yuin/gopher-lua"

	"github.com/alicebob/miniredis/server"
)

func mkLuaFuncs(conn redigo.Conn) map[string]lua.LGFunction {
	funcs := map[string]lua.LGFunction{
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
					l.Push(redisToLua(l, r))
				case string:
					l.Push(lua.LString(r))
				default:
					// TODO: oops?
					l.Push(lua.LString(res.(string)))
				}
			}

			return 1 // Notify that we pushed one value to the stack
		},
		"error_reply": func(l *lua.LState) int {
			msg := l.CheckString(1)
			res := &lua.LTable{}
			res.RawSetString("err", lua.LString(msg))
			l.Push(res)
			return 1
		},
		"status_reply": func(l *lua.LState) int {
			msg := l.CheckString(1)
			res := &lua.LTable{}
			res.RawSetString("ok", lua.LString(msg))
			l.Push(res)
			return 1
		},
	}
	funcs["pcall"] = funcs["call"]
	return funcs
}

func luaToRedis(l *lua.LState, c *server.Peer, value lua.LValue) {
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
		t := value.(*lua.LTable)
		// special case for tables with an 'err' or 'ok' field
		// note: according to the docs this only counts when 'err' or 'ok' is
		// the only field.
		if s := t.RawGetString("err"); s.Type() != lua.LTNil {
			c.WriteError(s.String())
			return
		}
		if s := t.RawGetString("ok"); s.Type() != lua.LTNil {
			c.WriteInline(s.String())
			return
		}

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
			luaToRedis(l, c, r)
		}
	default:
		c.WriteInline(lua.LVAsString(value))
	}
}

func redisToLua(l *lua.LState, res []interface{}) *lua.LTable {
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
				v = redisToLua(l, et)
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
