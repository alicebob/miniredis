# Miniredis

Pure Go Redis test server, used in Go unittests.


##

Sometimes you want to test code which uses Redis, without making it a full-blown
integration test.
Miniredis implements (parts of) the Redis server, to be used in unittests. It
enables a simple, cheap, in-memory, Redis replacement, with a real TCP interface. Think of it as the Redis version of `net/http/httptest`.

It saves you from using mock code, and since the redis server lives in the
test process you can query for values directly, without going through the server
stack.

There are no dependencies on external binaries, so you can easily integrate it in automated build processes.


## Commands

Implemented commands:

 - Connection (complete)
   - AUTH -- we accept every password
   - ECHO
   - PING
   - SELECT
   - QUIT
 - Key 
   - DEL
   - EXISTS
   - EXPIRE
   - EXPIREAT
   - KEYS
   - MOVE
   - PERSIST
   - PEXPIRE
   - PEXPIREAT
   - PTTL
   - RENAME
   - RANDOMKEY -- call math.rand.Seed(...) before using.
   - TTL
   - TYPE
 - Transactions (complete)
   - DISCARD
   - EXEC
   - MULTI
   - UNWATCH
   - WATCH
 - String keys (complete)
   - APPEND
   - BITCOUNT
   - BITOP
   - BITPOS
   - DECR
   - DECRBY
   - GET
   - GETBIT
   - GETRANGE
   - GETSET
   - INCR
   - INCRBY
   - INCRBYFLOAT
   - MGET
   - MSET
   - MSETNX
   - PSETEX
   - SET
   - SETBIT
   - SETEX
   - SETNX
   - SETRANGE
   - STRLEN
 - Hash keys
   - HDEL
   - HEXISTS
   - HGET
   - HGETALL
   - HINCRBY
   - HINCRBYFLOAT
   - HKEYS
   - HLEN
   - HMGET
   - HMSET
   - HSET
   - HSETNX
   - HVALS
 - List keys
   - LINDEX
   - LLEN
   - LPOP
   - LPUSH
   - LRANGE
   - LTRIM
   - RPOP
   - RPUSH
 - Set keys
   - SADD
   - SCARD
   - SISMEMBER
   - SMEMBERS
   - SREM
 - Sorted Set keys
   - ZADD
   - ZCARD
   - ZRANGE
   - ZRANGEBYSCORE
   - ZRANK
   - ZDEL
   - ZREVRANGE
   - ZREVRANGEBYSCORE
   - ZSCORE


Since this is intended to be used in unittests timeouts are not implemented.
You can use `Expire()` to see if an expiration is set. The value returned will
be that what the client set, without any interpretation. This is to keep things
testable.

## Example

``` Go
func TestSomething(t *testing.T) {
	s, err := miniredis.Run()
	if err != nil {
		panic(err)
	}
	defer s.Close()

	// Optionally set some keys your code expects:
	s.Set("foo", "bar")
	s.HSet("some", "other", "key")

	// Run your code and see if it behaves.
	// An example using the redigo library from "github.com/garyburd/redigo/redis":
	c, err := redis.Dial("tcp", s.Addr())
	_, err = c.Do("SET", "foo", "bar")

	// Optionally check values in redis...
	if got, err := s.Get("foo"); err != nil || got != "bar" {
        t.Error("'foo' has the wrong value")
    }
    // ... or use a helper for that:
    s.CheckGet(t, "foo", "bar")
}
```

## Not supported

Commands which will probably not be implemented:

 - Key
    - ~~DUMP~~
    - ~~MIGRATE~~
    - ~~OBJECT~~
    - ~~RESTORE~~
 - Scripting (all)
    - ~~EVAL~~
    - ~~EVALSHA~~
    - ~~SCRIPT *~~
 - Server
    - ~~CLIENT *~~
    - ~~COMMAND *~~
    - ~~CONFIG *~~
    - ~~DEBUG *~~
    

## &c.

See https://github.com/alicebob/miniredis_vs_redis for tests comparing
miniredis against the real thing. Tests are run against Redis 2.8.7 (Debian
testing).


[![Build Status](https://travis-ci.org/alicebob/miniredis.svg?branch=master)](https://travis-ci.org/alicebob/miniredis)
