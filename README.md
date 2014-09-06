# miniredis

Pure Go Redis server implementation, used in Go unittests.


##

Sometimes you want to test code which uses Redis, without making it a full-blown
integration test.
Miniredis implements (parts of) the Redis server, to be used in unittests. It
enables a simple, cheap, in-memory, Redis replacement, with a real TCP interface. Think of it as the Redis version of `net/http/httptest`.

It saves you from using mock code, and since the redis servers lives in the
test process you can query for values directly, without going through the server
stack.


## Commands

Implemented commands:

 - AUTH -- we accept every password
 - PING
 - ECHO
 - DEL
 - GET
 - SET
 - SETEX
 - SETNX
 - MGET
 - MSET
 - INCR
 - INCRBY
 - DECR
 - DECRBY
 - GETSET
 - HDEL
 - HEXISTS
 - HGET
 - HGETALL
 - HKEYS
 - HLEN
 - HMGET
 - HSET
 - HSETNX
 - HVALS
 - EXPIRE
 - TTL
 - PERSIST
 - SELECT

MULTI and EXEC are accepted but ignored (for now).

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

	// Optionally set keys your code expects:
	s.Set("foo", "bar")

	// Run your code and see if it behaves.
	// A connect example using the redigo libary from 
	// "github.com/garyburd/redigo/redis":
	c, err := redis.Dial("tcp", s.Addr())
	_, err = c.Do("SET", "foo", "bar")

	// Optionally check the server values:
	s.Get("foo")
}
```
