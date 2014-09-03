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

 - PING
 - GET
 - SET -- only the simple version, arguments are not supported

Since this is intended to be used in unittests timeouts are not implemented. (but you should be able to check what their EXPIRE values are. One day).
