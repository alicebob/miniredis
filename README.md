# miniredis

Pure Go Redis server implementation, used in unittests.


##

Miniredis implements (parts of) the Redis server, to be used in unittests. It
enables a simple, quick, in-memory, Redis replacement, with a real TCP interface, a-la
`net/http/httptest`.

Key timeouts are not implemented (but you should be able to check what their EXPIRE
values are).
