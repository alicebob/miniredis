package miniredis_test

import (
	"github.com/alicebob/miniredis"
	"github.com/garyburd/redigo/redis"
)

func Example() {
	s, err := miniredis.Run()
	if err != nil {
		panic(err)
	}
	defer s.Close()

	// Configure you application to connect to redis at s.Addr()
	// Any redis client should work, as long as you use redis commands which
	// miniredis implements.
	c, err := redis.Dial("tcp", s.Addr())
	if err != nil {
		panic(err)
	}
	_, err = c.Do("SET", "foo", "bar")
	if err != nil {
		panic(err)
	}

	// You can ask miniredis about keys directly, without going over the network.
	if s.Get("foo") != "bar" {
		panic("Didn't get 'bar' back")
	}
	// Or with a DB id
	if s.DB(42).Get("foo") != "" {
		panic("Didn't use a different DB")
	}

	// Check if there really was only one connection.
	if s.TotalConnectionCount() != 1 {
		panic("Too many connections made")
	}

	// Output:
}
