// +build int

package main

// Start a redis server in memory-only mode on a random port.

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"strconv"
	"time"
)

const (
	localSrc   = "./redis_src/"
	executable = "redis-server"
)

type ephemeral exec.Cmd

// Redis starts a memory-only redis on a random port. Will panic if that
// doesn't work.
// Returns something which you'll have to Close(), and a string to give to Dial()
func Redis() (*ephemeral, string) {
	return runRedis("")
}

// RedisAuth starts a memory-only redis on a random port. The redis has
// authentication enabled. See Redis()
func RedisAuth(passwd string) (*ephemeral, string) {
	return runRedis(fmt.Sprintf("requirepass %s", passwd))
}

// RedisUserAuth starts a memory-only redis on a random port. The redis has
// ACL rules enabled. See Redis()
func RedisUserAuth(users map[string]string) (*ephemeral, string) {
	acls := "user default on -@all +hello\n"
	for user, pass := range users {
		acls += fmt.Sprintf("user %s on +@all ~* >%s\n", user, pass)
	}
	return runRedis(acls)
}

// RedisCluster starts a memory-only redis on a random port. The redis has
// cluster mode enabled. See Redis()
func RedisCluster() (*ephemeral, string) {
	return runRedis("cluster-enabled yes\ncluster-config-file nodes.conf")
}

func RedisTLS() (*ephemeral, string) {
	port := arbitraryPort()
	e, _ := runRedis(fmt.Sprintf(
		`
			tls-port %d
			tls-cert-file ../testdata/server.crt
			tls-key-file ../testdata/server.key
			tls-ca-cert-file ../testdata/client.crt
		`,
		port))
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	return e, addr
}

func runRedis(extraConfig string) (*ephemeral, string) {
	port := arbitraryPort()

	// we prefer the executable from ./redis_src, if any. See ./get_redis.sh
	os.Setenv("PATH", fmt.Sprintf("%s:%s", localSrc, os.Getenv("PATH")))

	c := exec.Command(executable, "-")
	stdin, err := c.StdinPipe()
	if err != nil {
		panic(err)
	}
	fmt.Fprintf(stdin, "port %d\nbind 127.0.0.1\nappendonly no\n%s", port, extraConfig)
	stdin.Close()
	if err := c.Start(); err != nil {
		panic(err)
	}

	addr := fmt.Sprintf("127.0.0.1:%d", port)

	// Wait until the thing is ready
	timeout := time.Now().Add(1 * time.Second)
	for time.Now().Before(timeout) {
		conn, err := net.Dial("tcp", addr)
		if err == nil {
			conn.Close()
			e := ephemeral(*c)
			return &e, addr
		}
		time.Sleep(3 * time.Millisecond)
	}
	panic(fmt.Sprintf("No connection on port %d", port))
}

func (e *ephemeral) Close() {
	((*exec.Cmd)(e)).Process.Kill()
	((*exec.Cmd)(e)).Wait()
}

// arbitraryPort returns a non-used port.
func arbitraryPort() int {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}

	defer l.Close()
	addr := l.Addr().String()
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		panic(err)
	}
	p, _ := strconv.Atoi(port)
	return p
}
