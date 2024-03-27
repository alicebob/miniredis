package miniredis

import (
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2/server"
)

func TestRedis(t *testing.T) {
	s := RunT(t)

	peer := &server.Peer{}
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		blocking(s, peer, time.Second, func(p *server.Peer, cc *connCtx) bool {
			err := s.Ctx.Err()
			if err != nil {
				t.Error("blocking call should not retry command when context has error")
				return true
			}
			return false
		}, func(p *server.Peer) {
			// expect to time out
		})
	}()

	time.Sleep(time.Millisecond * 250)

	s.Close()
	wg.Wait()
}
