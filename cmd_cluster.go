// Commands from https://redis.io/commands#cluster

package miniredis

import (
	"fmt"
	"github.com/alicebob/miniredis/v2/server"
	"strings"
)

// commandsCluster handles some cluster operations.
func commandsCluster(m *Miniredis) {
	_ = m.srv.Register("CLUSTER", m.cmdCluster)
}

func (m *Miniredis) cmdCluster(c *server.Peer, cmd string, args []string) {
	if len(args) == 1 && strings.ToUpper(args[0]) == "SLOTS" {
		m.cmdClusterSlots(c, cmd, args)
	} else {
		j := strings.Join(args, " ")
		err := fmt.Sprintf("ERR 'CLUSTER %s' not supported", j)
		setDirty(c)
		c.WriteError(err)
	}
}

// CLUSTER SLOTS
func (m *Miniredis) cmdClusterSlots(c *server.Peer, cmd string, args []string) {
	withTx(m, c, func(c *server.Peer, ctx *connCtx) {
		c.WriteLen(1)
		c.WriteLen(3)
		c.WriteInt(0)
		c.WriteInt(16383)
		c.WriteLen(3)
		c.WriteBulk(m.srv.Addr().IP.String())
		c.WriteInt(m.srv.Addr().Port)
		c.WriteBulk("09dbe9720cda62f7865eabc5fd8857c5d2678366")
	})
}
