// Command 'INFO' from https://redis.io/commands/info/

package miniredis

import (
	"fmt"

	"github.com/alicebob/miniredis/v2/server"
)

func commandsInfo(m *Miniredis) {
	const cmdName = "INFO"

	if err := m.srv.Register(cmdName, m.cmdInfo); err != nil {
		panic(fmt.Errorf("register command (%s) failed with error: %s", cmdName, err.Error()))
	}
}

func (m *Miniredis) cmdInfo(c *server.Peer, cmd string, args []string) {
	if !m.isValidCMD(c, cmd) {
		return
	}

	if len(args) > 1 {
		setDirty(c)
		c.WriteError(errWrongNumber(cmd))
		return
	}

	withTx(m, c, func(c *server.Peer, ctx *connCtx) {
		const (
			clientsSectionName    = "clients"
			clientsSectionContent = "# Clients\nconnected_clients:%d\r\n"
		)

		var result string

		for _, key := range args {
			if key != clientsSectionName {
				setDirty(c)
				c.WriteError(fmt.Sprintf("section (%s) is not supported", key))
				return
			}
		}
		result = fmt.Sprintf(clientsSectionContent, m.Server().TotalConnections())

		c.WriteBulk(result)
	})
}
