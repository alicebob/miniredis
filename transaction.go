package miniredis

import (
	"github.com/bsm/redeo"
)

// withTx wraps the non-argument-checking part of command handling code in
// transaction logic.
func withTx(
	m *Miniredis,
	out *redeo.Responder,
	r *redeo.Request,
	cb txCmd,
) error {
	ctx := getCtx(r.Client())
	if inTx(ctx) {
		addTxCmd(ctx, cb)
		out.WriteInlineString("QUEUED")
		return nil
	}
	m.Lock()
	defer m.Unlock()
	cb(out, ctx)
	return nil
}
