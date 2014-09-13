package miniredis

import (
	"fmt"
	"strings"

	"github.com/bsm/redeo"
)

const (
	msgWrongType = "WRONGTYPE Operation against a key holding the wrong kind of value"
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

// formatFloat formats a float the way redis does (sort-of)
func formatFloat(v float64) string {
	// Format with %f and strip trailing 0s. This is the most like Redis does
	// it :(
	// .12 is the magic number where most output is the same as Redis.
	sv := fmt.Sprintf("%.12f", v)
	for strings.Contains(sv, ".") {
		if sv[len(sv)-1] != '0' {
			break
		}
		// Remove trailing 0s.
		sv = sv[:len(sv)-1]
		// Ends with a '.'.
		if sv[len(sv)-1] == '.' {
			sv = sv[:len(sv)-1]
			break
		}
	}
	return sv
}
