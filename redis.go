package miniredis

import (
	"fmt"
	"strings"

	"github.com/bsm/redeo"
)

const (
	msgWrongType   = "WRONGTYPE Operation against a key holding the wrong kind of value"
	msgInvalidInt  = "ERR value is not an integer or out of range"
	msgSyntaxError = "ERR syntax error"
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

// redisRange gives Go offsets for something l long with start/end in
// Redis semantics. Both start and end can be negative.
// Used for string range and list range things.
// The results can be used as: v[start:end]
func redisRange(l, start, end int) (int, int) {
	if start < 0 {
		start = l + start
		if start < 0 {
			start = 0
		}
	}
	if start > l {
		start = l
	}

	if end < 0 {
		end = l + end
		if end < 0 {
			end = 0
		}
	}
	end++ // end argument is inclusive in Redis.
	if end > l {
		end = l
	}

	if end < start {
		return 0, 0
	}
	return start, end
}
