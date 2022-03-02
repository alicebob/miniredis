package miniredis

import (
	"strconv"

	"github.com/alicebob/miniredis/v2/server"
)

// optInt parses an int option in a command.
// Writes "invalid integer" error to c if it's not a valid integer. Returns
// whether or not things were okay.
func optInt(c *server.Peer, src string, dest *int) bool {
	n, err := strconv.Atoi(src)
	if err != nil {
		setDirty(c)
		c.WriteError(msgInvalidInt)
		return false
	}
	*dest = n
	return true
}

// optLexrange handles ZRANGE{,BYLEX} ranges. They start with '[', '(', or are
// '+' or '-'.
// Sets destValue and destInclusive. destValue can be '+' or '-'.
// Returns whether or not things were okay.
func optLexrange(c *server.Peer, s string, destValue *string, destInclusive *bool) bool {
	if len(s) == 0 {
		setDirty(c)
		c.WriteError(msgInvalidRangeItem)
		return false
	}

	if s == "+" || s == "-" {
		*destValue = s
		*destInclusive = false
		return true
	}

	switch s[0] {
	case '(':
		*destValue = s[1:]
		*destInclusive = false
		return true
	case '[':
		*destValue = s[1:]
		*destInclusive = true
		return true
	default:
		setDirty(c)
		c.WriteError(msgInvalidRangeItem)
		return false
	}
}
