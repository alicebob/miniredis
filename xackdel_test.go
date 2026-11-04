package miniredis

import (
	"testing"

	"github.com/alicebob/miniredis/v2/proto"
)

func TestXackdelKeepsMessageUntilAllGroupsAcknowledge(t *testing.T) {
	_, c := runWithClient(t)
	mustDo(t, c, "XADD", "orders", "1-0", "id", "42", proto.String("1-0"))
	mustOK(t, c, "XGROUP", "CREATE", "orders", "stock", "0")
	mustOK(t, c, "XGROUP", "CREATE", "orders", "notify", "0")
	entry := proto.Array(proto.String("orders"), proto.Array(proto.Array(proto.String("1-0"), proto.Strings("id", "42"))))
	mustDo(t, c, "XREADGROUP", "GROUP", "stock", "worker", "STREAMS", "orders", ">", proto.Array(entry))
	mustDo(t, c, "XACKDEL", "orders", "stock", "ACKED", "IDS", "1", "1-0", proto.Array(proto.Int(2)))
	mustDo(t, c, "XREADGROUP", "GROUP", "notify", "worker", "STREAMS", "orders", ">", proto.Array(entry))
	mustDo(t, c, "XACKDEL", "orders", "notify", "ACKED", "IDS", "1", "1-0", proto.Array(proto.Int(1)))
	mustDo(t, c, "XRANGE", "orders", "-", "+", proto.Array())
}
