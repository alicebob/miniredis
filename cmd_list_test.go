package miniredis

import (
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2/proto"
)

// execute command in a go routine. Used to test blocking commands.
func goStrings(t *testing.T, s *Miniredis, args ...string) <-chan string {
	c, err := proto.Dial(s.Addr())
	ok(t, err)

	got := make(chan string, 1)
	go func() {
		defer c.Close()
		defer close(got)
		res, err := c.Do(args...)
		if err != nil {
			t.Error(err.Error())
			return
		}
		got <- res
	}()
	return got
}

func TestLpush(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	t.Run("basic", func(t *testing.T) {
		mustDo(t, c,
			"LPUSH", "l", "aap", "noot", "mies",
			proto.Int(3), // new length.
		)

		mustDo(t, c,
			"LRANGE", "l", "0", "0",
			proto.Strings("mies"),
		)

		mustDo(t, c,
			"LRANGE", "l", "-1", "-1",
			proto.Strings("aap"),
		)

		mustDo(t, c,
			"LPUSH", "l", "aap2", "noot2", "mies2",
			proto.Int(6),
		)

		mustDo(t, c,
			"LRANGE", "l", "0", "0",
			proto.Strings("mies2"),
		)

		mustDo(t, c,
			"LRANGE", "l", "-1", "-1",
			proto.Strings("aap"),
		)
	})

	t.Run("direct", func(t *testing.T) {
		l, err := s.Lpush("l2", "a")
		ok(t, err)
		equals(t, 1, l)
		l, err = s.Lpush("l2", "b")
		ok(t, err)
		equals(t, 2, l)
		list, err := s.List("l2")
		ok(t, err)
		equals(t, []string{"b", "a"}, list)

		el, err := s.Lpop("l2")
		ok(t, err)
		equals(t, "b", el)
		el, err = s.Lpop("l2")
		ok(t, err)
		equals(t, "a", el)
		// Key is removed on pop-empty.
		equals(t, false, s.Exists("l2"))
	})

	t.Run("direct, wakeup", func(t *testing.T) {
		go func() {
			time.Sleep(30 * time.Millisecond)
			l, err := s.Lpush("q1", "a")
			ok(t, err)
			equals(t, 1, l)
		}()

		mustDo(t, c,
			"BRPOPLPUSH", "q1", "q2", "1",
			proto.String("a"),
		)
	})

	t.Run("errors", func(t *testing.T) {
		mustDo(t, c,
			"LPUSH",
			proto.Error("ERR wrong number of arguments for 'lpush' command"),
		)
		mustDo(t, c,
			"LPUSH", "l",
			proto.Error("ERR wrong number of arguments for 'lpush' command"),
		)
		mustOK(t, c, "SET", "str", "value")
		mustDo(t, c,
			"LPUSH", "str", "noot", "mies",
			proto.Error(msgWrongType),
		)
	})
}

func TestLpushx(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	{
		must0(t, c,
			"LPUSHX", "l", "aap",
		)
		equals(t, false, s.Exists("l"))

		// Create the list with a normal LPUSH
		must1(t, c,
			"LPUSH", "l", "noot",
		)
		equals(t, true, s.Exists("l"))

		mustDo(t, c,
			"LPUSHX", "l", "mies",
			proto.Int(2),
		)
		equals(t, true, s.Exists("l"))
	}

	// Push more.
	{
		must1(t, c,
			"LPUSH", "l2", "aap1",
		)
		mustDo(t, c,
			"LPUSHX", "l2", "aap2", "noot2", "mies2",
			proto.Int(4),
		)

		mustDo(t, c,
			"LRANGE", "l2", "0", "0",
			proto.Strings("mies2"),
		)

		mustDo(t, c,
			"LRANGE", "l2", "-1", "-1",
			proto.Strings("aap1"),
		)
	}

	// Errors
	{
		mustDo(t, c,
			"LPUSHX",
			proto.Error("ERR wrong number of arguments for 'lpushx' command"),
		)
		mustDo(t, c,
			"LPUSHX", "l",
			proto.Error("ERR wrong number of arguments for 'lpushx' command"),
		)

		mustOK(t, c, "SET", "str", "value")
		mustDo(t, c,
			"LPUSHX", "str", "mies",
			proto.Error(msgWrongType),
		)
	}

}

func TestLpop(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	t.Run("single", func(t *testing.T) {
		mustDo(t, c,
			"LPUSH", "l", "aap", "noot", "mies",
			proto.Int(3),
		)

		mustDo(t, c,
			"LPOP", "l",
			proto.String("mies"),
		)

		mustDo(t, c,
			"LPOP", "l",
			proto.String("noot"),
		)

		mustDo(t, c,
			"LPOP", "l",
			proto.String("aap"),
		)

		// Last element has been popped. Key is gone.
		must0(t, c, "EXISTS", "l")

		// Can pop non-existing keys just fine.
		mustNil(t, c, "LPOP", "l")
	})

	t.Run("with count", func(t *testing.T) {
		mustDo(t, c,
			"LPUSH", "l2", "aap", "noot", "mies",
			proto.Int(3),
		)

		mustDo(t, c,
			"LPOP", "l2", "2",
			proto.Strings("mies", "noot"),
		)

		mustDo(t, c,
			"LPOP", "l2", "2",
			proto.Strings("aap"),
		)

		mustDo(t, c,
			"LPOP", "l2", "99",
			proto.NilList,
		)

		mustDo(t, c,
			"LPOP", "l2", "0",
			proto.NilList,
		)

		// Last element has been popped. Key is gone.
		must0(t, c, "EXISTS", "l2")
	})

	t.Run("errors", func(t *testing.T) {
		mustOK(t, c, "SET", "str", "value")
		mustDo(t, c,
			"LPOP", "str",
			proto.Error(msgWrongType),
		)
		mustDo(t, c,
			"LPOP", "str", "-1",
			proto.Error(msgOutOfRange),
		)
	})

	useRESP3(t, c)
	t.Run("RESP3", func(t *testing.T) {
		mustDo(t, c, "LPOP", "nosuch", proto.NilResp3)
		mustDo(t, c, "LPOP", "nosuch", "2", proto.NilResp3)
	})
}

func TestRPushPop(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	{
		mustDo(t, c,
			"RPUSH", "l", "aap", "noot", "mies",
			proto.Int(3),
		)

		mustDo(t, c,
			"LRANGE", "l", "0", "0",
			proto.Strings("aap"),
		)

		mustDo(t, c,
			"LRANGE", "l", "-1", "-1",
			proto.Strings("mies"),
		)
	}

	// Push more.
	{
		mustDo(t, c,
			"RPUSH", "l", "aap2", "noot2", "mies2",
			proto.Int(6),
		)

		mustDo(t, c,
			"LRANGE", "l", "0", "0",
			proto.Strings("aap"),
		)

		mustDo(t, c,
			"LRANGE", "l", "-1", "-1",
			proto.Strings("mies2"),
		)
	}

	// Direct usage
	{
		l, err := s.Push("l2", "a")
		ok(t, err)
		equals(t, 1, l)
		l, err = s.Push("l2", "b")
		ok(t, err)
		equals(t, 2, l)
		list, err := s.List("l2")
		ok(t, err)
		equals(t, []string{"a", "b"}, list)

		el, err := s.Pop("l2")
		ok(t, err)
		equals(t, "b", el)
		el, err = s.Pop("l2")
		ok(t, err)
		equals(t, "a", el)
		// Key is removed on pop-empty.
		equals(t, false, s.Exists("l2"))
	}

	// Wrong type of key
	{
		mustOK(t, c, "SET", "str", "value")
		mustDo(t, c,
			"RPUSH", "str", "noot", "mies",
			proto.Error(msgWrongType),
		)
	}
}

func TestRpop(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	s.Push("l", "aap", "noot", "mies")

	// Simple pops.
	{
		mustDo(t, c,
			"RPOP", "l",
			proto.String("mies"),
		)

		mustDo(t, c,
			"RPOP", "l",
			proto.String("noot"),
		)

		mustDo(t, c,
			"RPOP", "l",
			proto.String("aap"),
		)

		// Last element has been popped. Key is gone.
		must0(t, c, "EXISTS", "l")

		// Can pop non-existing keys just fine.
		mustNil(t, c, "RPOP", "l")
	}
}

func TestLindex(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	s.Push("l", "aap", "noot", "mies", "vuur")

	mustDo(t, c,
		"LINDEX", "l", "0",
		proto.String("aap"),
	)
	mustDo(t, c,
		"LINDEX", "l", "1",
		proto.String("noot"),
	)
	mustDo(t, c,
		"LINDEX", "l", "3",
		proto.String("vuur"),
	)

	mustNil(t, c, "LINDEX", "l", "3000") // Too many

	mustDo(t, c,
		"LINDEX", "l", "-1",
		proto.String("vuur"),
	)

	mustDo(t, c,
		"LINDEX", "l", "-2",
		proto.String("mies"),
	)

	mustNil(t, c, "LINDEX", "l", "-400") // Too big

	// Non existing key
	mustNil(t, c, "LINDEX", "nonexisting", "400")

	t.Run("errors", func(t *testing.T) {
		// Wrong type of key
		mustOK(t, c, "SET", "str", "value")
		mustDo(t, c,
			"LINDEX", "str", "1",
			proto.Error(msgWrongType),
		)

		// Not an integer
		mustDo(t, c,
			"LINDEX", "l", "noint",
			proto.Error("ERR value is not an integer or out of range"),
		)
		// Too many arguments
		mustDo(t, c,
			"LINDEX", "str", "l", "foo",
			proto.Error("ERR wrong number of arguments for 'lindex' command"),
		)
	})
}

func TestLpos(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	s.Push("l", "aap", "noot", "aap", "mies", "aap", "vuur", "aap", "aap")

	// Simple LPOS.
	// [aap, noot, aap, mies, aap, vuur, aap, aap]
	mustDo(t, c,
		"LPOS", "l", "aap",
		proto.Int(0),
	)
	mustDo(t, c,
		"LPOS", "l", "noot",
		proto.Int(1),
	)
	mustDo(t, c,
		"LPOS", "l", "mies",
		proto.Int(3),
	)
	mustDo(t, c,
		"LPOS", "l", "vuur",
		proto.Int(5),
	)
	mustNil(t, c, "LPOS", "l", "wim")

	// LPOS with RANK option.
	// [aap, noot, aap, mies, aap, vuur, aap, aap]
	mustDo(t, c,
		"LPOS", "l", "aap", "RANK", "1",
		proto.Int(0),
	)
	mustDo(t, c,
		"LPOS", "l", "aap", "RANK", "4",
		proto.Int(6),
	)
	mustDo(t, c,
		"LPOS", "l", "aap", "RANK", "5",
		proto.Int(7),
	)
	mustNil(t, c, "LPOS", "l", "aap", "RANK", "6")
	mustDo(t, c,
		"LPOS", "l", "aap", "RANK", "-1",
		proto.Int(7),
	)
	mustDo(t, c,
		"LPOS", "l", "aap", "RANK", "-3",
		proto.Int(4),
	)
	mustDo(t, c,
		"LPOS", "l", "aap", "RANK", "-5",
		proto.Int(0),
	)
	mustNil(t, c, "LPOS", "l", "aap", "RANK", "-6")

	// LPOS with COUNT
	// When COUNT is specified always return a list.
	// [aap, noot, aap, mies, aap, vuur, aap, aap]
	mustDo(t, c,
		"LPOS", "l", "wim", "COUNT", "1",
		proto.Ints())
	mustDo(t, c,
		"LPOS", "l", "aap", "COUNT", "1",
		proto.Ints(0),
	)
	mustDo(t, c,
		"LPOS", "l", "aap", "COUNT", "3",
		proto.Ints(0, 2, 4),
	)
	mustDo(t, c,
		"LPOS", "l", "aap", "COUNT", "5",
		proto.Ints(0, 2, 4, 6, 7),
	)
	mustDo(t, c,
		"LPOS", "l", "aap", "COUNT", "100",
		proto.Ints(0, 2, 4, 6, 7),
	)
	mustDo(t, c,
		// COUNT 0 means "unlimited".
		"LPOS", "l", "aap", "COUNT", "0",
		proto.Ints(0, 2, 4, 6, 7),
	)

	// LPOS with RANK and COUNT
	// [aap, noot, aap, mies, aap, vuur, aap, aap]
	mustDo(t, c,
		"LPOS", "l", "aap", "RANK", "3", "COUNT", "2",
		proto.Ints(4, 6),
	)
	mustDo(t, c,
		"LPOS", "l", "aap", "RANK", "3", "COUNT", "3",
		proto.Ints(4, 6, 7),
	)
	mustDo(t, c,
		"LPOS", "l", "aap", "RANK", "5", "COUNT", "100",
		proto.Ints(7),
	)
	mustDo(t, c,
		"LPOS", "l", "aap", "RANK", "-3", "COUNT", "2",
		proto.Ints(4, 2),
	)
	mustDo(t, c,
		"LPOS", "l", "aap", "RANK", "-3", "COUNT", "3",
		proto.Ints(4, 2, 0),
	)
	mustDo(t, c,
		"LPOS", "l", "aap", "RANK", "-5", "COUNT", "100",
		proto.Ints(0),
	)

	// LPOS with RANK and MAXLEN
	// [aap, noot, aap, mies, aap, vuur, aap, aap]
	mustNil(t, c, "LPOS", "l", "aap", "RANK", "4", "MAXLEN", "6")
	mustDo(t, c,
		"LPOS", "l", "aap", "RANK", "4", "MAXLEN", "7",
		proto.Int(6),
	)
	mustNil(t, c, "LPOS", "l", "aap", "RANK", "-4", "MAXLEN", "5")
	mustDo(t, c,
		"LPOS", "l", "aap", "RANK", "-4", "MAXLEN", "6",
		proto.Int(2),
	)

	// LPOS with COUNT and MAXLEN
	// [aap, noot, aap, mies, aap, vuur, aap, aap]
	mustDo(t, c,
		"LPOS", "l", "aap", "COUNT", "0", "MAXLEN", "1",
		proto.Ints(0),
	)
	mustDo(t, c,
		"LPOS", "l", "aap", "COUNT", "0", "MAXLEN", "4",
		proto.Ints(0, 2),
	)
	mustDo(t, c,
		"LPOS", "l", "aap", "COUNT", "0", "MAXLEN", "7",
		proto.Ints(0, 2, 4, 6),
	)
	mustDo(t, c,
		"LPOS", "l", "aap", "COUNT", "0", "MAXLEN", "8",
		proto.Ints(0, 2, 4, 6, 7),
	)
	mustDo(t, c,
		// MAXLEN 0 means "unlimited".
		"LPOS", "l", "aap", "COUNT", "0", "MAXLEN", "0",
		proto.Ints(0, 2, 4, 6, 7),
	)
	mustDo(t, c,
		"LPOS", "l", "aap", "COUNT", "2", "MAXLEN", "0",
		proto.Ints(0, 2),
	)
	mustDo(t, c,
		"LPOS", "l", "aap", "COUNT", "1", "MAXLEN", "0",
		proto.Ints(0),
	)

	// LPOS with RANK, COUNT, and MAXLEN
	// [aap, noot, aap, mies, aap, vuur, aap, aap]
	mustDo(t, c,
		"LPOS", "l", "aap", "RANK", "4", "COUNT", "2", "MAXLEN", "0",
		proto.Ints(6, 7))
	mustDo(t, c,
		"LPOS", "l", "aap", "RANK", "4", "COUNT", "2", "MAXLEN", "7",
		proto.Ints(6))
	mustDo(t, c,
		"LPOS", "l", "aap", "RANK", "4", "COUNT", "2", "MAXLEN", "6",
		proto.Ints())
	mustDo(t, c,
		"LPOS", "l", "aap", "RANK", "-3", "COUNT", "2", "MAXLEN", "0",
		proto.Ints(4, 2))
	mustDo(t, c,
		"LPOS", "l", "aap", "RANK", "-3", "COUNT", "2", "MAXLEN", "4",
		proto.Ints(4))
	mustDo(t, c,
		"LPOS", "l", "aap", "RANK", "-3", "COUNT", "2", "MAXLEN", "3",
		proto.Ints())

	t.Run("errors", func(t *testing.T) {
		// Wrong type of key.
		mustOK(t, c, "SET", "str", "value")
		mustDo(t, c,
			"LPOS", "str", "value",
			proto.Error(msgWrongType),
		)

		// Wrong number of arguments.
		mustDo(t, c,
			"LPOS", "l",
			proto.Error("ERR wrong number of arguments for 'lpos' command"),
		)

		// Wrong number of options.
		mustDo(t, c,
			"LPOS", "l", "aap", "RANK",
			proto.Error("ERR syntax error"),
		)
		mustDo(t, c,
			"LPOS", "l", "aap", "RANK", "1", "COUNT",
			proto.Error("ERR syntax error"),
		)
		mustDo(t, c,
			"LPOS", "l", "aap", "RANK", "1", "COUNT", "1", "MAXLEN",
			proto.Error("ERR syntax error"),
		)
		mustDo(t, c,
			"LPOS", "l", "aap", "RANK", "1", "COUNT", "1", "MAXLEN", "1", "RANK",
			proto.Error("ERR syntax error"),
		)

		// Invalid options.
		mustDo(t, c,
			"LPOS", "l", "aap", "RANKS", "1",
			proto.Error("ERR syntax error"))
		mustDo(t, c,
			"LPOS", "l", "aap", "RANK", "1", "COUNTING", "1",
			proto.Error("ERR syntax error"))
		mustDo(t, c,
			"LPOS", "l", "aap", "RANK", "1", "MAXLENGTH", "1",
			proto.Error("ERR syntax error"))

		// Invalid option values.
		mustDo(t, c,
			"LPOS", "l", "aap", "RANK", "not_an_int",
			proto.Error("ERR value is not an integer or out of range"))
		mustDo(t, c,
			"LPOS", "l", "aap", "RANK", "0",
			proto.Error("ERR RANK can't be zero: use 1 to start from the first match, 2 from the second ... or use negative to start from the end of the list"))
		mustDo(t, c,
			"LPOS", "l", "aap", "COUNT", "-1",
			proto.Error("ERR COUNT can't be negative"))
		mustDo(t, c,
			"LPOS", "l", "aap", "COUNT", "not_an_int",
			// Redis (incorrectly?) reports this as a negative number.
			proto.Error("ERR COUNT can't be negative"))
		mustDo(t, c,
			"LPOS", "l", "aap", "MAXLEN", "-1",
			proto.Error("ERR MAXLEN can't be negative"))
		mustDo(t, c,
			"LPOS", "l", "aap", "MAXLEN", "not_an_int",
			// Redis (incorrectly?) reports this as a negative number.
			proto.Error("ERR MAXLEN can't be negative"))

		// First invalid option encountered reports the error.
		mustDo(t, c,
			"LPOS", "l", "aap", "MAXLEN", "-1", "RANK", "not_an_int", "COUNT", "-1",
			proto.Error("ERR MAXLEN can't be negative"))
		mustDo(t, c,
			"LPOS", "l", "aap", "RANK", "not_an_int", "COUNT", "-1", "MAXLEN", "-1",
			proto.Error("ERR value is not an integer or out of range"))
		mustDo(t, c,
			"LPOS", "l", "aap", "COUNT", "-1", "MAXLEN", "-1", "RANK", "not_an_int",
			proto.Error("ERR COUNT can't be negative"))
	})
}

func TestLlen(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	s.Push("l", "aap", "noot", "mies", "vuur")

	mustDo(t, c,
		"LLEN", "l",
		proto.Int(4),
	)

	// Non existing key
	must0(t, c,
		"LLEN", "nonexisting",
	)

	// Wrong type of key
	mustOK(t, c, "SET", "str", "value")
	mustDo(t, c,
		"LLEN", "str",
		proto.Error(msgWrongType),
	)

	// Too many arguments
	mustDo(t, c,
		"LLEN", "too", "many",
		proto.Error("ERR wrong number of arguments for 'llen' command"),
	)
}

func TestLtrim(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	s.Push("l", "aap", "noot", "mies", "vuur")

	{
		mustOK(t, c, "LTRIM", "l", "0", "2")
		l, err := s.List("l")
		ok(t, err)
		equals(t, []string{"aap", "noot", "mies"}, l)
	}

	// Delete key on empty list
	{
		mustOK(t, c, "LTRIM", "l", "0", "-99")
		equals(t, false, s.Exists("l"))
	}

	// Not existing key
	mustOK(t, c, "LTRIM", "nonexisting", "0", "1")

	// Wrong type of key
	t.Run("errors", func(t *testing.T) {
		s.Set("str", "string!")
		mustDo(t, c,
			"LTRIM", "str", "0", "1",
			proto.Error(msgWrongType),
		)

		mustDo(t, c,
			"LTRIM", "l", "1", "2", "toomany",
			proto.Error(errWrongNumber("ltrim")),
		)
		mustDo(t, c,
			"LTRIM", "l", "1", "noint",
			proto.Error("ERR value is not an integer or out of range"),
		)
		mustDo(t, c,
			"LTRIM", "l", "noint", "1",
			proto.Error("ERR value is not an integer or out of range"),
		)
		mustDo(t, c,
			"LTRIM", "l", "1",
			proto.Error(errWrongNumber("ltrim")),
		)
		mustDo(t, c,
			"LTRIM", "l",
			proto.Error(errWrongNumber("ltrim")),
		)
		mustDo(t, c,
			"LTRIM",
			proto.Error(errWrongNumber("ltrim")),
		)
	})
}

func TestLrem(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	// Reverse
	{
		s.Push("l", "aap", "noot", "mies", "vuur", "noot", "noot")
		must1(t, c,
			"LREM", "l", "-1", "noot",
		)
		l, err := s.List("l")
		ok(t, err)
		equals(t, []string{"aap", "noot", "mies", "vuur", "noot"}, l)
	}
	// Normal
	{
		s.Push("l2", "aap", "noot", "mies", "vuur", "noot", "noot")
		mustDo(t, c,
			"LREM", "l2", "2", "noot",
			proto.Int(2),
		)
		l, err := s.List("l2")
		ok(t, err)
		equals(t, []string{"aap", "mies", "vuur", "noot"}, l)
	}

	// All
	{
		s.Push("l3", "aap", "noot", "mies", "vuur", "noot", "noot")
		mustDo(t, c,
			"LREM", "l3", "0", "noot",
			proto.Int(3),
		)
		l, err := s.List("l3")
		ok(t, err)
		equals(t, []string{"aap", "mies", "vuur"}, l)
	}

	// All
	{
		s.Push("l4", "aap", "noot", "mies", "vuur", "noot", "noot")
		mustDo(t, c,
			"LREM", "l4", "200", "noot",
			proto.Int(3),
		)
		l, err := s.List("l4")
		ok(t, err)
		equals(t, []string{"aap", "mies", "vuur"}, l)
	}

	// Delete key on empty list
	{
		s.Push("l5", "noot", "noot", "noot")
		mustDo(t, c,
			"LREM", "l5", "99", "noot",
			proto.Int(3),
		)
		equals(t, false, s.Exists("l5"))
	}

	// Non existing key
	must0(t, c,
		"LREM", "nonexisting", "0", "aap",
	)

	// Error cases
	{
		mustDo(t, c,
			"LREM",
			proto.Error(errWrongNumber("lrem")),
		)
		mustDo(t, c,
			"LREM", "l",
			proto.Error(errWrongNumber("lrem")),
		)
		mustDo(t, c,
			"LREM", "l", "1",
			proto.Error(errWrongNumber("lrem")),
		)
		mustDo(t, c,
			"LREM", "l", "noint", "aap",
			proto.Error("ERR value is not an integer or out of range"),
		)
		mustDo(t, c,
			"LREM", "l", "1", "aap", "toomany",
			proto.Error(errWrongNumber("lrem")),
		)
		s.Set("str", "string!")
		mustDo(t, c,
			"LREM", "str", "0", "aap",
			proto.Error("WRONGTYPE Operation against a key holding the wrong kind of value"),
		)
	}
}

func TestLset(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	s.Push("l", "aap", "noot", "mies", "vuur", "noot", "noot")
	// Simple LSET
	{
		mustOK(t, c, "LSET", "l", "1", "noot!")
		l, err := s.List("l")
		ok(t, err)
		equals(t, []string{"aap", "noot!", "mies", "vuur", "noot", "noot"}, l)
	}

	{
		mustOK(t, c,
			"LSET", "l", "-1", "noot?",
		)
		l, err := s.List("l")
		ok(t, err)
		equals(t, []string{"aap", "noot!", "mies", "vuur", "noot", "noot?"}, l)
	}

	// Out of range
	mustDo(t, c,
		"LSET", "l", "10000", "aap",
		proto.Error("ERR index out of range"),
	)
	mustDo(t, c,
		"LSET", "l", "-10000", "aap",
		proto.Error("ERR index out of range"),
	)

	// Non existing key
	mustDo(t, c,
		"LSET", "nonexisting", "0", "aap",
		proto.Error("ERR no such key"),
	)

	t.Run("errors", func(t *testing.T) {
		mustDo(t, c,
			"LSET",
			proto.Error(errWrongNumber("lset")),
		)
		mustDo(t, c,
			"LSET", "l",
			proto.Error(errWrongNumber("lset")),
		)
		mustDo(t, c,
			"LSET", "l", "1",
			proto.Error(errWrongNumber("lset")),
		)
		mustDo(t, c,
			"LSET", "l", "noint", "aap",
			proto.Error("ERR value is not an integer or out of range"),
		)
		mustDo(t, c,
			"LSET", "l", "1", "aap", "toomany",
			proto.Error(errWrongNumber("lset")),
		)

		s.Set("str", "string!")
		mustDo(t, c,
			"LSET", "str", "0", "aap",
			proto.Error(msgWrongType),
		)
	})
}

func TestLinsert(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	s.Push("l", "aap", "noot", "mies", "vuur", "noot", "end")
	// Before
	{
		mustDo(t, c,
			"LINSERT", "l", "BEFORE", "noot", "!",
			proto.Int(7),
		)
		l, err := s.List("l")
		ok(t, err)
		equals(t, []string{"aap", "!", "noot", "mies", "vuur", "noot", "end"}, l)
	}

	// After
	{
		mustDo(t, c,
			"LINSERT", "l", "AFTER", "noot", "?",
			proto.Int(8),
		)
		l, err := s.List("l")
		ok(t, err)
		equals(t, []string{"aap", "!", "noot", "?", "mies", "vuur", "noot", "end"}, l)
	}

	// Edge case before
	{
		mustDo(t, c,
			"LINSERT", "l", "BEFORE", "aap", "[",
			proto.Int(9),
		)
		l, err := s.List("l")
		ok(t, err)
		equals(t, []string{"[", "aap", "!", "noot", "?", "mies", "vuur", "noot", "end"}, l)
	}

	// Edge case after
	{
		mustDo(t, c,
			"LINSERT", "l", "AFTER", "end", "]",
			proto.Int(10),
		)
		l, err := s.List("l")
		ok(t, err)
		equals(t, []string{"[", "aap", "!", "noot", "?", "mies", "vuur", "noot", "end", "]"}, l)
	}

	// Non existing pivot
	mustDo(t, c,
		"LINSERT", "l", "before", "nosuch", "noot",
		proto.Int(-1),
	)

	// Non existing key
	must0(t, c,
		"LINSERT", "nonexisting", "before", "aap", "noot",
	)

	t.Run("errors", func(t *testing.T) {
		mustDo(t, c,
			"LINSERT",
			proto.Error(errWrongNumber("linsert")),
		)
		mustDo(t, c,
			"LINSERT", "l",
			proto.Error(errWrongNumber("linsert")),
		)
		mustDo(t, c,
			"LINSERT", "l", "before",
			proto.Error(errWrongNumber("linsert")),
		)
		mustDo(t, c,
			"LINSERT", "l", "before", "value",
			proto.Error(errWrongNumber("linsert")),
		)
		mustDo(t, c,
			"LINSERT", "l", "wrong", "value", "value",
			proto.Error("ERR syntax error"),
		)
		mustDo(t, c,
			"LINSERT", "l", "wrong", "value", "value", "toomany",
			proto.Error(errWrongNumber("linsert")),
		)

		s.Set("str", "string!")
		mustDo(t, c,
			"LINSERT", "str", "before", "value", "value",
			proto.Error(msgWrongType),
		)
	})
}

func TestRpoplpush(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	s.Push("l", "aap", "noot", "mies")
	s.Push("l2", "vuur", "noot", "end")
	{
		mustDo(t, c,
			"RPOPLPUSH", "l", "l2",
			proto.String("mies"),
		)
		s.CheckList(t, "l", "aap", "noot")
		s.CheckList(t, "l2", "mies", "vuur", "noot", "end")
	}
	// Again!
	{
		mustDo(t, c,
			"RPOPLPUSH", "l", "l2",
			proto.String("noot"),
		)
		s.CheckList(t, "l", "aap")
		s.CheckList(t, "l2", "noot", "mies", "vuur", "noot", "end")
	}
	// Again!
	{
		mustDo(t, c,
			"RPOPLPUSH", "l", "l2",
			proto.String("aap"),
		)
		assert(t, !s.Exists("l"), "l exists")
		s.CheckList(t, "l2", "aap", "noot", "mies", "vuur", "noot", "end")
	}

	// Non existing lists
	{
		s.Push("ll", "aap", "noot", "mies")

		mustDo(t, c,
			"RPOPLPUSH", "ll", "nosuch",
			proto.String("mies"),
		)
		assert(t, s.Exists("nosuch"), "nosuch exists")
		s.CheckList(t, "ll", "aap", "noot")
		s.CheckList(t, "nosuch", "mies")

		mustNil(t, c,
			"RPOPLPUSH", "nosuch2", "ll",
		)
	}

	// Cycle
	{
		s.Push("cycle", "aap", "noot", "mies")

		mustDo(t, c,
			"RPOPLPUSH", "cycle", "cycle",
			proto.String("mies"),
		)
		s.CheckList(t, "cycle", "mies", "aap", "noot")
	}

	// Error cases
	t.Run("errors", func(t *testing.T) {
		s.Push("src", "aap", "noot", "mies")
		mustDo(t, c,
			"RPOPLPUSH",
			proto.Error(errWrongNumber("rpoplpush")),
		)
		mustDo(t, c,
			"RPOPLPUSH", "l",
			proto.Error(errWrongNumber("rpoplpush")),
		)
		mustDo(t, c,
			"RPOPLPUSH", "too", "many", "arguments",
			proto.Error(errWrongNumber("rpoplpush")),
		)

		s.Set("str", "string!")
		mustDo(t, c,
			"RPOPLPUSH", "str", "src",
			proto.Error(msgWrongType),
		)
		mustDo(t, c,
			"RPOPLPUSH", "src", "str",
			proto.Error(msgWrongType),
		)
	})
}

func TestRpushx(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	// Simple cases
	{
		// No key key
		must0(t, c,
			"RPUSHX", "l", "value",
		)
		assert(t, !s.Exists("l"), "l doesn't exist")

		s.Push("l", "aap", "noot")
		mustDo(t, c,
			"RPUSHX", "l", "mies",
			proto.Int(3),
		)

		s.CheckList(t, "l", "aap", "noot", "mies")
	}

	// Push more.
	{
		must1(t, c,
			"LPUSH", "l2", "aap1",
		)
		mustDo(t, c,
			"RPUSHX", "l2", "aap2", "noot2", "mies2",
			proto.Int(4),
		)

		mustDo(t, c,
			"LRANGE", "l2", "0", "0",
			proto.Strings("aap1"),
		)

		mustDo(t, c,
			"LRANGE", "l2", "-1", "-1",
			proto.Strings("mies2"),
		)
	}

	t.Run("errors", func(t *testing.T) {
		s.Push("src", "aap", "noot", "mies")
		mustDo(t, c,
			"RPUSHX",
			proto.Error(errWrongNumber("rpushx")),
		)
		mustDo(t, c,
			"RPUSHX", "l",
			proto.Error(errWrongNumber("rpushx")),
		)
		s.Set("str", "string!")
		mustDo(t, c,
			"RPUSHX", "str", "value",
			proto.Error(msgWrongType),
		)
	})
}

func TestBrpop(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	// Simple cases
	{
		s.Push("ll", "aap", "noot", "mies")
		mustDo(t, c,
			"BRPOP", "ll", "1",
			proto.Strings("ll", "mies"),
		)
	}

	t.Run("errors", func(t *testing.T) {
		mustDo(t, c,
			"BRPOP",
			proto.Error(errWrongNumber("brpop")),
		)
		mustDo(t, c,
			"BRPOP", "key",
			proto.Error(errWrongNumber("brpop")),
		)
		mustDo(t, c,
			"BRPOP", "key", "-1",
			proto.Error("ERR timeout is negative"),
		)
		mustDo(t, c,
			"BRPOP", "key", "inf",
			proto.Error("ERR timeout is negative"),
		)
	})
}

func TestBrpopSimple(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	got := goStrings(t, s, "BRPOP", "mylist", "0")
	time.Sleep(30 * time.Millisecond)

	mustDo(t, c,
		"RPUSH", "mylist", "e1", "e2", "e3",
		proto.Int(3),
	)

	select {
	case have := <-got:
		equals(t, proto.Strings("mylist", "e3"), have)
	case <-time.After(500 * time.Millisecond):
		t.Error("BRPOP took too long")
	}
}

func TestBrpopMulti(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	got := goStrings(t, s, "BRPOP", "l1", "l2", "l3", "0")
	must1(t, c, "RPUSH", "l0", "e01")
	must1(t, c, "RPUSH", "l2", "e21")
	must1(t, c, "RPUSH", "l3", "e31")

	select {
	case have := <-got:
		equals(t, proto.Strings("l2", "e21"), have)
	case <-time.After(500 * time.Millisecond):
		t.Error("BRPOP took too long")
	}

	got = goStrings(t, s, "BRPOP", "l1", "l2", "l3", "0")
	select {
	case have := <-got:
		equals(t, proto.Strings("l3", "e31"), have)
	case <-time.After(500 * time.Millisecond):
		t.Error("BRPOP took too long")
	}
}

func TestBrpopTimeout(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	got := goStrings(t, s, "BRPOP", "l1", "0.1")
	select {
	case have := <-got:
		equals(t, proto.NilList, have)
	case <-time.After(200 * time.Millisecond):
		t.Error("BRPOP took too long")
	}
}

func TestBrpopTx(t *testing.T) {
	// BRPOP in a transaction behaves as if the timeout triggers right away
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	{
		mustOK(t, c,
			"MULTI",
		)
		mustDo(t, c,
			"BRPOP", "l1", "3",
			proto.Inline("QUEUED"),
		)
		mustDo(t, c,
			"SET", "foo", "bar",
			proto.Inline("QUEUED"),
		)

		mustDo(t, c,
			"EXEC",
			proto.Array(
				proto.NilList,
				proto.Inline("OK"),
			),
		)
	}

	// Now set something
	s.Push("l1", "e1")
	{
		mustOK(t, c,
			"MULTI",
		)
		mustDo(t, c,
			"BRPOP", "l1", "3",
			proto.Inline("QUEUED"),
		)
		mustDo(t, c,
			"SET", "foo", "bar",
			proto.Inline("QUEUED"),
		)

		mustDo(t, c,
			"EXEC",
			proto.Array(
				proto.Strings("l1", "e1"),
				proto.Inline("OK"),
			),
		)
	}
}

func TestBlpop(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	t.Run("basic", func(t *testing.T) {
		s.Push("ll", "aap", "noot", "mies")
		mustDo(t, c,
			"BLPOP", "ll", "1",
			proto.Strings("ll", "aap"),
		)
	})

	t.Run("errors", func(t *testing.T) {
		mustDo(t, c,
			"BLPOP",
			proto.Error(errWrongNumber("blpop")),
		)
		mustDo(t, c,
			"BLPOP", "key",
			proto.Error(errWrongNumber("blpop")),
		)
		mustDo(t, c,
			"BLPOP", "key", "-1",
			proto.Error("ERR timeout is negative"),
		)
		mustDo(t, c,
			"BLPOP", "key", "inf",
			proto.Error("ERR timeout is negative"),
		)
	})
}

func TestBlpopResourceCleanup(t *testing.T) {
	s, err := Run()
	ok(t, err)
	c, err := proto.Dial(s.Addr())
	ok(t, err)

	// Let's say a client issued BLPOP and then the client was closed
	go func() {
		_, err := c.Do("BLPOP", "key", "0")
		assert(t, strings.Contains(err.Error(), "use of closed network connection"), "got a network error")
	}()

	time.Sleep(50 * time.Millisecond)

	c.Close()
	s.Close() // expect BLPOP to stop blocking
}

func TestBrpoplpush(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	// Simple cases
	{
		s.Push("l1", "aap", "noot", "mies")
		mustDo(t, c,
			"BRPOPLPUSH", "l1", "l2", "1",
			proto.String("mies"),
		)

		lv, err := s.List("l2")
		ok(t, err)
		equals(t, []string{"mies"}, lv)
	}

	t.Run("errors", func(t *testing.T) {
		mustDo(t, c,
			"BRPOPLPUSH",
			proto.Error(errWrongNumber("brpoplpush")),
		)
		mustDo(t, c,
			"BRPOPLPUSH", "key",
			proto.Error(errWrongNumber("brpoplpush")),
		)
		mustDo(t, c,
			"BRPOPLPUSH", "key", "bar",
			proto.Error(errWrongNumber("brpoplpush")),
		)
		mustDo(t, c,
			"BRPOPLPUSH", "key", "foo", "-1",
			proto.Error("ERR timeout is negative"),
		)
		mustDo(t, c,
			"BRPOPLPUSH", "key", "foo", "inf",
			proto.Error("ERR timeout is negative"),
		)
		mustDo(t, c,
			"BRPOPLPUSH", "key", "foo", "1", "baz",
			proto.Error(errWrongNumber("brpoplpush")),
		)
	})
}

func TestBrpoplpushSimple(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	got := goStrings(t, s, "BRPOPLPUSH", "from", "to", "1")
	time.Sleep(30 * time.Millisecond)

	mustDo(t, c,
		"RPUSH", "from", "e1", "e2", "e3",
		proto.Int(3),
	)

	select {
	case have := <-got:
		equals(t, proto.String("e3"), have)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("BRPOP took too long")
	}

	lv, err := s.List("from")
	ok(t, err)
	equals(t, []string{"e1", "e2"}, lv)
	lv, err = s.List("to")
	ok(t, err)
	equals(t, []string{"e3"}, lv)
}

func TestBrpoplpushTimeout(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()

	got := goStrings(t, s, "BRPOPLPUSH", "l1", "l2", "0.1")
	select {
	case have := <-got:
		equals(t, proto.NilList, have)
	case <-time.After(200 * time.Millisecond):
		t.Error("BRPOPLPUSH took too long")
	}
}

func TestLmove(t *testing.T) {
	s, err := Run()
	ok(t, err)
	defer s.Close()
	c, err := proto.Dial(s.Addr())
	ok(t, err)
	defer c.Close()

	s.Push("src", "LR", "LL", "RR", "RL")
	s.Push("dst", "m1", "m2", "m3")
	// RIGHT LEFT
	{
		mustDo(t, c,
			"LMOVE", "src", "dst", "RIGHT", "LEFT",
			proto.String("RL"),
		)
		s.CheckList(t, "src", "LR", "LL", "RR")
		s.CheckList(t, "dst", "RL", "m1", "m2", "m3")
	}
	// LEFT RIGHT
	{
		mustDo(t, c,
			"LMOVE", "src", "dst", "LEFT", "RIGHT",
			proto.String("LR"),
		)
		s.CheckList(t, "src", "LL", "RR")
		s.CheckList(t, "dst", "RL", "m1", "m2", "m3", "LR")
	}
	// RIGHT RIGHT
	{
		mustDo(t, c,
			"LMOVE", "src", "dst", "RIGHT", "RIGHT",
			proto.String("RR"),
		)
		s.CheckList(t, "src", "LL")
		s.CheckList(t, "dst", "RL", "m1", "m2", "m3", "LR", "RR")
	}
	// LEFT LEFT
	{
		mustDo(t, c,
			"LMOVE", "src", "dst", "LEFT", "LEFT",
			proto.String("LL"),
		)
		assert(t, !s.Exists("src"), "src exists")
		s.CheckList(t, "dst", "LL", "RL", "m1", "m2", "m3", "LR", "RR")
	}

	// Non existing lists
	{
		s.Push("ll", "aap", "noot", "mies")

		mustDo(t, c,
			"LMOVE", "ll", "nosuch", "RIGHT", "LEFT",
			proto.String("mies"),
		)
		assert(t, s.Exists("nosuch"), "nosuch exists")
		s.CheckList(t, "ll", "aap", "noot")
		s.CheckList(t, "nosuch", "mies")

		mustNil(t, c,
			"LMOVE", "nosuch2", "ll", "RIGHT", "LEFT",
		)
	}

	// Cycle
	{
		s.Push("cycle", "aap", "noot", "mies")

		mustDo(t, c,
			"LMOVE", "cycle", "cycle", "RIGHT", "LEFT",
			proto.String("mies"),
		)
		s.CheckList(t, "cycle", "mies", "aap", "noot")

		mustDo(t, c,
			"LMOVE", "cycle", "cycle", "LEFT", "RIGHT",
			proto.String("mies"),
		)
		s.CheckList(t, "cycle", "aap", "noot", "mies")
	}

	// Error cases
	t.Run("errors", func(t *testing.T) {
		s.Push("src", "aap", "noot", "mies")
		s.Push("dst", "aap", "noot", "mies")
		mustDo(t, c,
			"LMOVE",
			proto.Error(errWrongNumber("lmove")),
		)
		mustDo(t, c,
			"LMOVE", "l",
			proto.Error(errWrongNumber("lmove")),
		)
		mustDo(t, c,
			"LMOVE", "l", "l",
			proto.Error(errWrongNumber("lmove")),
		)
		mustDo(t, c,
			"LMOVE", "l", "l", "l",
			proto.Error(errWrongNumber("lmove")),
		)
		mustDo(t, c,
			"LMOVE", "too", "many", "many", "many", "arguments",
			proto.Error(errWrongNumber("lmove")),
		)

		s.Set("str", "string!")
		mustDo(t, c,
			"LMOVE", "str", "src", "left", "right",
			proto.Error(msgWrongType),
		)
		mustDo(t, c,
			"LMOVE", "src", "str", "left", "right",
			proto.Error(msgWrongType),
		)

		mustDo(t, c,
			"LMOVE", "src", "dst", "no", "good",
			proto.Error("ERR syntax error"),
		)
		mustDo(t, c,
			"LMOVE", "src", "dst", "invalid", "right",
			proto.Error("ERR syntax error"),
		)
		mustDo(t, c,
			"LMOVE", "src", "dst", "left", "invalid",
			proto.Error("ERR syntax error"),
		)
	})
}
