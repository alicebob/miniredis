// +build int

package main

// Sorted Set keys.

import (
	"testing"
)

func TestSortedSet(t *testing.T) {
	testRaw(t, func(c *client) {
		c.Do("ZADD", "z", "1", "aap", "2", "noot", "3", "mies")
		c.Do("ZADD", "z", "1", "vuur", "4", "noot")
		c.Do("TYPE", "z")
		c.Do("EXISTS", "z")
		c.Do("ZCARD", "z")

		c.Do("ZRANK", "z", "aap")
		c.Do("ZRANK", "z", "noot")
		c.Do("ZRANK", "z", "mies")
		c.Do("ZRANK", "z", "vuur")
		c.Do("ZRANK", "z", "nosuch")
		c.Do("ZRANK", "nosuch", "nosuch")
		c.Do("ZREVRANK", "z", "aap")
		c.Do("ZREVRANK", "z", "noot")
		c.Do("ZREVRANK", "z", "mies")
		c.Do("ZREVRANK", "z", "vuur")
		c.Do("ZREVRANK", "z", "nosuch")
		c.Do("ZREVRANK", "nosuch", "nosuch")

		c.Do("ZADD", "zi", "inf", "aap", "-inf", "noot", "+inf", "mies")
		c.Do("ZRANK", "zi", "noot")

		// Double key
		c.Do("ZADD", "zz", "1", "aap", "2", "aap")
		c.Do("ZCARD", "zz")

		c.Do("ZPOPMAX", "zz", "2")
		c.Do("ZPOPMAX", "zz")
		c.Do("ZPOPMAX", "zz", "-100")
		c.Do("ZPOPMAX", "nosuch", "1")
		c.Do("ZPOPMAX", "zz", "100")

		c.Do("ZPOPMIN", "zz", "2")
		c.Do("ZPOPMIN", "zz")
		c.Do("ZPOPMIN", "zz", "-100")
		c.Do("ZPOPMIN", "nosuch", "1")
		c.Do("ZPOPMIN", "zz", "100")

		// failure cases
		c.Do("SET", "str", "I am a string")
		c.Do("ZADD")
		c.Do("ZADD", "s")
		c.Do("ZADD", "s", "1")
		c.Do("ZADD", "s", "1", "aap", "1")
		c.Do("ZADD", "s", "nofloat", "aap")
		c.Do("ZADD", "str", "1", "aap")
		c.Do("ZCARD")
		c.Do("ZCARD", "too", "many")
		c.Do("ZCARD", "str")
		c.Do("ZRANK")
		c.Do("ZRANK", "key")
		c.Do("ZRANK", "key", "too", "many")
		c.Do("ZRANK", "str", "member")
		c.Do("ZREVRANK")
		c.Do("ZREVRANK", "key")
		c.Do("ZPOPMAX")
		c.Do("ZPOPMAX", "set", "noint")
		c.Do("ZPOPMAX", "set", "1", "toomany")
		c.Do("ZPOPMIN")
		c.Do("ZPOPMIN", "set", "noint")
		c.Do("ZPOPMIN", "set", "1", "toomany")

		c.Do("RENAME", "z", "z2")
		c.Do("EXISTS", "z")
		c.Do("EXISTS", "z2")
		c.Do("MOVE", "z2", "3")
		c.Do("EXISTS", "z2")
		c.Do("SELECT", "3")
		c.Do("EXISTS", "z2")
		c.Do("DEL", "z2")
		c.Do("EXISTS", "z2")
	})

	testRaw(t, func(c *client) {
		c.Do("ZADD", "z", "0", "new\nline\n")
		c.Do("ZADD", "z", "0", "line")
		c.Do("ZADD", "z", "0", "another\nnew\nline\n")
		c.Do("ZSCAN", "z", "0", "MATCH", "*")
		c.Do("ZRANGEBYLEX", "z", "[a", "[z")
		c.Do("ZRANGE", "z", "0", "-1", "WITHSCORES")
	})
}

func TestSortedSetAdd(t *testing.T) {
	testRaw(t, func(c *client) {
		c.Do("ZADD", "z",
			"1", "aap",
			"2", "noot",
		)
		c.Do("ZADD", "z", "NX",
			"1.1", "aap",
			"3", "mies",
		)
		c.Do("ZADD", "z", "XX",
			"1.2", "aap",
			"4", "vuur",
		)
		c.Do("ZADD", "z", "CH",
			"1.2", "aap",
			"4.1", "vuur",
			"5", "roos",
		)
		c.Do("ZADD", "z", "CH", "XX",
			"1.2", "aap",
			"4.2", "vuur",
			"5", "roos",
			"5", "zand",
		)
		c.Do("ZADD", "z", "XX", "XX", "XX", "XX",
			"1.2", "aap",
		)
		c.Do("ZADD", "z", "NX", "NX", "NX", "NX",
			"1.2", "aap",
		)
		c.Do("ZADD", "z", "XX", "NX", "1.1", "foo")
		c.Do("ZADD", "z", "XX")
		c.Do("ZADD", "z", "NX")
		c.Do("ZADD", "z", "CH")
		c.Do("ZADD", "z", "??")
		c.Do("ZADD", "z", "1.2", "aap", "XX")
		c.Do("ZADD", "z", "1.2", "aap", "CH")
		c.Do("ZADD", "z")
	})
	testRaw(t, func(c *client) {
		c.Do("ZADD", "z", "INCR", "1", "aap")
		c.Do("ZADD", "z", "INCR", "1", "aap")
		c.Do("ZADD", "z", "INCR", "1", "aap")
		c.Do("ZADD", "z", "INCR", "-12", "aap")
		c.Do("ZADD", "z", "INCR", "INCR", "-12", "aap")
		c.Do("ZADD", "z", "CH", "INCR", "-12", "aap") // 'CH' is ignored
		c.Do("ZADD", "z", "INCR", "CH", "-12", "aap") // 'CH' is ignored
		c.Do("ZADD", "z", "INCR", "NX", "12", "aap")
		c.Do("ZADD", "z", "INCR", "XX", "12", "aap")
		c.Do("ZADD", "q", "INCR", "NX", "12", "aap")
		c.Do("ZADD", "q", "INCR", "XX", "12", "aap")

		c.Do("ZADD", "z", "INCR", "1", "aap", "2", "tiger")
		c.Do("ZADD", "z", "INCR", "-12")
		c.Do("ZADD", "z", "INCR", "-12", "aap", "NX")
	})
}

func TestSortedSetRange(t *testing.T) {
	testRaw(t, func(c *client) {
		c.Do("ZADD", "z",
			"1", "aap",
			"2", "noot",
			"3", "mies",
			"2", "nootagain",
			"3", "miesagain",
			"+Inf", "the stars",
			"+Inf", "more stars",
			"-Inf", "big bang",
		)
		c.Do("ZRANGE", "z", "0", "-1")
		c.Do("ZRANGE", "z", "0", "-1", "WITHSCORES")
		c.Do("ZRANGE", "z", "0", "-1", "WiThScOrEs")
		c.Do("ZRANGE", "z", "0", "-2")
		c.Do("ZRANGE", "z", "0", "-1000")
		c.Do("ZRANGE", "z", "2", "-2")
		c.Do("ZRANGE", "z", "400", "-1")
		c.Do("ZRANGE", "z", "300", "-110")
		c.Do("ZREVRANGE", "z", "0", "-1")
		c.Do("ZREVRANGE", "z", "0", "-1", "WITHSCORES")
		c.Do("ZREVRANGE", "z", "0", "-1", "WiThScOrEs")
		c.Do("ZREVRANGE", "z", "0", "-2")
		c.Do("ZREVRANGE", "z", "0", "-1000")
		c.Do("ZREVRANGE", "z", "2", "-2")
		c.Do("ZREVRANGE", "z", "400", "-1")
		c.Do("ZREVRANGE", "z", "300", "-110")

		c.Do("ZADD", "zz",
			"0", "aap",
			"0", "Aap",
			"0", "AAP",
			"0", "aAP",
			"0", "aAp",
		)
		c.Do("ZRANGE", "zz", "0", "-1")

		// failure cases
		c.Do("ZRANGE")
		c.Do("ZRANGE", "foo")
		c.Do("ZRANGE", "foo", "1")
		c.Do("ZRANGE", "foo", "2", "3", "toomany")
		c.Do("ZRANGE", "foo", "2", "3", "WITHSCORES", "toomany")
		c.Do("ZRANGE", "foo", "noint", "3")
		c.Do("ZRANGE", "foo", "2", "noint")
		c.Do("SET", "str", "I am a string")
		c.Do("ZRANGE", "str", "300", "-110")

		c.Do("ZREVRANGE")
		c.Do("ZREVRANGE", "str", "300", "-110")
	})
}

func TestSortedSetRem(t *testing.T) {
	testRaw(t, func(c *client) {
		c.Do("ZADD", "z",
			"1", "aap",
			"2", "noot",
			"3", "mies",
			"2", "nootagain",
			"3", "miesagain",
			"+Inf", "the stars",
			"+Inf", "more stars",
			"-Inf", "big bang",
		)
		c.Do("ZREM", "z", "nosuch")
		c.Do("ZREM", "z", "mies", "nootagain")
		c.Do("ZRANGE", "z", "0", "-1")

		// failure cases
		c.Do("ZREM")
		c.Do("ZREM", "foo")
		c.Do("SET", "str", "I am a string")
		c.Do("ZREM", "str", "member")
	})
}

func TestSortedSetRemRangeByLex(t *testing.T) {
	testRaw(t, func(c *client) {
		c.Do("ZADD", "z",
			"12", "zero kelvin",
			"12", "minusfour",
			"12", "one",
			"12", "oneone",
			"12", "two",
			"12", "zwei",
			"12", "three",
			"12", "drei",
			"12", "inf",
		)
		c.Do("ZRANGEBYLEX", "z", "-", "+")
		c.Do("ZREMRANGEBYLEX", "z", "[o", "(t")
		c.Do("ZRANGEBYLEX", "z", "-", "+")
		c.Do("ZREMRANGEBYLEX", "z", "-", "+")
		c.Do("ZRANGEBYLEX", "z", "-", "+")

		// failure cases
		c.Do("ZREMRANGEBYLEX")
		c.Do("ZREMRANGEBYLEX", "key")
		c.Do("ZREMRANGEBYLEX", "key", "[a")
		c.Do("ZREMRANGEBYLEX", "key", "[a", "[b", "c")
		c.Do("ZREMRANGEBYLEX", "key", "!a", "[b")
		c.Do("SET", "str", "I am a string")
		c.Do("ZREMRANGEBYLEX", "str", "[a", "[b")
	})
}

func TestSortedSetRemRangeByRank(t *testing.T) {
	testRaw(t, func(c *client) {
		c.Do("ZADD", "z",
			"12", "zero kelvin",
			"12", "minusfour",
			"12", "one",
			"12", "oneone",
			"12", "two",
			"12", "zwei",
			"12", "three",
			"12", "drei",
			"12", "inf",
		)
		c.Do("ZREMRANGEBYRANK", "z", "-2", "-1")
		c.Do("ZRANGE", "z", "0", "-1")
		c.Do("ZRANGEBYSCORE", "z", "-inf", "inf")
		c.Do("ZREMRANGEBYRANK", "z", "-2", "-1")
		c.Do("ZRANGE", "z", "0", "-1")
		c.Do("ZREMRANGEBYRANK", "z", "0", "-1")
		c.Do("EXISTS", "z")

		c.Do("ZREMRANGEBYRANK", "nosuch", "-2", "-1")

		// failure cases
		c.Do("ZREMRANGEBYRANK")
		c.Do("ZREMRANGEBYRANK", "key")
		c.Do("ZREMRANGEBYRANK", "key", "0")
		c.Do("ZREMRANGEBYRANK", "key", "noint", "-1")
		c.Do("ZREMRANGEBYRANK", "key", "0", "noint")
		c.Do("ZREMRANGEBYRANK", "key", "0", "1", "too many")
		c.Do("SET", "str", "I am a string")
		c.Do("ZREMRANGEBYRANK", "str", "0", "-1")
	})
}

func TestSortedSetRemRangeByScore(t *testing.T) {
	testRaw(t, func(c *client) {
		c.Do("ZADD", "z",
			"1", "aap",
			"2", "noot",
			"3", "mies",
			"2", "nootagain",
			"3", "miesagain",
			"+Inf", "the stars",
			"+Inf", "more stars",
			"-Inf", "big bang",
		)
		c.Do("ZREMRANGEBYSCORE", "z", "-inf", "(2")
		c.Do("ZRANGE", "z", "0", "-1")
		c.Do("ZREMRANGEBYSCORE", "z", "(1000", "(2000")
		c.Do("ZRANGE", "z", "0", "-1")
		c.Do("ZREMRANGEBYSCORE", "z", "-inf", "+inf")
		c.Do("EXISTS", "z")

		c.Do("ZREMRANGEBYSCORE", "nosuch", "-inf", "inf")

		// failure cases
		c.Do("ZREMRANGEBYSCORE")
		c.Do("ZREMRANGEBYSCORE", "key")
		c.Do("ZREMRANGEBYSCORE", "key", "0")
		c.Do("ZREMRANGEBYSCORE", "key", "noint", "-1")
		c.Do("ZREMRANGEBYSCORE", "key", "0", "noint")
		c.Do("ZREMRANGEBYSCORE", "key", "0", "1", "too many")
		c.Do("SET", "str", "I am a string")
		c.Do("ZREMRANGEBYSCORE", "str", "0", "-1")
	})
}

func TestSortedSetScore(t *testing.T) {
	testRaw(t, func(c *client) {
		c.Do("ZADD", "z",
			"1", "aap",
			"2", "noot",
			"3", "mies",
			"2", "nootagain",
			"3", "miesagain",
			"+Inf", "the stars",
		)
		c.Do("ZSCORE", "z", "mies")
		c.Do("ZSCORE", "z", "the stars")
		c.Do("ZSCORE", "z", "nosuch")
		c.Do("ZSCORE", "nosuch", "nosuch")

		// failure cases
		c.Do("ZSCORE")
		c.Do("ZSCORE", "foo")
		c.Do("ZSCORE", "foo", "too", "many")
		c.Do("SET", "str", "I am a string")
		c.Do("ZSCORE", "str", "member")
	})
}

func TestSortedSetRangeByScore(t *testing.T) {
	testRaw(t, func(c *client) {
		c.Do("ZADD", "z",
			"1", "aap",
			"2", "noot",
			"3", "mies",
			"2", "nootagain",
			"3", "miesagain",
			"+Inf", "the stars",
			"+Inf", "more stars",
			"-Inf", "big bang",
		)
		c.Do("ZRANGEBYSCORE", "z", "-inf", "inf")
		c.Do("ZRANGEBYSCORE", "z", "-inf", "inf", "LIMIT", "1", "2")
		c.Do("ZRANGEBYSCORE", "z", "-inf", "inf", "LIMIT", "-1", "2")
		c.Do("ZRANGEBYSCORE", "z", "-inf", "inf", "LIMIT", "1", "-2")
		c.Do("ZREVRANGEBYSCORE", "z", "inf", "-inf")
		c.Do("ZREVRANGEBYSCORE", "z", "inf", "-inf", "LIMIT", "1", "2")
		c.Do("ZREVRANGEBYSCORE", "z", "inf", "-inf", "LIMIT", "-1", "2")
		c.Do("ZREVRANGEBYSCORE", "z", "inf", "-inf", "LIMIT", "1", "-2")
		c.Do("ZRANGEBYSCORE", "z", "-inf", "inf", "WITHSCORES")
		c.Do("ZRANGEBYSCORE", "z", "-inf", "inf", "WiThScOrEs")
		c.Do("ZREVRANGEBYSCORE", "z", "-inf", "inf", "WITHSCORES", "LIMIT", "1", "2")
		c.Do("ZRANGEBYSCORE", "z", "0", "3")
		c.Do("ZRANGEBYSCORE", "z", "0", "inf")
		c.Do("ZRANGEBYSCORE", "z", "(1", "3")
		c.Do("ZRANGEBYSCORE", "z", "(1", "(3")
		c.Do("ZRANGEBYSCORE", "z", "1", "(3")
		c.Do("ZRANGEBYSCORE", "z", "1", "(3", "LIMIT", "0", "2")
		c.Do("ZRANGEBYSCORE", "foo", "2", "3", "LIMIT", "1", "2", "WITHSCORES")
		c.Do("ZCOUNT", "z", "-inf", "inf")
		c.Do("ZCOUNT", "z", "0", "3")
		c.Do("ZCOUNT", "z", "0", "inf")
		c.Do("ZCOUNT", "z", "(2", "inf")

		// Bunch of limit edge cases
		c.Do("ZRANGEBYSCORE", "z", "-inf", "inf", "LIMIT", "0", "7")
		c.Do("ZRANGEBYSCORE", "z", "-inf", "inf", "LIMIT", "0", "8")
		c.Do("ZRANGEBYSCORE", "z", "-inf", "inf", "LIMIT", "0", "9")
		c.Do("ZRANGEBYSCORE", "z", "-inf", "inf", "LIMIT", "7", "0")
		c.Do("ZRANGEBYSCORE", "z", "-inf", "inf", "LIMIT", "7", "1")
		c.Do("ZRANGEBYSCORE", "z", "-inf", "inf", "LIMIT", "7", "2")
		c.Do("ZRANGEBYSCORE", "z", "-inf", "inf", "LIMIT", "8", "0")
		c.Do("ZRANGEBYSCORE", "z", "-inf", "inf", "LIMIT", "8", "1")
		c.Do("ZRANGEBYSCORE", "z", "-inf", "inf", "LIMIT", "8", "2")
		c.Do("ZRANGEBYSCORE", "z", "-inf", "inf", "LIMIT", "9", "2")
		c.Do("ZRANGEBYSCORE", "z", "-inf", "inf", "LIMIT", "-1", "2")
		c.Do("ZRANGEBYSCORE", "z", "-inf", "inf", "LIMIT", "-1", "-1")

		// failure cases
		c.Do("ZRANGEBYSCORE")
		c.Do("ZRANGEBYSCORE", "foo")
		c.Do("ZRANGEBYSCORE", "foo", "1")
		c.Do("ZRANGEBYSCORE", "foo", "2", "3", "toomany")
		c.Do("ZRANGEBYSCORE", "foo", "2", "3", "WITHSCORES", "toomany")
		c.Do("ZRANGEBYSCORE", "foo", "2", "3", "LIMIT", "noint", "1")
		c.Do("ZRANGEBYSCORE", "foo", "2", "3", "LIMIT", "1", "noint")
		c.Do("ZREVRANGEBYSCORE", "z", "-inf", "inf", "WITHSCORES", "LIMIT", "1", "-2", "toomany")
		c.Do("ZRANGEBYSCORE", "foo", "noint", "3")
		c.Do("ZRANGEBYSCORE", "foo", "[4", "3")
		c.Do("ZRANGEBYSCORE", "foo", "2", "noint")
		c.Do("ZRANGEBYSCORE", "foo", "4", "[3")
		c.Do("SET", "str", "I am a string")
		c.Do("ZRANGEBYSCORE", "str", "300", "-110")

		c.Do("ZREVRANGEBYSCORE")
		c.Do("ZREVRANGEBYSCORE", "foo", "[4", "3")
		c.Do("ZREVRANGEBYSCORE", "str", "300", "-110")

		c.Do("ZCOUNT")
		c.Do("ZCOUNT", "foo", "[4", "3")
		c.Do("ZCOUNT", "str", "300", "-110")
	})

	// Issue #10
	testRaw(t, func(c *client) {
		c.Do("ZADD", "key", "3.3", "element")
		c.Do("ZRANGEBYSCORE", "key", "3.3", "3.3")
		c.Do("ZRANGEBYSCORE", "key", "4.3", "4.3")
		c.Do("ZREVRANGEBYSCORE", "key", "3.3", "3.3")
		c.Do("ZREVRANGEBYSCORE", "key", "4.3", "4.3")
	})
}

func TestSortedSetRangeByLex(t *testing.T) {
	testRaw(t, func(c *client) {
		c.Do("ZADD", "z",
			"12", "zero kelvin",
			"12", "minusfour",
			"12", "one",
			"12", "oneone",
			"12", "two",
			"12", "zwei",
			"12", "three",
			"12", "drei",
			"12", "inf",
		)
		c.Do("ZRANGEBYLEX", "z", "-", "+")
		c.Do("ZREVRANGEBYLEX", "z", "+", "-")
		c.Do("ZLEXCOUNT", "z", "-", "+")
		c.Do("ZRANGEBYLEX", "z", "[o", "[three")
		c.Do("ZREVRANGEBYLEX", "z", "[three", "[o")
		c.Do("ZLEXCOUNT", "z", "[o", "[three")
		c.Do("ZRANGEBYLEX", "z", "(o", "(z")
		c.Do("ZREVRANGEBYLEX", "z", "(z", "(o")
		c.Do("ZLEXCOUNT", "z", "(o", "(z")
		c.Do("ZRANGEBYLEX", "z", "+", "(z")
		c.Do("ZREVRANGEBYLEX", "z", "(z", "+")
		c.Do("ZRANGEBYLEX", "z", "(a", "-")
		c.Do("ZREVRANGEBYLEX", "z", "-", "(a")
		c.Do("ZRANGEBYLEX", "z", "(z", "(a")
		c.Do("ZREVRANGEBYLEX", "z", "(a", "(z")
		c.Do("ZRANGEBYLEX", "nosuch", "-", "+")
		c.Do("ZREVRANGEBYLEX", "nosuch", "+", "-")
		c.Do("ZLEXCOUNT", "nosuch", "-", "+")
		c.Do("ZRANGEBYLEX", "z", "-", "+", "LIMIT", "1", "2")
		c.Do("ZREVRANGEBYLEX", "z", "+", "-", "LIMIT", "1", "2")
		c.Do("ZRANGEBYLEX", "z", "-", "+", "LIMIT", "-1", "2")
		c.Do("ZREVRANGEBYLEX", "z", "+", "-", "LIMIT", "-1", "2")
		c.Do("ZRANGEBYLEX", "z", "-", "+", "LIMIT", "1", "-2")
		c.Do("ZREVRANGEBYLEX", "z", "+", "-", "LIMIT", "1", "-2")

		c.Do("ZADD", "z", "12", "z")
		c.Do("ZADD", "z", "12", "zz")
		c.Do("ZADD", "z", "12", "zzz")
		c.Do("ZADD", "z", "12", "zzzz")
		c.Do("ZRANGEBYLEX", "z", "[z", "+")
		c.Do("ZREVRANGEBYLEX", "z", "+", "[z")
		c.Do("ZRANGEBYLEX", "z", "(z", "+")
		c.Do("ZREVRANGEBYLEX", "z", "+", "(z")
		c.Do("ZLEXCOUNT", "z", "(z", "+")

		// failure cases
		c.Do("ZRANGEBYLEX")
		c.Do("ZREVRANGEBYLEX")
		c.Do("ZRANGEBYLEX", "key")
		c.Do("ZRANGEBYLEX", "key", "[a")
		c.Do("ZRANGEBYLEX", "key", "[a", "[b", "c")
		c.Do("ZRANGEBYLEX", "key", "!a", "[b")
		c.Do("ZRANGEBYLEX", "key", "[a", "!b")
		c.Do("ZRANGEBYLEX", "key", "[a", "b]")
		c.Error("not valid string range item", "ZRANGEBYLEX", "key", "[a", "")
		c.Error("not valid string range item", "ZRANGEBYLEX", "key", "", "[b")
		c.Do("ZRANGEBYLEX", "key", "[a", "[b", "LIMIT")
		c.Do("ZRANGEBYLEX", "key", "[a", "[b", "LIMIT", "1")
		c.Do("ZRANGEBYLEX", "key", "[a", "[b", "LIMIT", "a", "1")
		c.Do("ZRANGEBYLEX", "key", "[a", "[b", "LIMIT", "1", "a")
		c.Do("ZRANGEBYLEX", "key", "[a", "[b", "LIMIT", "1", "1", "toomany")
		c.Do("SET", "str", "I am a string")
		c.Do("ZRANGEBYLEX", "str", "[a", "[b")

		c.Do("ZLEXCOUNT")
		c.Do("ZLEXCOUNT", "key")
		c.Do("ZLEXCOUNT", "key", "[a")
		c.Do("ZLEXCOUNT", "key", "[a", "[b", "c")
		c.Do("ZLEXCOUNT", "key", "!a", "[b")
		c.Do("ZLEXCOUNT", "str", "[a", "[b")
	})

	testRaw(t, func(c *client) {
		c.Do("ZADD", "idx", "0", "ccc")
		c.Do("ZRANGEBYLEX", "idx", "[d", "[e")
		c.Do("ZRANGEBYLEX", "idx", "[c", "[d")
	})
}

func TestSortedSetIncyby(t *testing.T) {
	testRaw(t, func(c *client) {
		c.Do("ZINCRBY", "z", "1.0", "m")
		c.Do("ZINCRBY", "z", "1.0", "m")
		c.Do("ZINCRBY", "z", "1.0", "m")
		c.Do("ZINCRBY", "z", "2.0", "m")
		c.Do("ZINCRBY", "z", "3", "m2")
		c.Do("ZINCRBY", "z", "3", "m2")
		c.Do("ZINCRBY", "z", "3", "m2")

		// failure cases
		c.Do("ZINCRBY")
		c.Do("ZINCRBY", "key")
		c.Do("ZINCRBY", "key", "1.0")
		c.Do("ZINCRBY", "key", "nofloat", "m")
		c.Do("ZINCRBY", "key", "1.0", "too", "many")
		c.Do("SET", "str", "I am a string")
		c.Do("ZINCRBY", "str", "1.0", "member")
	})
}

func TestZscan(t *testing.T) {
	testRaw(t, func(c *client) {
		// No set yet
		c.Do("ZSCAN", "h", "0")

		c.Do("ZADD", "h", "1.0", "key1")
		c.Do("ZSCAN", "h", "0")
		c.Do("ZSCAN", "h", "0", "COUNT", "12")
		c.Do("ZSCAN", "h", "0", "cOuNt", "12")

		c.Do("ZADD", "h", "2.0", "anotherkey")
		c.Do("ZSCAN", "h", "0", "MATCH", "anoth*")
		c.Do("ZSCAN", "h", "0", "MATCH", "anoth*", "COUNT", "100")
		c.Do("ZSCAN", "h", "0", "COUNT", "100", "MATCH", "anoth*")

		// Can't really test multiple keys.
		// c.Do("SET", "key2", "value2")
		// c.Do("SCAN", "0")

		// Error cases
		c.Do("ZSCAN")
		c.Do("ZSCAN", "noint")
		c.Do("ZSCAN", "h", "0", "COUNT", "noint")
		c.Do("ZSCAN", "h", "0", "COUNT")
		c.Do("ZSCAN", "h", "0", "MATCH")
		c.Do("ZSCAN", "h", "0", "garbage")
		c.Do("ZSCAN", "h", "0", "COUNT", "12", "MATCH", "foo", "garbage")
		// c.Do("ZSCAN", "nosuch", "0", "COUNT", "garbage")
		c.Do("SET", "str", "1")
		c.Do("ZSCAN", "str", "0")
	})
}

func TestZunionstore(t *testing.T) {
	testRaw(t, func(c *client) {
		c.Do("ZADD", "h1", "1.0", "key1")
		c.Do("ZADD", "h1", "2.0", "key2")
		c.Do("ZADD", "h2", "1.0", "key1")
		c.Do("ZADD", "h2", "4.0", "key2")
		c.Do("ZUNIONSTORE", "res", "2", "h1", "h2")
		c.Do("ZRANGE", "res", "0", "-1", "WITHSCORES")

		c.Do("ZUNIONSTORE", "weighted", "2", "h1", "h2", "WEIGHTS", "2.0", "12")
		c.Do("ZRANGE", "weighted", "0", "-1", "WITHSCORES")
		c.Do("ZUNIONSTORE", "weighted2", "2", "h1", "h2", "WEIGHTS", "2", "-12")
		c.Do("ZRANGE", "weighted2", "0", "-1", "WITHSCORES")

		c.Do("ZUNIONSTORE", "amin", "2", "h1", "h2", "AGGREGATE", "min")
		c.Do("ZRANGE", "amin", "0", "-1", "WITHSCORES")
		c.Do("ZUNIONSTORE", "amax", "2", "h1", "h2", "AGGREGATE", "max")
		c.Do("ZRANGE", "amax", "0", "-1", "WITHSCORES")
		c.Do("ZUNIONSTORE", "asum", "2", "h1", "h2", "AGGREGATE", "sum")
		c.Do("ZRANGE", "asum", "0", "-1", "WITHSCORES")

		// Error cases
		c.Do("ZUNIONSTORE")
		c.Do("ZUNIONSTORE", "h")
		c.Do("ZUNIONSTORE", "h", "noint")
		c.Do("ZUNIONSTORE", "h", "0", "f")
		c.Do("ZUNIONSTORE", "h", "2", "f")
		c.Do("ZUNIONSTORE", "h", "-1", "f")
		c.Do("ZUNIONSTORE", "h", "2", "f1", "f2", "f3")
		c.Do("ZUNIONSTORE", "h", "2", "f1", "f2", "WEIGHTS")
		c.Do("ZUNIONSTORE", "h", "2", "f1", "f2", "WEIGHTS", "1")
		c.Do("ZUNIONSTORE", "h", "2", "f1", "f2", "WEIGHTS", "1", "2", "3")
		c.Do("ZUNIONSTORE", "h", "2", "f1", "f2", "WEIGHTS", "f", "2")
		c.Do("ZUNIONSTORE", "h", "2", "f1", "f2", "AGGREGATE", "foo")
		c.Do("SET", "str", "1")
		c.Do("ZUNIONSTORE", "h", "1", "str")
	})
	// overwrite
	testRaw(t, func(c *client) {
		c.Do("ZADD", "h1", "1.0", "key1")
		c.Do("ZADD", "h1", "2.0", "key2")
		c.Do("ZADD", "h2", "1.0", "key1")
		c.Do("ZADD", "h2", "4.0", "key2")
		c.Do("SET", "str", "1")
		c.Do("ZUNIONSTORE", "str", "2", "h1", "h2")
		c.Do("TYPE", "str")
		c.Do("ZUNIONSTORE", "h2", "2", "h1", "h2")
		c.Do("ZRANGE", "h2", "0", "-1", "WITHSCORES")
		c.Do("TYPE", "h1")
		c.Do("TYPE", "h2")
	})
	// not a sorted set, still fine
	testRaw(t, func(c *client) {
		c.Do("SADD", "super", "1", "2", "3")
		c.Do("SADD", "exclude", "3")
		c.Do("ZUNIONSTORE", "tmp", "2", "super", "exclude", "weights", "1", "0", "aggregate", "min")
		c.Do("ZRANGE", "tmp", "0", "-1", "withscores")
	})
}

func TestZinterstore(t *testing.T) {
	testRaw(t, func(c *client) {
		c.Do("ZADD", "h1", "1.0", "key1")
		c.Do("ZADD", "h1", "2.0", "key2")
		c.Do("ZADD", "h1", "3.0", "key3")
		c.Do("ZADD", "h2", "1.0", "key1")
		c.Do("ZADD", "h2", "4.0", "key2")
		c.Do("ZADD", "h3", "4.0", "key4")
		c.Do("ZINTERSTORE", "res", "2", "h1", "h2")
		c.Do("ZRANGE", "res", "0", "-1", "WITHSCORES")

		c.Do("ZINTERSTORE", "weighted", "2", "h1", "h2", "WEIGHTS", "2.0", "12")
		c.Do("ZRANGE", "weighted", "0", "-1", "WITHSCORES")
		c.Do("ZINTERSTORE", "weighted2", "2", "h1", "h2", "WEIGHTS", "2", "-12")
		c.Do("ZRANGE", "weighted2", "0", "-1", "WITHSCORES")

		c.Do("ZINTERSTORE", "amin", "2", "h1", "h2", "AGGREGATE", "min")
		c.Do("ZRANGE", "amin", "0", "-1", "WITHSCORES")
		c.Do("ZINTERSTORE", "amax", "2", "h1", "h2", "AGGREGATE", "max")
		c.Do("ZRANGE", "amax", "0", "-1", "WITHSCORES")
		c.Do("ZINTERSTORE", "asum", "2", "h1", "h2", "AGGREGATE", "sum")
		c.Do("ZRANGE", "asum", "0", "-1", "WITHSCORES")

		// Error cases
		c.Do("ZINTERSTORE")
		c.Do("ZINTERSTORE", "h")
		c.Do("ZINTERSTORE", "h", "noint")
		c.Do("ZINTERSTORE", "h", "0", "f")
		c.Do("ZINTERSTORE", "h", "2", "f")
		c.Do("ZINTERSTORE", "h", "-1", "f")
		c.Do("ZINTERSTORE", "h", "2", "f1", "f2", "f3")
		c.Do("ZINTERSTORE", "h", "2", "f1", "f2", "WEIGHTS")
		c.Do("ZINTERSTORE", "h", "2", "f1", "f2", "WEIGHTS", "1")
		c.Do("ZINTERSTORE", "h", "2", "f1", "f2", "WEIGHTS", "1", "2", "3")
		c.Do("ZINTERSTORE", "h", "2", "f1", "f2", "WEIGHTS", "f", "2")
		c.Do("ZINTERSTORE", "h", "2", "f1", "f2", "AGGREGATE", "foo")
		c.Do("SET", "str", "1")
		c.Do("ZINTERSTORE", "h", "1", "str")
	})
}

func TestZpopminmax(t *testing.T) {
	testRaw(t, func(c *client) {
		c.Do("ZADD", "set:zpop", "1.0", "key1")
		c.Do("ZADD", "set:zpop", "2.0", "key2")
		c.Do("ZADD", "set:zpop", "3.0", "key3")
		c.Do("ZADD", "set:zpop", "4.0", "key4")
		c.Do("ZADD", "set:zpop", "5.0", "key5")
		c.Do("ZCARD", "set:zpop")

		c.Do("ZSCORE", "set:zpop", "key1")
		c.Do("ZSCORE", "set:zpop", "key5")

		c.Do("ZPOPMIN", "set:zpop")
		c.Do("ZPOPMIN", "set:zpop", "2")
		c.Do("ZPOPMIN", "set:zpop", "100")
		c.Do("ZPOPMIN", "set:zpop", "-100")

		c.Do("ZPOPMAX", "set:zpop")
		c.Do("ZPOPMAX", "set:zpop", "2")
		c.Do("ZPOPMAX", "set:zpop", "100")
		c.Do("ZPOPMAX", "set:zpop", "-100")
		c.Do("ZPOPMAX", "nosuch", "1")

		// Wrong args
		c.Do("ZPOPMIN")
		c.Do("ZPOPMIN", "set:zpop", "h1")
		c.Do("ZPOPMIN", "set:zpop", "1", "h2")
	})
}
