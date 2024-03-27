package miniredis

import (
	"testing"

	"github.com/alicebob/miniredis/v2/proto"
)

// Test PFADD
func TestPfadd(t *testing.T) {
	s, c := runWithClient(t)

	mustDo(t, c,
		"PFADD", "h", "aap", "noot", "mies",
		proto.Int(1),
	)

	mustDo(t, c,
		"PFADD", "h", "aap", // already exists in hll => returns 0
		proto.Int(0),
	)

	mustDo(t, c,
		"TYPE", "h",
		proto.Inline("hll"),
	)

	t.Run("direct usage", func(t *testing.T) {
		added, err := s.SetAdd("s1", "aap")
		ok(t, err)
		equals(t, 1, added)

		members, err := s.Members("s1")
		ok(t, err)
		equals(t, []string{"aap"}, members)
	})

	t.Run("errors", func(t *testing.T) {
		mustOK(t, c, "SET", "str", "value")
		mustDo(t, c,
			"PFADD", "str", "hi",
			proto.Error(msgNotValidHllValue),
		)
		// Wrong argument counts
		mustDo(t, c,
			"PFADD",
			proto.Error(errWrongNumber("pfadd")),
		)
	})
}

// Test PFCOUNT
func TestPfcount(t *testing.T) {
	s, c := runWithClient(t)

	// Add 100 unique random values
	for i := 0; i < 100; i++ {
		mustDo(t, c,
			"PFADD", "h1", randomStr(10),
			proto.Int(1), // hll changes each time
		)
	}

	// Add 1 more unique value
	specificValue := randomStr(10)
	mustDo(t, c,
		"PFADD", "h1", specificValue,
		proto.Int(1), // hll changes because of new element
	)
	for i := 0; i < 50; i++ {
		mustDo(t, c,
			"PFADD", "h1", specificValue,
			proto.Int(0), // hll doesn't change because this element has already been added before
		)
	}

	mustDo(t, c,
		"PFCOUNT", "h1",
		proto.Int(101),
	)

	// Create a new hll
	mustDo(t, c,
		"PFADD", "h2", randomStr(10), randomStr(10), randomStr(10),
		proto.Int(1),
	)

	mustDo(t, c,
		"PFCOUNT", "h2",
		proto.Int(3),
	)

	// Several hlls are involved - a sum of all the counts is returned
	mustDo(t, c,
		"PFCOUNT",
		"h1", // has 101 unique values
		"h2", // has 3 unique values
		"h3", // empty key
		proto.Int(104),
	)

	// A nonexisting key
	mustDo(t, c,
		"PFCOUNT", "h9",
		proto.Int(0),
	)

	t.Run("errors", func(t *testing.T) {
		s.Set("str", "value")

		mustDo(t, c,
			"PFCOUNT",
			proto.Error(errWrongNumber("pfcount")),
		)
		mustDo(t, c,
			"PFCOUNT", "str",
			proto.Error(msgNotValidHllValue),
		)
		mustDo(t, c,
			"PFCOUNT", "h1", "str",
			proto.Error(msgNotValidHllValue),
		)
	})
}

// Test PFMERGE
func TestPfmerge(t *testing.T) {
	s, c := runWithClient(t)

	// Add 100 unique random values to h1 and 50 of these 100 to h2
	for i := 0; i < 100; i++ {
		value := randomStr(10)
		mustDo(t, c,
			"PFADD", "h1", value,
			proto.Int(1), // hll changes each time
		)
		if i%2 == 0 {
			mustDo(t, c,
				"PFADD", "h2", value,
				proto.Int(1), // hll changes each time
			)
		}
	}

	for i := 0; i < 100; i++ {
		mustDo(t, c,
			"PFADD", "h3", randomStr(10),
			proto.Int(1), // hll changes each time
		)
	}

	// Merge non-intersecting hlls
	{
		mustOK(t, c,
			"PFMERGE",
			"res1",
			"h1", // count 100
			"h3", // count 100
		)
		mustDo(t, c,
			"PFCOUNT", "res1",
			proto.Int(200),
		)
	}

	// Merge intersecting hlls
	{
		mustOK(t, c,
			"PFMERGE",
			"res2",
			"h1", // count 100
			"h2", // count 50 (all 50 are presented in h1)
		)
		mustDo(t, c,
			"PFCOUNT", "res2",
			proto.Int(100),
		)
	}

	// Merge all hlls
	{
		mustOK(t, c,
			"PFMERGE",
			"res3",
			"h1", // count 100
			"h2", // count 50 (all 50 are presented in h1)
			"h3", // count 100
			"h4", // empty key
		)
		mustDo(t, c,
			"PFCOUNT", "res3",
			proto.Int(200),
		)
	}

	t.Run("direct", func(t *testing.T) {
		commonElem := randomStr(10)
		s.PfAdd("h5", commonElem, randomStr(10), randomStr(10), randomStr(10), randomStr(10))
		s.PfAdd("h6", commonElem, randomStr(10), randomStr(10))

		sum, err := s.PfCount("h5", "h6", "h7") // h7 is empty
		ok(t, err)
		equals(t, sum, 8)

		s.PfMerge("h8", "h5", "h6")
		sum, err = s.PfCount("h8")
		ok(t, err)
		equals(t, sum, 7) // common elem is counted once
	})

	t.Run("errors", func(t *testing.T) {
		s.Set("str", "value")

		mustDo(t, c,
			"PFMERGE",
			proto.Error(errWrongNumber("pfmerge")),
		)
		mustDo(t, c,
			"PFMERGE", "h10", "str",
			proto.Error(msgNotValidHllValue),
		)
	})
}
