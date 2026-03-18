# Hash Field TTL Commands Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add HPERSIST, HTTL, HPTTL, and HSETEX wire commands plus HExpire, HPersist, HTTL, and HSetEX direct API methods to miniredis.

**Architecture:** Each wire command follows the existing pattern: register in `commandsHash()`, parse args into an opts struct, execute inside `withTx`. HTTL/HPTTL share a helper. HPERSIST/HTTL/HPTTL share a `parseHFieldsArgs` helper for the `FIELDS numfields field...` block. Direct methods follow the `Miniredis`-delegates-to-`RedisDB` pattern in `direct.go`.

**Tech Stack:** Go, miniredis internal APIs (`server.Peer`, `withTx`, `db.hashTTLs`)

**Spec:** `docs/superpowers/specs/2026-03-18-hash-field-ttl-commands-design.md`

---

## Chunk 1: HPERSIST, HTTL, HPTTL + shared FIELDS parser

### Task 1: Extract shared FIELDS parser

The `FIELDS numfields field [field ...]` block is parsed identically by HPERSIST, HTTL, and HPTTL. Extract a reusable parser.

**Files:**
- Modify: `cmd_hash.go` (add `parseFieldsArgs` function)

- [ ] **Step 1: Add `parseFieldsArgs` helper**

Add this function at the end of `cmd_hash.go`, before the `abs` function:

```go
// parseFieldsArgs parses "FIELDS numfields field [field ...]" from args.
// Returns the parsed field names, or an error string.
func parseFieldsArgs(args []string) ([]string, string) {
	if len(args) < 2 {
		return nil, fmt.Sprintf(msgMandatoryArgument, "FIELDS")
	}

	if strings.ToLower(args[0]) != "fields" {
		return nil, fmt.Sprintf(msgMandatoryArgument, "FIELDS")
	}

	var numFields int
	if err := optIntSimple(args[1], &numFields); err != nil {
		return nil, msgNumFieldsInvalid
	}
	if numFields <= 0 {
		return nil, msgNumFieldsInvalid
	}

	if len(args) < 2+numFields {
		return nil, msgNumFieldsParameter
	}

	// Reject trailing args after the declared fields
	if len(args) > 2+numFields {
		return nil, msgNumFieldsParameter
	}

	return append([]string{}, args[2:2+numFields]...), ""
}
```

- [ ] **Step 2: Run existing HEXPIRE tests to confirm nothing broke**

Run: `cd /Users/adam.rothman/src/miniredis && go test -run TestHexpire -v`
Expected: All tests PASS

- [ ] **Step 3: Commit**

```bash
git add cmd_hash.go
git commit -m "refactor: extract parseFieldsArgs helper for FIELDS numfields parsing"
```

### Task 2: Implement HPERSIST wire command

**Files:**
- Modify: `cmd_hash.go` (register + implement `cmdHpersist`)
- Modify: `cmd_hash_test.go` (add `TestHpersist`)

- [ ] **Step 1: Write the failing test**

Add to `cmd_hash_test.go`:

```go
func TestHpersist(t *testing.T) {
	s, c := runWithClient(t)

	t.Run("remove expiration from field", func(t *testing.T) {
		must1(t, c, "HSET", "h1", "f1", "v1")
		mustDo(t, c,
			"HEXPIRE", "h1", "10", "FIELDS", "1", "f1",
			proto.Ints(1),
		)
		mustDo(t, c,
			"HPERSIST", "h1", "FIELDS", "1", "f1",
			proto.Ints(1),
		)
		// Field should survive past original TTL
		s.FastForward(20 * time.Second)
		mustDo(t, c,
			"HGET", "h1", "f1",
			proto.String("v1"),
		)
	})

	t.Run("field without TTL", func(t *testing.T) {
		must1(t, c, "HSET", "h2", "f1", "v1")
		mustDo(t, c,
			"HPERSIST", "h2", "FIELDS", "1", "f1",
			proto.Ints(-1),
		)
	})

	t.Run("non-existent field", func(t *testing.T) {
		must1(t, c, "HSET", "h3", "f1", "v1")
		mustDo(t, c,
			"HPERSIST", "h3", "FIELDS", "1", "nosuch",
			proto.Ints(-2),
		)
	})

	t.Run("non-existent key", func(t *testing.T) {
		mustDo(t, c,
			"HPERSIST", "nokey", "FIELDS", "1", "f1",
			proto.Ints(-2),
		)
	})

	t.Run("multiple fields mixed", func(t *testing.T) {
		mustDo(t, c, "HSET", "h4", "f1", "v1", "f2", "v2", proto.Int(2))
		// Only f1 gets a TTL
		mustDo(t, c,
			"HEXPIRE", "h4", "10", "FIELDS", "1", "f1",
			proto.Ints(1),
		)
		mustDo(t, c,
			"HPERSIST", "h4", "FIELDS", "3", "f1", "f2", "nosuch",
			proto.Ints(1, -1, -2),
		)
	})

	t.Run("wrong type", func(t *testing.T) {
		mustOK(t, c, "SET", "str", "value")
		mustDo(t, c,
			"HPERSIST", "str", "FIELDS", "1", "f1",
			proto.Error(msgWrongType),
		)
	})

	t.Run("error cases", func(t *testing.T) {
		mustDo(t, c,
			"HPERSIST",
			proto.Error(errWrongNumber("hpersist")),
		)
		mustDo(t, c,
			"HPERSIST", "h1",
			proto.Error(errWrongNumber("hpersist")),
		)
		mustDo(t, c,
			"HPERSIST", "h1", "FIELDS", "0", "dummy",
			proto.Error(msgNumFieldsInvalid),
		)
		mustDo(t, c,
			"HPERSIST", "h1", "FIELDS", "2", "f1",
			proto.Error(msgNumFieldsParameter),
		)
	})
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/adam.rothman/src/miniredis && go test -run TestHpersist -v`
Expected: FAIL (command not registered)

- [ ] **Step 3: Implement HPERSIST**

In `cmd_hash.go`, add to `commandsHash()`:
```go
m.srv.Register("HPERSIST", m.cmdHpersist)
```

Add the handler:

```go
// HPERSIST
func (m *Miniredis) cmdHpersist(c *server.Peer, cmd string, args []string) {
	if !m.isValidCMD(c, cmd, args, atLeast(3)) {
		return
	}

	key := args[0]
	fields, errMsg := parseFieldsArgs(args[1:])
	if errMsg != "" {
		setDirty(c)
		c.WriteError(errMsg)
		return
	}

	withTx(m, c, func(c *server.Peer, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		if _, ok := db.keys[key]; !ok {
			c.WriteLen(len(fields))
			for range fields {
				c.WriteInt(-2)
			}
			return
		}

		if db.t(key) != keyTypeHash {
			c.WriteError(msgWrongType)
			return
		}

		c.WriteLen(len(fields))
		for _, field := range fields {
			if _, ok := db.hashKeys[key][field]; !ok {
				c.WriteInt(-2)
				continue
			}

			fieldTTLs := db.hashTTLs[key]
			if fieldTTLs == nil {
				c.WriteInt(-1)
				continue
			}
			if _, ok := fieldTTLs[field]; !ok {
				c.WriteInt(-1)
				continue
			}

			delete(fieldTTLs, field)
			c.WriteInt(1)
		}
	})
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/adam.rothman/src/miniredis && go test -run TestHpersist -v`
Expected: All PASS

- [ ] **Step 5: Run full HEXPIRE tests to confirm no regressions**

Run: `cd /Users/adam.rothman/src/miniredis && go test -run "TestHexpire|TestHpersist|TestCheckHashFieldTTL" -v`
Expected: All PASS

- [ ] **Step 6: Commit**

```bash
git add cmd_hash.go cmd_hash_test.go
git commit -m "feat: implement HPERSIST command"
```

### Task 3: Implement HTTL and HPTTL wire commands

**Files:**
- Modify: `cmd_hash.go` (register + implement `cmdHttl`, `cmdHpttl`, shared `cmdHttlGeneric`)
- Modify: `cmd_hash_test.go` (add `TestHttl`, `TestHpttl`)

- [ ] **Step 1: Write the failing tests**

Add to `cmd_hash_test.go`:

```go
func TestHttl(t *testing.T) {
	s, c := runWithClient(t)

	t.Run("field with TTL", func(t *testing.T) {
		must1(t, c, "HSET", "h1", "f1", "v1")
		mustDo(t, c,
			"HEXPIRE", "h1", "300", "FIELDS", "1", "f1",
			proto.Ints(1),
		)
		mustDo(t, c,
			"HTTL", "h1", "FIELDS", "1", "f1",
			proto.Ints(300),
		)
	})

	t.Run("field without TTL", func(t *testing.T) {
		must1(t, c, "HSET", "h2", "f1", "v1")
		mustDo(t, c,
			"HTTL", "h2", "FIELDS", "1", "f1",
			proto.Ints(-1),
		)
	})

	t.Run("non-existent field", func(t *testing.T) {
		must1(t, c, "HSET", "h3", "f1", "v1")
		mustDo(t, c,
			"HTTL", "h3", "FIELDS", "1", "nosuch",
			proto.Ints(-2),
		)
	})

	t.Run("non-existent key", func(t *testing.T) {
		mustDo(t, c,
			"HTTL", "nokey", "FIELDS", "1", "f1",
			proto.Ints(-2),
		)
	})

	t.Run("TTL decreases after FastForward", func(t *testing.T) {
		must1(t, c, "HSET", "h5", "f1", "v1")
		mustDo(t, c,
			"HEXPIRE", "h5", "100", "FIELDS", "1", "f1",
			proto.Ints(1),
		)
		s.FastForward(30 * time.Second)
		mustDo(t, c,
			"HTTL", "h5", "FIELDS", "1", "f1",
			proto.Ints(70),
		)
	})

	t.Run("multiple fields mixed", func(t *testing.T) {
		mustDo(t, c, "HSET", "h6", "f1", "v1", "f2", "v2", proto.Int(2))
		mustDo(t, c,
			"HEXPIRE", "h6", "60", "FIELDS", "1", "f1",
			proto.Ints(1),
		)
		mustDo(t, c,
			"HTTL", "h6", "FIELDS", "3", "f1", "f2", "nosuch",
			proto.Ints(60, -1, -2),
		)
	})

	t.Run("wrong type", func(t *testing.T) {
		mustOK(t, c, "SET", "str", "value")
		mustDo(t, c,
			"HTTL", "str", "FIELDS", "1", "f1",
			proto.Error(msgWrongType),
		)
	})

	t.Run("error cases", func(t *testing.T) {
		mustDo(t, c,
			"HTTL",
			proto.Error(errWrongNumber("httl")),
		)
		mustDo(t, c,
			"HTTL", "h1",
			proto.Error(errWrongNumber("httl")),
		)
		mustDo(t, c,
			"HTTL", "h1", "FIELDS", "0", "dummy",
			proto.Error(msgNumFieldsInvalid),
		)
	})
}

func TestHpttl(t *testing.T) {
	_, c := runWithClient(t)

	t.Run("field with TTL in milliseconds", func(t *testing.T) {
		must1(t, c, "HSET", "h1", "f1", "v1")
		mustDo(t, c,
			"HEXPIRE", "h1", "10", "FIELDS", "1", "f1",
			proto.Ints(1),
		)
		mustDo(t, c,
			"HPTTL", "h1", "FIELDS", "1", "f1",
			proto.Ints(10000),
		)
	})

	t.Run("field without TTL", func(t *testing.T) {
		must1(t, c, "HSET", "h2", "f1", "v1")
		mustDo(t, c,
			"HPTTL", "h2", "FIELDS", "1", "f1",
			proto.Ints(-1),
		)
	})

	t.Run("non-existent key", func(t *testing.T) {
		mustDo(t, c,
			"HPTTL", "nokey", "FIELDS", "1", "f1",
			proto.Ints(-2),
		)
	})

	t.Run("wrong type", func(t *testing.T) {
		mustOK(t, c, "SET", "str", "value")
		mustDo(t, c,
			"HPTTL", "str", "FIELDS", "1", "f1",
			proto.Error(msgWrongType),
		)
	})

	t.Run("error cases", func(t *testing.T) {
		mustDo(t, c,
			"HPTTL",
			proto.Error(errWrongNumber("hpttl")),
		)
		mustDo(t, c,
			"HPTTL", "h1",
			proto.Error(errWrongNumber("hpttl")),
		)
	})
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/adam.rothman/src/miniredis && go test -run "TestHttl|TestHpttl" -v`
Expected: FAIL

- [ ] **Step 3: Implement HTTL, HPTTL, and shared helper**

In `cmd_hash.go`, add to `commandsHash()`:
```go
m.srv.Register("HTTL", m.cmdHttl, server.ReadOnlyOption())
m.srv.Register("HPTTL", m.cmdHpttl, server.ReadOnlyOption())
```

Add the handlers:

```go
// HTTL
func (m *Miniredis) cmdHttl(c *server.Peer, cmd string, args []string) {
	m.cmdHttlGeneric(c, cmd, args, time.Second)
}

// HPTTL
func (m *Miniredis) cmdHpttl(c *server.Peer, cmd string, args []string) {
	m.cmdHttlGeneric(c, cmd, args, time.Millisecond)
}

func (m *Miniredis) cmdHttlGeneric(c *server.Peer, cmd string, args []string, unit time.Duration) {
	if !m.isValidCMD(c, cmd, args, atLeast(3)) {
		return
	}

	key := args[0]
	fields, errMsg := parseFieldsArgs(args[1:])
	if errMsg != "" {
		setDirty(c)
		c.WriteError(errMsg)
		return
	}

	withTx(m, c, func(c *server.Peer, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		if _, ok := db.keys[key]; !ok {
			c.WriteLen(len(fields))
			for range fields {
				c.WriteInt(-2)
			}
			return
		}

		if db.t(key) != keyTypeHash {
			c.WriteError(msgWrongType)
			return
		}

		c.WriteLen(len(fields))
		for _, field := range fields {
			if _, ok := db.hashKeys[key][field]; !ok {
				c.WriteInt(-2)
				continue
			}

			fieldTTLs := db.hashTTLs[key]
			if fieldTTLs == nil {
				c.WriteInt(-1)
				continue
			}
			ttl, ok := fieldTTLs[field]
			if !ok {
				c.WriteInt(-1)
				continue
			}

			c.WriteInt(int(ttl / unit))
		}
	})
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/adam.rothman/src/miniredis && go test -run "TestHttl|TestHpttl" -v`
Expected: All PASS

- [ ] **Step 5: Run all hash tests to confirm no regressions**

Run: `cd /Users/adam.rothman/src/miniredis && go test -run "TestH" -v`
Expected: All PASS

- [ ] **Step 6: Commit**

```bash
git add cmd_hash.go cmd_hash_test.go
git commit -m "feat: implement HTTL and HPTTL commands"
```

## Chunk 2: HSETEX + Direct API methods + Integration tests + README

### Task 4: Implement HSETEX wire command

**Files:**
- Modify: `cmd_hash.go` (register + implement `cmdHsetex` + `parseHSetEXArgs`)
- Modify: `cmd_hash_test.go` (add `TestHsetex`)

- [ ] **Step 1: Write the failing test**

Add `"strconv"` to the imports in `cmd_hash_test.go`, then add:

```go
func TestHsetex(t *testing.T) {
	s, c := runWithClient(t)

	t.Run("basic with EX", func(t *testing.T) {
		must1(t, c, "HSETEX", "h1", "EX", "10", "FIELDS", "1", "f1", "v1")
		mustDo(t, c,
			"HGET", "h1", "f1",
			proto.String("v1"),
		)
		// Verify TTL is set
		mustDo(t, c,
			"HTTL", "h1", "FIELDS", "1", "f1",
			proto.Ints(10),
		)
	})

	t.Run("multiple fields with EX", func(t *testing.T) {
		must1(t, c, "HSETEX", "h2", "EX", "60", "FIELDS", "2", "f1", "v1", "f2", "v2")
		mustDo(t, c, "HGET", "h2", "f1", proto.String("v1"))
		mustDo(t, c, "HGET", "h2", "f2", proto.String("v2"))
		mustDo(t, c,
			"HTTL", "h2", "FIELDS", "2", "f1", "f2",
			proto.Ints(60, 60),
		)
	})

	t.Run("with PX", func(t *testing.T) {
		must1(t, c, "HSETEX", "h3", "PX", "5000", "FIELDS", "1", "f1", "v1")
		mustDo(t, c,
			"HPTTL", "h3", "FIELDS", "1", "f1",
			proto.Ints(5000),
		)
	})

	t.Run("with EXAT", func(t *testing.T) {
		now := time.Now()
		s.SetTime(now)
		exat := now.Add(30 * time.Second).Unix()
		must1(t, c, "HSETEX", "h_exat", "EXAT", strconv.FormatInt(exat, 10), "FIELDS", "1", "f1", "v1")
		mustDo(t, c, "HGET", "h_exat", "f1", proto.String("v1"))
		mustDo(t, c,
			"HTTL", "h_exat", "FIELDS", "1", "f1",
			proto.Ints(30),
		)
	})

	t.Run("with PXAT", func(t *testing.T) {
		now := time.Now()
		s.SetTime(now)
		pxat := now.Add(10 * time.Second).UnixMilli()
		must1(t, c, "HSETEX", "h_pxat", "PXAT", strconv.FormatInt(pxat, 10), "FIELDS", "1", "f1", "v1")
		mustDo(t, c, "HGET", "h_pxat", "f1", proto.String("v1"))
		mustDo(t, c,
			"HPTTL", "h_pxat", "FIELDS", "1", "f1",
			proto.Ints(10000),
		)
	})

	t.Run("no expiration option", func(t *testing.T) {
		must1(t, c, "HSETEX", "h4", "FIELDS", "1", "f1", "v1")
		mustDo(t, c, "HGET", "h4", "f1", proto.String("v1"))
		mustDo(t, c,
			"HTTL", "h4", "FIELDS", "1", "f1",
			proto.Ints(-1),
		)
	})

	t.Run("FNX - fields don't exist", func(t *testing.T) {
		must1(t, c, "HSETEX", "h5", "FNX", "EX", "10", "FIELDS", "1", "f1", "v1")
		mustDo(t, c, "HGET", "h5", "f1", proto.String("v1"))
	})

	t.Run("FNX - some fields exist", func(t *testing.T) {
		must1(t, c, "HSET", "h6", "f1", "old")
		must0(t, c, "HSETEX", "h6", "FNX", "EX", "10", "FIELDS", "2", "f1", "new", "f2", "v2")
		// Nothing should have changed
		mustDo(t, c, "HGET", "h6", "f1", proto.String("old"))
		mustDo(t, c, "HGET", "h6", "f2", proto.Nil)
	})

	t.Run("FXX - all fields exist", func(t *testing.T) {
		mustDo(t, c, "HSET", "h7", "f1", "old1", "f2", "old2", proto.Int(2))
		must1(t, c, "HSETEX", "h7", "FXX", "EX", "10", "FIELDS", "2", "f1", "new1", "f2", "new2")
		mustDo(t, c, "HGET", "h7", "f1", proto.String("new1"))
		mustDo(t, c, "HGET", "h7", "f2", proto.String("new2"))
	})

	t.Run("FXX - some fields missing", func(t *testing.T) {
		must1(t, c, "HSET", "h8", "f1", "old")
		must0(t, c, "HSETEX", "h8", "FXX", "EX", "10", "FIELDS", "2", "f1", "new", "f2", "v2")
		// Nothing should have changed
		mustDo(t, c, "HGET", "h8", "f1", proto.String("old"))
	})

	t.Run("FXX - key doesn't exist", func(t *testing.T) {
		must0(t, c, "HSETEX", "nokey", "FXX", "EX", "10", "FIELDS", "1", "f1", "v1")
		mustDo(t, c, "EXISTS", "nokey", proto.Int(0))
	})

	t.Run("KEEPTTL", func(t *testing.T) {
		must1(t, c, "HSET", "h9", "f1", "v1")
		mustDo(t, c,
			"HEXPIRE", "h9", "100", "FIELDS", "1", "f1",
			proto.Ints(1),
		)
		// Update value with KEEPTTL
		must1(t, c, "HSETEX", "h9", "KEEPTTL", "FIELDS", "1", "f1", "newval")
		mustDo(t, c, "HGET", "h9", "f1", proto.String("newval"))
		mustDo(t, c,
			"HTTL", "h9", "FIELDS", "1", "f1",
			proto.Ints(100),
		)
	})

	t.Run("expiration actually expires", func(t *testing.T) {
		must1(t, c, "HSETEX", "h10", "EX", "1", "FIELDS", "1", "f1", "v1")
		s.FastForward(2 * time.Second)
		mustDo(t, c, "HGET", "h10", "f1", proto.Nil)
	})

	t.Run("overwrites existing field and clears old TTL", func(t *testing.T) {
		must1(t, c, "HSET", "h11", "f1", "old")
		mustDo(t, c,
			"HEXPIRE", "h11", "100", "FIELDS", "1", "f1",
			proto.Ints(1),
		)
		// Set without expiration option - should clear TTL
		must1(t, c, "HSETEX", "h11", "FIELDS", "1", "f1", "new")
		mustDo(t, c,
			"HTTL", "h11", "FIELDS", "1", "f1",
			proto.Ints(-1),
		)
	})

	t.Run("wrong type", func(t *testing.T) {
		mustOK(t, c, "SET", "str", "value")
		mustDo(t, c,
			"HSETEX", "str", "EX", "10", "FIELDS", "1", "f1", "v1",
			proto.Error(msgWrongType),
		)
	})

	t.Run("error cases", func(t *testing.T) {
		// Not enough args
		mustDo(t, c,
			"HSETEX",
			proto.Error(errWrongNumber("hsetex")),
		)
		mustDo(t, c,
			"HSETEX", "k",
			proto.Error(errWrongNumber("hsetex")),
		)

		// Invalid EX value
		mustDo(t, c,
			"HSETEX", "k", "EX", "notanumber", "FIELDS", "1", "f1", "v1",
			proto.Error(msgInvalidInt),
		)

		// Zero EX
		mustDo(t, c,
			"HSETEX", "k", "EX", "0", "FIELDS", "1", "f1", "v1",
			proto.Error("ERR invalid expire time in HSETEX"),
		)

		// Negative EX
		mustDo(t, c,
			"HSETEX", "k", "EX", "-1", "FIELDS", "1", "f1", "v1",
			proto.Error("ERR invalid expire time in HSETEX"),
		)

		// FNX + FXX
		mustDo(t, c,
			"HSETEX", "k", "FNX", "FXX", "EX", "10", "FIELDS", "1", "f1", "v1",
			proto.Error(msgSyntaxError),
		)

		// EX + PX
		mustDo(t, c,
			"HSETEX", "k", "EX", "10", "PX", "1000", "FIELDS", "1", "f1", "v1",
			proto.Error(msgSyntaxError),
		)

		// Invalid numfields
		mustDo(t, c,
			"HSETEX", "k", "FIELDS", "0", "f1", "v1",
			proto.Error(msgNumFieldsInvalid),
		)

		// Odd number of field-value args
		mustDo(t, c,
			"HSETEX", "k", "FIELDS", "1", "f1",
			proto.Error(msgNumFieldsParameter),
		)
	})
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/adam.rothman/src/miniredis && go test -run TestHsetex -v`
Expected: FAIL

- [ ] **Step 3: Add error message constant**

In `redis.go`, add alongside the other invalid time messages:

```go
msgInvalidHSETEXTime = "ERR invalid expire time in HSETEX"
```

- [ ] **Step 4: Implement parseHSetEXArgs**

Add to `cmd_hash.go`:

```go
type hsetexOpts struct {
	key     string
	fnx     bool
	fxx     bool
	ttlMode string // "", "EX", "PX", "EXAT", "PXAT", "KEEPTTL"
	ttlVal  int    // raw value for EX/PX/EXAT/PXAT
	fields  []string
	values  []string
}

func parseHSetEXArgs(args []string) (hsetexOpts, string) {
	var opts hsetexOpts
	opts.key = args[0]
	args = args[1:]

	for len(args) > 0 {
		switch strings.ToUpper(args[0]) {
		case "FNX":
			opts.fnx = true
			args = args[1:]
		case "FXX":
			opts.fxx = true
			args = args[1:]
		case "KEEPTTL":
			if opts.ttlMode != "" {
				return hsetexOpts{}, msgSyntaxError
			}
			opts.ttlMode = "KEEPTTL"
			args = args[1:]
		case "EX", "PX", "EXAT", "PXAT":
			if opts.ttlMode != "" {
				return hsetexOpts{}, msgSyntaxError
			}
			mode := strings.ToUpper(args[0])
			if len(args) < 2 {
				return hsetexOpts{}, msgInvalidInt
			}
			var val int
			if err := optIntSimple(args[1], &val); err != nil {
				return hsetexOpts{}, msgInvalidInt
			}
			if val <= 0 {
				return hsetexOpts{}, msgInvalidHSETEXTime
			}
			opts.ttlMode = mode
			opts.ttlVal = val
			args = args[2:]
		case "FIELDS":
			if len(args) < 2 {
				return hsetexOpts{}, msgNumFieldsInvalid
			}
			var numFields int
			if err := optIntSimple(args[1], &numFields); err != nil {
				return hsetexOpts{}, msgNumFieldsInvalid
			}
			if numFields <= 0 {
				return hsetexOpts{}, msgNumFieldsInvalid
			}
			// Need numFields * 2 args (field value pairs)
			if len(args) < 2+numFields*2 {
				return hsetexOpts{}, msgNumFieldsParameter
			}
			if len(args) > 2+numFields*2 {
				return hsetexOpts{}, msgNumFieldsParameter
			}
			fvArgs := args[2 : 2+numFields*2]
			for i := 0; i < len(fvArgs); i += 2 {
				opts.fields = append(opts.fields, fvArgs[i])
				opts.values = append(opts.values, fvArgs[i+1])
			}
			args = args[2+numFields*2:]
		default:
			return hsetexOpts{}, msgSyntaxError
		}
	}

	if opts.fnx && opts.fxx {
		return hsetexOpts{}, msgSyntaxError
	}

	if len(opts.fields) == 0 {
		return hsetexOpts{}, fmt.Sprintf(msgMandatoryArgument, "FIELDS")
	}

	return opts, ""
}
```

- [ ] **Step 5: Implement cmdHsetex**

Add to `cmd_hash.go`, and register in `commandsHash()`:

```go
m.srv.Register("HSETEX", m.cmdHsetex)
```

```go
// HSETEX
func (m *Miniredis) cmdHsetex(c *server.Peer, cmd string, args []string) {
	if !m.isValidCMD(c, cmd, args, atLeast(4)) {
		return
	}

	opts, errMsg := parseHSetEXArgs(args)
	if errMsg != "" {
		setDirty(c)
		c.WriteError(errMsg)
		return
	}

	withTx(m, c, func(c *server.Peer, ctx *connCtx) {
		db := m.db(ctx.selectedDB)

		if t, ok := db.keys[opts.key]; ok && t != keyTypeHash {
			c.WriteError(msgWrongType)
			return
		}

		// FNX: only set if none of the specified fields exist
		if opts.fnx {
			for _, field := range opts.fields {
				if _, ok := db.hashKeys[opts.key][field]; ok {
					c.WriteInt(0)
					return
				}
			}
		}

		// FXX: only set if all of the specified fields exist
		if opts.fxx {
			for _, field := range opts.fields {
				if _, ok := db.hashKeys[opts.key][field]; !ok {
					c.WriteInt(0)
					return
				}
			}
		}

		// Resolve TTL
		var ttl time.Duration
		hasTTL := false
		keepTTL := false
		switch opts.ttlMode {
		case "EX":
			ttl = time.Duration(opts.ttlVal) * time.Second
			hasTTL = true
		case "PX":
			ttl = time.Duration(opts.ttlVal) * time.Millisecond
			hasTTL = true
		case "EXAT":
			ttl = m.at(opts.ttlVal, time.Second)
			hasTTL = true
		case "PXAT":
			ttl = m.at(opts.ttlVal, time.Millisecond)
			hasTTL = true
		case "KEEPTTL":
			keepTTL = true
		}

		// Set all fields
		for i, field := range opts.fields {
			db.hashSet(opts.key, field, opts.values[i])

			if keepTTL {
				// Don't touch existing TTL
				continue
			}

			// Initialize TTL map if needed
			if db.hashTTLs[opts.key] == nil {
				if hasTTL {
					db.hashTTLs[opts.key] = map[string]time.Duration{}
				}
			}

			if hasTTL {
				db.hashTTLs[opts.key][field] = ttl
			} else {
				// No TTL option: remove any existing TTL
				if fieldTTLs := db.hashTTLs[opts.key]; fieldTTLs != nil {
					delete(fieldTTLs, field)
				}
			}
		}

		c.WriteInt(1)
	})
}
```

- [ ] **Step 6: Run test to verify it passes**

Run: `cd /Users/adam.rothman/src/miniredis && go test -run TestHsetex -v`
Expected: All PASS

- [ ] **Step 7: Run all hash tests**

Run: `cd /Users/adam.rothman/src/miniredis && go test -run "TestH" -v`
Expected: All PASS

- [ ] **Step 8: Commit**

```bash
git add cmd_hash.go cmd_hash_test.go redis.go
git commit -m "feat: implement HSETEX command"
```

### Task 5: Add direct API methods

**Files:**
- Modify: `direct.go` (add `HExpire`, `HPersist`, `HTTL`, `HSetEX` on both `*Miniredis` and `*RedisDB`)
- Modify: `cmd_hash_test.go` (add `TestDirectHashFieldTTL`)

- [ ] **Step 1: Write the failing test**

Add to `cmd_hash_test.go`:

```go
func TestDirectHashFieldTTL(t *testing.T) {
	s := NewMiniRedis()
	defer s.Close()

	t.Run("HExpire", func(t *testing.T) {
		s.HSet("h1", "f1", "v1")

		// Set TTL
		assert(t, s.HExpire("h1", "f1", 10*time.Second), "HExpire should return true")

		// Verify with HTTL
		equals(t, 10*time.Second, s.HTTL("h1", "f1"))

		// Non-existent field
		assert(t, !s.HExpire("h1", "nosuch", 10*time.Second), "HExpire should return false for missing field")

		// Non-existent key
		assert(t, !s.HExpire("nokey", "f1", 10*time.Second), "HExpire should return false for missing key")
	})

	t.Run("HPersist", func(t *testing.T) {
		s.HSet("h2", "f1", "v1")
		s.HExpire("h2", "f1", 10*time.Second)

		// Remove TTL
		assert(t, s.HPersist("h2", "f1"), "HPersist should return true")
		equals(t, time.Duration(0), s.HTTL("h2", "f1"))

		// No TTL to remove
		assert(t, !s.HPersist("h2", "f1"), "HPersist should return false when no TTL")

		// Non-existent field
		assert(t, !s.HPersist("h2", "nosuch"), "HPersist should return false for missing field")
	})

	t.Run("HTTL", func(t *testing.T) {
		s.HSet("h3", "f1", "v1")

		// No TTL
		equals(t, time.Duration(0), s.HTTL("h3", "f1"))

		// With TTL
		s.HExpire("h3", "f1", 30*time.Second)
		equals(t, 30*time.Second, s.HTTL("h3", "f1"))

		// Non-existent
		equals(t, time.Duration(0), s.HTTL("h3", "nosuch"))
		equals(t, time.Duration(0), s.HTTL("nokey", "f1"))
	})

	t.Run("HSetEX", func(t *testing.T) {
		s.HSetEX("h4", 10*time.Second, "f1", "v1", "f2", "v2")

		equals(t, "v1", s.HGet("h4", "f1"))
		equals(t, "v2", s.HGet("h4", "f2"))
		equals(t, 10*time.Second, s.HTTL("h4", "f1"))
		equals(t, 10*time.Second, s.HTTL("h4", "f2"))
	})

	t.Run("HSetEX overwrites and sets new TTL", func(t *testing.T) {
		s.HSet("h5", "f1", "old")
		s.HExpire("h5", "f1", 100*time.Second)

		s.HSetEX("h5", 5*time.Second, "f1", "new")
		equals(t, "new", s.HGet("h5", "f1"))
		equals(t, 5*time.Second, s.HTTL("h5", "f1"))
	})

	t.Run("DB-level methods", func(t *testing.T) {
		db := s.DB(0)
		db.HSet("h6", "f1", "v1")
		assert(t, db.HExpire("h6", "f1", 10*time.Second), "DB.HExpire should return true")
		equals(t, 10*time.Second, db.HTTL("h6", "f1"))
		assert(t, db.HPersist("h6", "f1"), "DB.HPersist should return true")
		equals(t, time.Duration(0), db.HTTL("h6", "f1"))
		db.HSetEX("h7", 5*time.Second, "f1", "v1")
		equals(t, "v1", db.HGet("h7", "f1"))
		equals(t, 5*time.Second, db.HTTL("h7", "f1"))
	})
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /Users/adam.rothman/src/miniredis && go test -run TestDirectHashFieldTTL -v`
Expected: FAIL (methods not defined)

- [ ] **Step 3: Implement direct methods**

Add to `direct.go`:

```go
// HExpire sets a TTL on a hash field. Returns true if the field exists.
func (m *Miniredis) HExpire(key, field string, ttl time.Duration) bool {
	return m.DB(m.selectedDB).HExpire(key, field, ttl)
}

// HExpire sets a TTL on a hash field. Returns true if the field exists.
func (db *RedisDB) HExpire(key, field string, ttl time.Duration) bool {
	db.master.Lock()
	defer db.master.Unlock()
	defer db.master.signal.Broadcast()

	if !db.exists(key) || db.t(key) != keyTypeHash {
		return false
	}
	if _, ok := db.hashKeys[key][field]; !ok {
		return false
	}
	if db.hashTTLs[key] == nil {
		db.hashTTLs[key] = map[string]time.Duration{}
	}
	db.hashTTLs[key][field] = ttl
	return true
}

// HPersist removes a TTL from a hash field. Returns true if there was a TTL.
func (m *Miniredis) HPersist(key, field string) bool {
	return m.DB(m.selectedDB).HPersist(key, field)
}

// HPersist removes a TTL from a hash field. Returns true if there was a TTL.
func (db *RedisDB) HPersist(key, field string) bool {
	db.master.Lock()
	defer db.master.Unlock()
	defer db.master.signal.Broadcast()

	fieldTTLs := db.hashTTLs[key]
	if fieldTTLs == nil {
		return false
	}
	if _, ok := fieldTTLs[field]; !ok {
		return false
	}
	delete(fieldTTLs, field)
	return true
}

// HTTL returns the remaining TTL of a hash field. Returns 0 if no TTL.
func (m *Miniredis) HTTL(key, field string) time.Duration {
	return m.DB(m.selectedDB).HTTL(key, field)
}

// HTTL returns the remaining TTL of a hash field. Returns 0 if no TTL.
func (db *RedisDB) HTTL(key, field string) time.Duration {
	db.master.Lock()
	defer db.master.Unlock()

	fieldTTLs := db.hashTTLs[key]
	if fieldTTLs == nil {
		return 0
	}
	return fieldTTLs[field]
}

// HSetEX sets hash fields with a TTL.
func (m *Miniredis) HSetEX(key string, ttl time.Duration, fv ...string) {
	m.DB(m.selectedDB).HSetEX(key, ttl, fv...)
}

// HSetEX sets hash fields with a TTL.
func (db *RedisDB) HSetEX(key string, ttl time.Duration, fv ...string) {
	db.master.Lock()
	defer db.master.Unlock()
	defer db.master.signal.Broadcast()

	db.hashSet(key, fv...)
	if db.hashTTLs[key] == nil {
		db.hashTTLs[key] = map[string]time.Duration{}
	}
	for i := 0; i < len(fv); i += 2 {
		db.hashTTLs[key][fv[i]] = ttl
	}
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /Users/adam.rothman/src/miniredis && go test -run TestDirectHashFieldTTL -v`
Expected: All PASS

- [ ] **Step 5: Run all tests**

Run: `cd /Users/adam.rothman/src/miniredis && go test ./... -count=1`
Expected: All PASS

- [ ] **Step 6: Commit**

```bash
git add direct.go cmd_hash_test.go
git commit -m "feat: add HExpire, HPersist, HTTL, HSetEX direct methods"
```

### Task 6: Add integration tests

**Files:**
- Modify: `integration/hash_test.go`

- [ ] **Step 1: Add integration tests**

Add to `integration/hash_test.go`, within `TestHash`:

```go
	t.Run("persist", func(t *testing.T) {
		testRaw(t, func(c *client) {
			c.Do("HSET", "aap", "noot", "mies")
			c.Do("HEXPIRE", "aap", "10", "FIELDS", "1", "noot")
			c.Do("HPERSIST", "aap", "FIELDS", "1", "noot")

			c.Error("wrong number", "HPERSIST", "aap")
			c.Error("numFields", "HPERSIST", "aap", "FIELDS", "0", "dummy")
		})
	})

	t.Run("ttl", func(t *testing.T) {
		testRaw(t, func(c *client) {
			c.Do("HSET", "aap", "noot", "mies")
			c.Do("HEXPIRE", "aap", "10", "FIELDS", "1", "noot")
			c.Do("HTTL", "aap", "FIELDS", "1", "noot")
			c.Do("HPTTL", "aap", "FIELDS", "1", "noot")

			c.Error("wrong number", "HTTL", "aap")
			c.Error("wrong number", "HPTTL", "aap")
		})
	})

	t.Run("setex", func(t *testing.T) {
		testRaw(t, func(c *client) {
			c.Do("HSETEX", "aap", "EX", "10", "FIELDS", "1", "noot", "mies")
			c.Do("HGET", "aap", "noot")
			c.Do("HTTL", "aap", "FIELDS", "1", "noot")

			c.Do("HSETEX", "bbb", "FNX", "PX", "5000", "FIELDS", "1", "cc", "dd")
			c.Do("HSETEX", "bbb", "FNX", "PX", "5000", "FIELDS", "1", "cc", "ee")
			c.Do("HGET", "bbb", "cc")

			c.Error("wrong number", "HSETEX")
			c.Error("wrong number", "HSETEX", "k")
			c.Error("syntax", "HSETEX", "k", "EX", "10", "PX", "5000", "FIELDS", "1", "f1", "v1")
		})
	})
```

- [ ] **Step 2: Run integration tests (miniredis only, no real Redis needed)**

Run: `cd /Users/adam.rothman/src/miniredis && go test ./integration/ -run "TestHash$" -v`
Expected: All PASS (these will run against miniredis since `skip(t)` skips when no real Redis is available)

- [ ] **Step 3: Commit**

```bash
git add integration/hash_test.go
git commit -m "test: add integration tests for HPERSIST, HTTL, HPTTL, HSETEX"
```

### Task 7: Update README

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Update hash commands list**

In `README.md`, find the `Hash keys` section (around line 100-116). Add the new commands in alphabetical order. The section should become:

```
 - Hash keys (complete)
   - HDEL
   - HEXISTS
   - HEXPIRE
   - HGET
   - HGETALL
   - HINCRBY
   - HINCRBYFLOAT
   - HKEYS
   - HLEN
   - HMGET
   - HMSET
   - HPERSIST
   - HPTTL
   - HRANDFIELD
   - HSET
   - HSETEX
   - HSETNX
   - HSTRLEN
   - HTTL
   - HVALS
   - HSCAN
```

- [ ] **Step 2: Commit**

```bash
git add README.md
git commit -m "docs: add HEXPIRE, HPERSIST, HTTL, HPTTL, HSETEX to README"
```
