# Hash Field TTL Commands: HSETEX, HPERSIST, HTTL, HPTTL

## Summary

Add four new Redis wire commands (HSETEX, HPERSIST, HTTL, HPTTL) and four direct API methods (HExpire, HPersist, HTTL, HSetEX) to miniredis. These complement the existing HEXPIRE command to provide full hash-field-TTL support.

Note: no direct HPTTL method is needed because the HTTL direct method returns `time.Duration`, which already has full precision.

## Background

PR #424 added HEXPIRE support, including the internal `db.hashTTLs` storage (`map[string]map[string]time.Duration`) and TTL expiration logic in `db.checkHashFieldTTL()`. The infrastructure is in place; we need to add the remaining commands that operate on hash field TTLs.

## Wire Commands

### Key-does-not-exist behavior

For HPERSIST, HTTL, and HPTTL: when the key does not exist, return an array of `-2` values (one per requested field). This matches the existing HEXPIRE implementation in this codebase. Note: real Redis may return a nil response for non-existent keys in some versions; we follow the HEXPIRE precedent for internal consistency.

### HPERSIST

Removes the expiration from hash fields.

**Syntax:** `HPERSIST key FIELDS numfields field [field ...]`

**Returns (array, one integer per field):**
- `1` — expiration removed
- `-1` — field exists but has no TTL
- `-2` — field or key doesn't exist

**Implementation:** Parse the `FIELDS numfields` block, then for each field check and delete `db.hashTTLs[key][field]`.

### HTTL

Returns the remaining TTL of hash fields in seconds.

**Syntax:** `HTTL key FIELDS numfields field [field ...]`

**Returns (array, one integer per field):**
- `>= 0` — remaining TTL in seconds
- `-1` — field exists but no TTL
- `-2` — field or key doesn't exist

### HPTTL

Identical to HTTL but returns milliseconds.

**Syntax:** `HPTTL key FIELDS numfields field [field ...]`

**Returns (array, one integer per field):**
- `>= 0` — remaining TTL in milliseconds
- `-1` — field exists but no TTL
- `-2` — field or key doesn't exist

**Shared implementation:** HTTL and HPTTL share a single implementation function with a time divisor parameter.

### HSETEX

Sets hash fields with expiration atomically.

**Syntax:**
```
HSETEX key [FNX | FXX] [EX seconds | PX milliseconds | EXAT unix-time-seconds | PXAT unix-time-milliseconds | KEEPTTL] FIELDS numfields field value [field value ...]
```

**Returns:**
- `1` — all fields were set
- `0` — no fields were set (FNX/FXX condition not met)

**Options:**
- `FNX` — only set if *none* of the specified fields exist. If the key doesn't exist, FNX succeeds (no fields can exist).
- `FXX` — only set if *all* of the specified fields exist. If the key doesn't exist, FXX fails (returns 0, key is not created).
- `EX seconds` — set expiration in seconds
- `PX milliseconds` — set expiration in milliseconds
- `EXAT unix-time-seconds` — set expiration as Unix timestamp (seconds)
- `PXAT unix-time-milliseconds` — set expiration as Unix timestamp (milliseconds)
- `KEEPTTL` — retain each field's existing TTL (if any)
- Expiration options (`EX`, `PX`, `EXAT`, `PXAT`, `KEEPTTL`) are mutually exclusive
- `FNX` and `FXX` are mutually exclusive

**When no expiration option is provided:** Fields are set with no TTL (persistent). Any existing field TTLs are removed.

**TTL validation:** `EX` and `PX` values must be positive integers (> 0). Zero or negative values return an error (reuse `msgInvalidSETime` pattern). `EXAT` and `PXAT` must be valid positive timestamps.

**Atomicity:** Either all fields are set (with the specified expiration) or none are.

**Argument parsing:** Extracted into a `parseHSetEXArgs` function. Uses a `for/switch` loop like the `SET` command. The parse function returns the raw timestamp value for `EXAT`/`PXAT` options (not a resolved duration), since converting absolute timestamps to relative durations requires `m.at()` which is only available on the `*Miniredis` receiver. The command handler calls `m.at()` to resolve EXAT/PXAT to a duration before applying. Mutual exclusivity of options is validated after the full parse loop completes.

## Direct API Methods

Simple convenience methods for test setup and assertions. All follow the existing pattern: `Miniredis` method delegates to `m.DB(m.selectedDB).Method()`, `RedisDB` method acquires lock and does the work.

### `HExpire(key, field string, ttl time.Duration) bool`
Sets a TTL on a single hash field. Returns `true` if the field exists and TTL was set.

### `HPersist(key, field string) bool`
Removes TTL from a single hash field. Returns `true` if there was a TTL to remove.

### `HTTL(key, field string) time.Duration`
Returns remaining TTL of a hash field. Returns `0` if no TTL or field doesn't exist (same convention as existing `TTL()` method).

### `HSetEX(key string, ttl time.Duration, fv ...string)`
Sets hash fields with a TTL. Simple fire-and-forget like `HSet`.

## Command Registration

In `commandsHash()` in `cmd_hash.go`:

```go
m.srv.Register("HPERSIST", m.cmdHpersist)
m.srv.Register("HTTL", m.cmdHttl, server.ReadOnlyOption())
m.srv.Register("HPTTL", m.cmdHpttl, server.ReadOnlyOption())
m.srv.Register("HSETEX", m.cmdHsetex)
```

HTTL and HPTTL are read-only. HPERSIST and HSETEX are not.

## Error Messages

Reuse existing error constants from `redis.go`:
- `msgSyntaxError` — conflicting options or bad syntax
- `msgWrongType` — wrong key type
- `msgNumFieldsParameter` / `msgNumFieldsInvalid` — FIELDS block parsing
- `msgInvalidInt` — non-integer TTL values
- `msgMandatoryArgument` — missing FIELDS keyword
- `msgInvalidSETime` — zero or negative EX/PX values (or add a HSETEX-specific variant following the `msgInvalidSETEXTime` pattern)

No new error constants needed beyond potentially `msgInvalidHSETEXTime`.

## Files Modified

- `cmd_hash.go` — new command implementations + registration
- `cmd_hash_test.go` — unit tests for all 4 wire commands
- `direct.go` — 4 new direct methods (HExpire, HPersist, HTTL, HSetEX)
- `integration/hash_test.go` — integration tests against real Redis
- `README.md` — add new commands to the command list

## Testing Strategy

### Unit tests (`cmd_hash_test.go`)
Per command:
- Happy path with fields that exist
- Key doesn't exist (returns -2 for all fields)
- Field doesn't exist (returns -2 for that field)
- Wrong type error
- Argument validation (wrong arg count, invalid numfields)

Command-specific:
- **HPERSIST:** field with TTL (returns 1), field without TTL (returns -1)
- **HTTL/HPTTL:** correct value in seconds/milliseconds, field without TTL returns -1
- **HSETEX:** FNX when fields exist (returns 0, nothing set), FXX when fields missing (returns 0, nothing set), FXX on non-existent key (returns 0, key not created), all EX/PX/EXAT/PXAT/KEEPTTL variants, no expiration option (fields set with no TTL), mutual exclusivity errors (EX+PX, FNX+FXX), zero/negative TTL errors

### Integration tests (`integration/hash_test.go`)
One test per command covering core behavior, run against both miniredis and real Redis.

### Direct methods
Exercised within unit tests (e.g., set TTL with `m.HExpire()`, verify with `m.HTTL()`).
