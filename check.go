package miniredis

// 'Error' methods.

import (
	"fmt"
	"path/filepath"
	"runtime"
	"testing"
)

// CheckStr does not call Errorf() only when there is a string key with the
// expected value. Normal use case is `m.CheckStr(t, "username", "theking")`.
func (m *Miniredis) CheckStr(t *testing.T, key, expected string) {
	m.Lock()
	defer m.Unlock()

	db := m.db(m.selectedDB)
	v, ok := db.keys[key]
	if !ok {
		lError(t, "string key %#v not found", key)
		return
	}
	if v != "string" {
		lError(t, "key %#v is not a string key, but a %s", key, v)
		return
	}
	found, ok := db.stringKeys[key]
	if !ok {
		panic("internal: stringKeys not found, but should be there")
	}
	if found != expected {
		lError(t, "string key %#v: Expected %#v, got %#v", key, expected, found)
		return
	}
}

func lError(t *testing.T, format string, args ...interface{}) {
	_, file, line, _ := runtime.Caller(2)
	prefix := fmt.Sprintf("%s:%d: ", filepath.Base(file), line)
	fmt.Printf(prefix+format+"\n", args...)
	t.Fail()
}
