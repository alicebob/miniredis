package miniredis

// 'Fail' methods.

import (
	"fmt"
	"path/filepath"
	"runtime"
)

// T is implemented by Testing.T
type T interface {
	Fail()
}

// CheckGet does not call Errorf() iff there is a string key with the
// expected value. Normal use case is `m.CheckGet(t, "username", "theking")`.
func (m *Miniredis) CheckGet(t T, key, expected string) {
	found, err := m.Get(key)
	if err != nil {
		lError(t, "GET error, key %#v: %v", key, err)
		return
	}
	if found != expected {
		lError(t, "GET error, key %#v: Expected %#v, got %#v", key, expected, found)
		return
	}
}

func lError(t T, format string, args ...interface{}) {
	_, file, line, _ := runtime.Caller(2)
	prefix := fmt.Sprintf("%s:%d: ", filepath.Base(file), line)
	fmt.Printf(prefix+format+"\n", args...)
	t.Fail()
}
