package proto

import (
	"fmt"
	"strings"
)

// Byte-safe string
func String(s string) string {
	return fmt.Sprintf("$%d\r\n%s\r\n", len(s), s)
}

// Inline string
func Inline(s string) string {
	return inline('+', s)
}

// Error
func Error(s string) string {
	return inline('-', s)
}

func inline(r rune, s string) string {
	return fmt.Sprintf("%s%s\r\n", string(r), s)
}

// Int
func Int(n int) string {
	return fmt.Sprintf(":%d\r\n", n)
}

// Array assembles the args in a list. Args should be raw redis commands.
// Example: Array(String("foo"), String("bar"))
func Array(args ...string) string {
	return fmt.Sprintf("*%d\r\n", len(args)) + strings.Join(args, "")
}
