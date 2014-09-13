package miniredis

// helper for the KEYS command

import (
	"regexp"
)

// patternRE compiles a KEYS argument to a regexp. Returns nil if the given
// pattern will never match anything.
func patternRE(k string) *regexp.Regexp {
	re := []byte(`^\Q`)
	for i := 0; i < len(k); i++ {
		p := k[i]
		switch p {
		case '*':
			re = append(re, []byte(`\E.*\Q`)...)
		case '?':
			re = append(re, []byte(`\E.\Q`)...)
		case '[':
			charClass := []byte{}
			i++
			for ; i < len(k); i++ {
				if k[i] == ']' {
					break
				}
				if k[i] == '\\' {
					if i == len(k)-1 {
						// Ends with a '\'. U-huh.
						return nil
					}
					charClass = append(charClass, k[i])
					i++
					charClass = append(charClass, k[i])
					continue
				}
				charClass = append(charClass, k[i])
			}
			if len(charClass) == 0 {
				// '[]' is valid in Redis, but matches nothing.
				return nil
			}
			re = append(re, []byte(`\E[`)...)
			re = append(re, charClass...)
			re = append(re, []byte(`]\Q`)...)

		case '\\':
			if i == len(k)-1 {
				// Ends with a '\'. U-huh.
				return nil
			}
			// Forget the \, keep the next char.
			i++
			re = append(re, k[i])
			continue
		default:
			re = append(re, p)
		}
	}
	re = append(re, []byte(`\E$`)...)
	return regexp.MustCompile(string(re))
}
