package hostconfig

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValidateName(t *testing.T) {
	cases := []struct {
		desc string
		name string
		pass bool
	}{
		{"valid alphanumeric", "host1", true},
		{"empty", "", false},
		{"invalid character", "host%1", false},
		{"dots only", "..", false},
		{"path traversal", "foo/bar", false},
		{"nested path traversal", "../../etc", false},
		{"leading hyphen", "-host1", false},
		{"hyphen and underscore", "host_1-a", true},
		{"space", "host 1", false},
		{"plain lowercase", "hostname", true},
		{"embedded single quote", "host'1", false},
		{"embedded newline", "host\n1", false},
		{"embedded backtick", "host`1", false},
		{"bare backtick", "`", false},
		{"bare single quote", "'", false},
		{"bare slash", "/", false},
		{"bare backslash", "\\", false},
		{"63 chars, at limit", strings.Repeat("a", 63), true},
		{"64 chars, over limit", strings.Repeat("a", 64), false},
	}

	for _, c := range cases {
		t.Run(c.desc, func(t *testing.T) {
			if c.pass {
				assert.NoError(t, validate(Config{Name: c.name}))
			} else {
				assert.Error(t, validate(Config{Name: c.name}))
			}
		})
	}
}
