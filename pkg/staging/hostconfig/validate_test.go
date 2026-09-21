package hostconfig

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValidateName(t *testing.T) {
	cases := []struct {
		name string
		pass bool
	}{
		{"host1", true},
		{"", false},
		{"host%1", false},
		{"..", false},
		{"foo/bar", false},
		{"../../etc", false},
		{"-host1", false},
		{"host_1-a", true},
		{"host 1", false},
		{"hostname", true},
	}

	for _, c := range cases {
		if c.pass {
			assert.NoError(t, Validate(Config{Name: c.name}), "Name=%q", c.name)
		} else {
			assert.Error(t, Validate(Config{Name: c.name}), "Name=%q", c.name)
		}
	}
}
