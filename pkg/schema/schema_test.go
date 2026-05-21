package schema

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetSchema(t *testing.T) {
	s := GetSchema()
	require.NotEmpty(t, s)
}

func TestGetSchema_ContainsExpectedTypes(t *testing.T) {
	s := GetSchema()
	require.Contains(t, s, "type", "schema should contain GraphQL type definitions")
}
