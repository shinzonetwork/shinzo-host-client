package schema

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetSchema(t *testing.T) {
	s := GetSchema()
	require.NotEmpty(t, s)
}

func TestGetBranchableSchema(t *testing.T) {
	s := GetBranchableSchema()
	require.NotEmpty(t, s)
}

func TestGetSchemaForBuild(t *testing.T) {
	s := GetSchemaForBuild()
	require.NotEmpty(t, s)

	// Without the branchable build tag, should return standard schema
	if !IsBranchable() {
		require.Equal(t, GetSchema(), s)
	} else {
		require.Equal(t, GetBranchableSchema(), s)
	}
}

func TestIsBranchable(t *testing.T) {
	// Just verify it returns a boolean without panicking
	_ = IsBranchable()
}

func TestGetSchemaForBuild_ReturnsNonEmpty(t *testing.T) {
	s := GetSchemaForBuild()
	require.NotEmpty(t, s)

	// Without the branchable build tag, IsBranchable() returns false,
	// so GetSchemaForBuild should return the standard schema.
	// With the branchable build tag, it returns the branchable schema.
	if IsBranchable() {
		require.Equal(t, GetBranchableSchema(), s)
		require.NotEqual(t, GetSchema(), s, "branchable schema should differ from standard")
	} else {
		require.Equal(t, GetSchema(), s)
	}
}

func TestGetSchemaForBuild_DefaultPath(t *testing.T) {
	// Verify the default (non-branchable) path explicitly
	// Without build tag, IsBranchable() is false, so GetSchemaForBuild
	// must return the same content as GetSchema().
	result := GetSchemaForBuild()
	standard := GetSchema()

	if !IsBranchable() {
		require.Equal(t, standard, result,
			"GetSchemaForBuild should return standard schema when IsBranchable is false")
	}
}

func TestGetSchema_ContainsExpectedTypes(t *testing.T) {
	s := GetSchema()
	require.Contains(t, s, "type", "schema should contain GraphQL type definitions")
}

func TestGetBranchableSchema_ContainsExpectedTypes(t *testing.T) {
	s := GetBranchableSchema()
	require.Contains(t, s, "type", "branchable schema should contain GraphQL type definitions")
}

func TestIsBranchable_DefaultBuild(t *testing.T) {
	// Without the branchable build tag, IsBranchable must return false.
	require.False(t, IsBranchable(),
		"IsBranchable should return false in default (non-branchable) builds")
}

func TestGetSchemaForBuild_ReturnsSameAsGetSchema(t *testing.T) {
	// In the default build, GetSchemaForBuild must return GetSchema() (the standard schema).
	require.Equal(t, GetSchema(), GetSchemaForBuild(),
		"GetSchemaForBuild must equal GetSchema in default build")
}

func TestGetSchema_And_GetBranchableSchema_BothNonEmpty(t *testing.T) {
	standard := GetSchema()
	branchable := GetBranchableSchema()
	require.NotEmpty(t, standard)
	require.NotEmpty(t, branchable)
}

func TestGetSchemaForBuild_CallsBothSchemasAvailable(t *testing.T) {
	// Verify that both schemas are loadable and GetSchemaForBuild picks the correct one.
	result := GetSchemaForBuild()
	standard := GetSchema()
	branchable := GetBranchableSchema()

	// In default build, IsBranchable is false so result should equal standard.
	if !IsBranchable() {
		require.Equal(t, standard, result)
		// Branchable schema is still available but not selected.
		require.NotEmpty(t, branchable)
	}
}
