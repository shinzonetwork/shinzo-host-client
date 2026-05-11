package view

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// NewSchemaService
// ---------------------------------------------------------------------------

func TestNewSchemaService(t *testing.T) {
	ss := NewSchemaService()
	require.NotNil(t, ss)
}

// ---------------------------------------------------------------------------
// NormalizeSDL
// ---------------------------------------------------------------------------

func TestNormalizeSDL(t *testing.T) {
	tests := []struct {
		name     string
		sdl      string
		typeName string
		options  SDLOptions
		want     string
	}{
		{
			name:     "empty typeName does not rename",
			sdl:      sdlFooBase,
			typeName: "",
			options:  SDLOptions{Materialized: false},
			want:     sdlFooMaterialized,
		},
		{
			name:     "with typeName renames the type",
			sdl:      "type OldName {\n  id: ID\n}",
			typeName: "NewName",
			options:  SDLOptions{Materialized: false},
			want:     "type NewName @materialized(if: false) {\n  id: ID\n}",
		},
		{
			name:     "materialized true",
			sdl:      sdlFooBase,
			typeName: "",
			options:  SDLOptions{Materialized: true},
			want:     sdlFooMatTrue,
		},
		{
			name:     "materialized false",
			sdl:      sdlFooBase,
			typeName: "",
			options:  SDLOptions{Materialized: false},
			want:     sdlFooMaterialized,
		},
		{
			name:     "required fields added",
			sdl:      sdlFooBase,
			typeName: "",
			options: SDLOptions{
				Materialized:   false,
				RequiredFields: []FieldDef{{Name: testFieldCreatedAt, Type: gqlScalarString}},
			},
			want: "type Foo @materialized(if: false) {\n  id: ID\n  createdAt: String\n}",
		},
		{
			name:     "combination: rename + materialized + required fields",
			sdl:      "type Old {\n  id: ID\n}",
			typeName: "New",
			options: SDLOptions{
				Materialized:   true,
				RequiredFields: []FieldDef{{Name: "ts", Type: gqlScalarInt}},
			},
			want: "type New @materialized(if: true) {\n  id: ID\n  ts: Int\n}",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ss := NewSchemaService()
			got := ss.NormalizeSDL(tt.sdl, tt.typeName, tt.options)
			require.Equal(t, tt.want, got)
		})
	}
}

// ---------------------------------------------------------------------------
// ensureMaterializedDirective
// ---------------------------------------------------------------------------

func TestEnsureMaterializedDirective(t *testing.T) {
	tests := []struct {
		name         string
		sdl          string
		materialized bool
		want         string
	}{
		{
			name:         "replace existing directive with true",
			sdl:          sdlFooMaterialized,
			materialized: true,
			want:         sdlFooMatTrue,
		},
		{
			name:         "replace existing directive with false",
			sdl:          sdlFooMatTrue,
			materialized: false,
			want:         sdlFooMaterialized,
		},
		{
			name:         "add directive to type with braces",
			sdl:          sdlFooBase,
			materialized: true,
			want:         sdlFooMatTrue,
		},
		{
			name:         "no type block returns unchanged",
			sdl:          gqlScalarDateTime,
			materialized: true,
			want:         gqlScalarDateTime,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ss := NewSchemaService()
			got := ss.ensureMaterializedDirective(tt.sdl, tt.materialized)
			require.Equal(t, tt.want, got)
		})
	}
}

// ---------------------------------------------------------------------------
// ParseMaterializedFromSDL
// ---------------------------------------------------------------------------

func TestParseMaterializedFromSDL(t *testing.T) {
	tests := []struct {
		name string
		sdl  string
		want bool
	}{
		{
			name: "materialized true",
			sdl:  sdlFooMatTrue,
			want: true,
		},
		{
			name: "materialized false",
			sdl:  sdlFooMaterialized,
			want: false,
		},
		{
			name: "no directive returns false",
			sdl:  sdlFooBase,
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ss := NewSchemaService()
			got := ss.ParseMaterializedFromSDL(tt.sdl)
			require.Equal(t, tt.want, got)
		})
	}
}

// ---------------------------------------------------------------------------
// addFieldIfMissing
// ---------------------------------------------------------------------------

func TestEnsureMaterializedDirective_ExistingDirectiveWithoutParens(t *testing.T) {
	ss := NewSchemaService()
	// @materialized without parentheses - the regex should still match
	got := ss.ensureMaterializedDirective("type Foo @materialized {\n  id: ID\n}", true)
	require.Contains(t, got, "@materialized(if: true)")
}

func TestNormalizeSDL_MultipleRequiredFields(t *testing.T) {
	ss := NewSchemaService()
	sdl := sdlFooBase
	opts := SDLOptions{
		Materialized: false,
		RequiredFields: []FieldDef{
			{Name: testFieldCreatedAt, Type: gqlScalarString},
			{Name: "blockNumber", Type: gqlScalarInt},
		},
	}
	got := ss.NormalizeSDL(sdl, "", opts)
	require.Contains(t, got, "createdAt: String")
	require.Contains(t, got, "blockNumber: Int")
	require.Contains(t, got, "@materialized(if: false)")
}

func TestNormalizeSDL_RequiredFieldAlreadyExists(t *testing.T) {
	ss := NewSchemaService()
	sdl := "type Foo {\n  blockNumber: Int\n}"
	opts := SDLOptions{
		Materialized: false,
		RequiredFields: []FieldDef{
			{Name: "blockNumber", Type: gqlScalarInt},
		},
	}
	got := ss.NormalizeSDL(sdl, "", opts)
	// blockNumber already exists, should not be duplicated
	require.Equal(t, 1, strings.Count(got, "blockNumber"))
}

func TestAddFieldIfMissing(t *testing.T) {
	tests := []struct {
		name      string
		sdl       string
		fieldName string
		fieldType string
		want      string
	}{
		{
			name:      "field missing is added before last }",
			sdl:       sdlFooBase,
			fieldName: testFieldCreatedAt,
			fieldType: gqlScalarString,
			want:      sdlFooBaseWithCreatedAtStr,
		},
		{
			name:      "field already present is unchanged",
			sdl:       sdlFooBaseWithCreatedAtStr,
			fieldName: testFieldCreatedAt,
			fieldType: gqlScalarString,
			want:      sdlFooBaseWithCreatedAtStr,
		},
		{
			name:      "no closing brace returns unchanged",
			sdl:       gqlScalarDateTime,
			fieldName: testFieldCreatedAt,
			fieldType: gqlScalarString,
			want:      gqlScalarDateTime,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ss := NewSchemaService()
			got := ss.addFieldIfMissing(tt.sdl, tt.fieldName, tt.fieldType)
			require.Equal(t, tt.want, got)
		})
	}
}
