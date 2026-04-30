package view

import (
	"fmt"
	"regexp"
	"strings"
)

// SchemaService provides utilities for normalizing and parsing SDL schemas for Shinzo views.
type SchemaService struct{}

// NewSchemaService creates a new instance of SchemaService.
func NewSchemaService() *SchemaService {
	return &SchemaService{}
}

// NormalizeSDL applies standard transformations to an SDL schema.
func (ss *SchemaService) NormalizeSDL(sdl string, typeName string, options SDLOptions) string {
	// Replace type name if provided
	if typeName != "" {
		re := regexp.MustCompile(`type\s+(\w+)\s*`)
		sdl = re.ReplaceAllString(sdl, "type "+typeName+" ")
	}

	// Handle @materialized directive
	sdl = ss.ensureMaterializedDirective(sdl, options.Materialized)

	// Add required fields
	for _, field := range options.RequiredFields {
		sdl = ss.addFieldIfMissing(sdl, field.Name, field.Type)
	}

	return sdl
}

// SDLOptions defines options for SDLs within views.
type SDLOptions struct {
	Materialized   bool
	RequiredFields []FieldDef
}

// FieldDef represents a field definition with a name and type.
type FieldDef struct {
	Name string
	Type string
}

func (ss *SchemaService) ensureMaterializedDirective(sdl string, materialized bool) string {
	directive := fmt.Sprintf("@materialized(if: %t)", materialized)

	if strings.Contains(sdl, "@materialized") {
		re := regexp.MustCompile(`@materialized\s*(\([^)]*\))?`)
		return re.ReplaceAllString(sdl, directive)
	}

	re := regexp.MustCompile(`(type\s+\w+\s*)(\{)`)
	if re.MatchString(sdl) {
		return re.ReplaceAllString(sdl, "${1}"+directive+" $2")
	}
	return sdl
}

// ParseMaterializedFromSDL extracts the materialized value from an SDL's @materialized directive.
func (ss *SchemaService) ParseMaterializedFromSDL(sdl string) bool {
	re := regexp.MustCompile(`@materialized\s*\(\s*if\s*:\s*(true|false)\s*\)`)
	match := re.FindStringSubmatch(sdl)
	if len(match) > 1 {
		return match[1] == "true"
	}
	return false
}

func (ss *SchemaService) addFieldIfMissing(sdl, fieldName, fieldType string) string {
	if strings.Contains(sdl, fieldName) {
		return sdl
	}

	idx := strings.LastIndex(sdl, "}")
	if idx == -1 {
		return sdl
	}
	return sdl[:idx] + "  " + fieldName + ": " + fieldType + "\n" + sdl[idx:]
}
