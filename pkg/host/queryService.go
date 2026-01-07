package host

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/shinzonetwork/shinzo-host-client/pkg/constants"
)

// QueryService provides dynamic query building capabilities for different collection types
type QueryService struct {
	// Cache of field mappings for each collection type
	fieldCache map[string][]string
}

// NewQueryService creates a new instance of QueryService
func NewQueryService() *QueryService {
	qs := &QueryService{
		fieldCache: make(map[string][]string),
	}
	qs.initializeFieldMappings()
	return qs
}

// CollectionType represents the different attestation collection types
type CollectionType string

const (
	CollectionBlock           CollectionType = "Block"
	CollectionTransaction     CollectionType = "Transaction"
	CollectionLog             CollectionType = "Log"
	CollectionAccessListEntry CollectionType = "AccessListEntry"
)

// initializeFieldMappings extracts field names from attestation types using reflection
func (qs *QueryService) initializeFieldMappings() {
	// Block fields
	blockFields := qs.extractFields(constants.Block{})
	qs.fieldCache[string(CollectionBlock)] = blockFields

	// Transaction fields
	transactionFields := qs.extractFields(constants.Transaction{})
	qs.fieldCache[string(CollectionTransaction)] = transactionFields

	// Log fields
	logFields := qs.extractFields(constants.Log{})
	qs.fieldCache[string(CollectionLog)] = logFields

	// AccessListEntry fields
	accessListFields := qs.extractFields(constants.AccessListEntry{})
	qs.fieldCache[string(CollectionAccessListEntry)] = accessListFields
}

// extractFields uses reflection to extract field names from a struct
func (qs *QueryService) extractFields(model interface{}) []string {
	t := reflect.TypeOf(model)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	var fields []string
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		jsonTag := field.Tag.Get("json")
		if jsonTag != "" && jsonTag != "-" {
			// Handle nested structs by including their fields too
			if field.Type.Kind() == reflect.Slice {
				// For slices, get the element type
				elemType := field.Type.Elem()
				if elemType.Kind() == reflect.Struct {
					nestedFields := qs.extractNestedFields(elemType)
					fields = append(fields, nestedFields...)
				}
			} else if field.Type.Kind() == reflect.Struct {
				// For direct nested structs
				nestedFields := qs.extractNestedFields(field.Type)
				fields = append(fields, nestedFields...)
			}
			fields = append(fields, jsonTag)
		}
	}

	return fields
}

// extractNestedFields extracts fields from nested struct types
func (qs *QueryService) extractNestedFields(t reflect.Type) []string {
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	var fields []string
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		jsonTag := field.Tag.Get("json")
		if jsonTag != "" && jsonTag != "-" {
			fields = append(fields, jsonTag)
		}
	}

	return fields
}

// GetFieldsForCollection returns all available fields for a given collection type
func (qs *QueryService) GetFieldsForCollection(collectionType CollectionType) []string {
	fields, exists := qs.fieldCache[string(collectionType)]
	if !exists {
		return []string{"_docID", "_version"} // fallback to system fields
	}

	// Always include system fields
	return append([]string{"_docID", "_version"}, fields...)
}

// BuildDynamicQuery creates a query for the specified collection type with optional filters
func (qs *QueryService) BuildDynamicQuery(collectionType CollectionType, filters map[string]interface{}) string {
	fields := qs.GetFieldsForCollection(collectionType)
	fieldsStr := strings.Join(fields, " ")

	// Build collection name based on type
	collectionName := qs.getCollectionName(collectionType)

	if len(filters) == 0 {
		// No filters - return all fields
		return fmt.Sprintf("%s { %s }", collectionName, fieldsStr)
	}

	// Build filter string
	filterStr := qs.buildFilterString(filters)

	return fmt.Sprintf("%s(filter: %s) { %s }", collectionName, filterStr, fieldsStr)
}

// BuildNestedQuery creates a query that includes nested relationships
func (qs *QueryService) BuildNestedQuery(primaryCollection CollectionType, nestedCollections []CollectionType, filters map[string]interface{}) string {
	// Start with primary collection
	query := qs.BuildDynamicQuery(primaryCollection, filters)

	// Add nested collections if specified
	for _, nestedType := range nestedCollections {
		nestedFields := qs.GetFieldsForCollection(nestedType)
		nestedFieldsStr := strings.Join(nestedFields, " ")
		nestedName := qs.getCollectionName(nestedType)

		// Add nested collection to query
		query = strings.Replace(query, "}", fmt.Sprintf(" %s { %s } }", nestedName, nestedFieldsStr), 1)
	}

	return query
}

// getCollectionName maps collection types to their actual DefraDB collection names
func (qs *QueryService) getCollectionName(collectionType CollectionType) string {
	switch collectionType {
	case CollectionBlock:
		return "Ethereum__Mainnet__Block"
	case CollectionTransaction:
		return "Ethereum__Mainnet__Transaction"
	case CollectionLog:
		return "Ethereum__Mainnet__Log"
	case CollectionAccessListEntry:
		return "Ethereum__Mainnet__AccessListEntry"
	default:
		return string(collectionType)
	}
}

// buildFilterString converts a filter map to GraphQL filter syntax
func (qs *QueryService) buildFilterString(filters map[string]interface{}) string {
	if len(filters) == 0 {
		return ""
	}

	var filterPairs []string
	for field, value := range filters {
		switch v := value.(type) {
		case string:
			filterPairs = append(filterPairs, fmt.Sprintf("%s: {_eq: \"%s\"}", field, v))
		case int, int64, uint64:
			filterPairs = append(filterPairs, fmt.Sprintf("%s: {_eq: %d}", field, v))
		case float64:
			filterPairs = append(filterPairs, fmt.Sprintf("%s: {_eq: %f}", field, v))
		case bool:
			filterPairs = append(filterPairs, fmt.Sprintf("%s: {_eq: %t}", field, v))
		default:
			// Fallback to string representation
			filterPairs = append(filterPairs, fmt.Sprintf("%s: {_eq: \"%v\"}", field, v))
		}
	}

	return fmt.Sprintf("{ %s }", strings.Join(filterPairs, " "))
}

// GetCollectionHierarchy returns the natural nesting relationships between collections
func (qs *QueryService) GetCollectionHierarchy() map[CollectionType][]CollectionType {
	return map[CollectionType][]CollectionType{
		CollectionBlock:           {CollectionTransaction},
		CollectionTransaction:     {CollectionLog, CollectionAccessListEntry},
		CollectionLog:             {},
		CollectionAccessListEntry: {},
	}
}

// BuildHierarchicalQuery creates a query that respects the natural hierarchy of collections
func (qs *QueryService) BuildHierarchicalQuery(rootCollection CollectionType, filters map[string]interface{}) string {
	hierarchy := qs.GetCollectionHierarchy()

	// Start with root collection (without nested collections first)
	rootFields := qs.GetFieldsForCollection(rootCollection)
	rootFieldsStr := strings.Join(rootFields, " ")
	collectionName := qs.getCollectionName(rootCollection)

	var query string
	if len(filters) == 0 {
		// No filters - just return all fields
		query = fmt.Sprintf("%s { %s }", collectionName, rootFieldsStr)
	} else {
		// Build filter string
		filterStr := qs.buildFilterString(filters)
		query = fmt.Sprintf("%s(filter: %s) { %s }", collectionName, filterStr, rootFieldsStr)
	}

	// Add nested collections as separate fields outside the filter
	// We need to insert them before the closing brace of the root collection
	if nested, exists := hierarchy[rootCollection]; exists {
		for _, nestedType := range nested {
			nestedFields := qs.GetFieldsForCollection(nestedType)
			nestedFieldsStr := strings.Join(nestedFields, " ")
			nestedName := qs.getCollectionName(nestedType)

			// Add nested collection as a separate field, not inside the filter
			// Insert before the final closing brace
			query = strings.TrimSuffix(query, " }")
			query = fmt.Sprintf("%s %s { %s } }", query, nestedName, nestedFieldsStr)
		}
	}

	return query
}
