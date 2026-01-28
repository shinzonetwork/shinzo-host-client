package graphql

import (
	"fmt"
	"strings"
)

// WithReturnDocIdAndVersion adds _docID and _version fields to a GraphQL query
// It ensures that _docID and _version are returned for all documents and nested collections
// without duplicating these fields
func WithReturnDocIdAndVersion(query string) (string, error) {
	if query == "" {
		return "", fmt.Errorf("query cannot be empty")
	}

	// Parse the query to find all selection sets and add _docID and _version where needed
	trimmed := strings.TrimSpace(query)
	if trimmed == "" {
		return "", fmt.Errorf("query cannot be empty")
	}

	return addDocIdAndVersionToSelectionSets(trimmed)
}

// addDocIdAndVersionToSelectionSets adds _docID and _version to all selection sets that don't already have them
func addDocIdAndVersionToSelectionSets(query string) (string, error) {
	result := query

	// Find the main selection set by looking for CollectionName { ... } or CollectionName(args) { ... }
	// We need to find the opening brace that starts the main selection set, not filter/order arguments

	// First, find all opening braces and their positions
	var bracePositions []int
	for i, char := range result {
		if char == '{' {
			bracePositions = append(bracePositions, i)
		}
	}

	if len(bracePositions) == 0 {
		// No braces found, return as-is
		return result, nil
	}

	// Find the main selection set brace (the one not wrapped in parentheses)
	var mainBracePos int = -1

	for _, bracePos := range bracePositions {
		// Check if this brace is wrapped in parentheses
		beforeBrace := result[:bracePos]

		// Count parentheses to see if we're inside an argument list
		parenCount := 0
		for _, char := range beforeBrace {
			if char == '(' {
				parenCount++
			} else if char == ')' {
				parenCount--
			}
		}

		// If parenCount is 0, this brace is not inside parentheses (it's the main selection set)
		if parenCount == 0 {
			mainBracePos = bracePos
			break
		}
	}

	if mainBracePos == -1 {
		// No main selection set found, return as-is
		return result, nil
	}

	closeBracePos := findMatchingCloseBrace(result, mainBracePos)
	if closeBracePos == -1 {
		// No matching closing brace found, return as-is
		return result, nil
	}

	// Extract the selection set content (between the braces)
	contentStart := mainBracePos + 1
	contentEnd := closeBracePos
	content := result[contentStart:contentEnd]

	// Check if _docID and _version are already present
	hasDocID := strings.Contains(content, "_docID")
	hasVersion := strings.Contains(content, "_version")

	// Check if _version is complete (has the full nested structure)
	completeVersionPattern := "_version { cid signature { type identity value } }"
	hasCompleteVersion := strings.Contains(content, completeVersionPattern)

	// If both are already present and complete, skip
	if hasDocID && hasCompleteVersion {
		return result, nil
	}

	// Determine what needs to be added
	needDocID := !hasDocID
	needVersion := !hasCompleteVersion // Need version if it's missing or incomplete

	// Build new content: always put _docID first, then _version, then everything else
	trimmedContent := strings.TrimSpace(content)

	var newContentBuilder strings.Builder

	// Start with _docID if needed
	if needDocID {
		newContentBuilder.WriteString("_docID")
	} else if hasDocID {
		// Extract existing _docID from content - just write "_docID" plus space
		// We'll handle it in the right position
		newContentBuilder.WriteString("_docID")
	}

	// Always add a space after _docID if we have it
	if needDocID || hasDocID {
		newContentBuilder.WriteString(" ")
	}

	// Add _version if needed
	if needVersion {
		newContentBuilder.WriteString("_version { cid signature { type identity value } }")
	}

	// Add the rest of the content (excluding _docID and _version from the main selection set if they were already there)
	remainingContent := ""
	hasRemainingContent := false
	if trimmedContent != "" {
		remainingContent = trimmedContent

		// If _docID already existed in the top-level, remove it from remainingContent
		// Only remove from the beginning (top-level), not nested selection sets
		if hasDocID && !needDocID {
			remainingContent = strings.TrimSpace(remainingContent)
			// Remove _docID from the start
			if strings.HasPrefix(remainingContent, "_docID ") {
				remainingContent = strings.TrimSpace(remainingContent[len("_docID "):])
			} else if remainingContent == "_docID" {
				remainingContent = ""
			} else if strings.HasPrefix(remainingContent, "_docID") {
				remainingContent = strings.TrimSpace(remainingContent[len("_docID"):])
			}
		}

		// If _version already existed in the top-level, remove it (complete or incomplete) if we need to add/complete it
		// This handles both complete and incomplete _version fields
		if hasVersion && needVersion {
			remainingContent = strings.TrimSpace(remainingContent)

			// Try to remove complete version first
			completePattern := "_version { cid signature { type identity value } }"
			if strings.HasPrefix(remainingContent, completePattern+" ") {
				remainingContent = strings.TrimSpace(remainingContent[len(completePattern+" "):])
			} else if remainingContent == completePattern {
				remainingContent = ""
			} else if strings.HasPrefix(remainingContent, completePattern) {
				remainingContent = strings.TrimSpace(remainingContent[len(completePattern):])
			} else {
				// Try to find and remove incomplete _version field
				// Look for "_version" followed by optional nested structure
				versionRemoved := false
				versionStart := strings.Index(remainingContent, "_version")
				if versionStart != -1 {
					// Find the end of the _version field (could be just "_version", "_version { ... }", etc.)
					afterVersion := versionStart + len("_version")

					// Skip whitespace
					for afterVersion < len(remainingContent) && (remainingContent[afterVersion] == ' ' || remainingContent[afterVersion] == '\n' || remainingContent[afterVersion] == '\t') {
						afterVersion++
					}

					if afterVersion < len(remainingContent) && remainingContent[afterVersion] == '{' {
						// Find the matching closing brace
						openBrace := afterVersion
						braceCount := 1
						closeBrace := -1
						for i := openBrace + 1; i < len(remainingContent); i++ {
							if remainingContent[i] == '{' {
								braceCount++
							} else if remainingContent[i] == '}' {
								braceCount--
								if braceCount == 0 {
									closeBrace = i
									break
								}
							}
						}
						if closeBrace != -1 {
							// Remove _version and its nested structure
							beforeVersion := remainingContent[:versionStart]
							afterVersionField := ""
							if closeBrace+1 < len(remainingContent) {
								afterVersionField = remainingContent[closeBrace+1:]
							}
							remainingContent = strings.TrimSpace(beforeVersion + " " + afterVersionField)
							remainingContent = strings.TrimSpace(remainingContent)
							versionRemoved = true
						}
					} else {
						// Just "_version" with no nested structure
						beforeVersion := remainingContent[:versionStart]
						afterVersion := remainingContent[afterVersion:]
						remainingContent = strings.TrimSpace(beforeVersion + " " + afterVersion)
						remainingContent = strings.TrimSpace(remainingContent)
						versionRemoved = true
					}
				}

				// Fallback: try simple pattern matching
				if !versionRemoved {
					// Try to find complete version with space before it
					if idx := strings.Index(remainingContent, " "+completePattern+" "); idx != -1 {
						remainingContent = remainingContent[:idx] + remainingContent[idx+len(" "+completePattern+" "):]
						remainingContent = strings.TrimSpace(remainingContent)
					} else if idx := strings.Index(remainingContent, " "+completePattern); idx != -1 {
						remainingContent = remainingContent[:idx] + remainingContent[idx+len(" "+completePattern):]
						remainingContent = strings.TrimSpace(remainingContent)
					}
				}
			}
		}

		hasRemainingContent = (remainingContent != "")
	}

	// Add space after _version if it was added and there's remaining content, or if _version exists and we're adding _docID
	if (needVersion && hasRemainingContent) || (hasVersion && needDocID) {
		newContentBuilder.WriteString(" ")
	}

	// Add remaining content if any
	if hasRemainingContent {
		newContentBuilder.WriteString(remainingContent)
	}

	newContent := newContentBuilder.String()

	// Replace the selection set content
	result = result[:contentStart] + newContent + result[contentEnd:]

	return result, nil
}

// findMatchingCloseBrace finds the matching closing brace for an opening brace
func findMatchingCloseBrace(query string, openBracePos int) int {
	if openBracePos >= len(query) || query[openBracePos] != '{' {
		return -1
	}

	braceCount := 1
	for i := openBracePos + 1; i < len(query); i++ {
		switch query[i] {
		case '{':
			braceCount++
		case '}':
			braceCount--
			if braceCount == 0 {
				return i
			}
		}
	}

	return -1 // No matching closing brace found
}
