package graphql

import (
	"strings"
)

// WithReturnDocIDAndVersion adds _docID and _version fields to a GraphQL query
// It ensures that _docID and _version are returned for all documents and nested collections
// without duplicating these fields.
func WithReturnDocIDAndVersion(query string) (string, error) {
	if query == "" {
		return "", ErrQueryEmpty
	}

	// Parse the query to find all selection sets and add _docID and _version where needed
	trimmed := strings.TrimSpace(query)
	if trimmed == "" {
		return "", ErrQueryEmpty
	}

	return addDocIDAndVersionToSelectionSets(trimmed)
}

// addDocIDAndVersionToSelectionSets adds _docID and _version to all selection sets that don't already have them.
func addDocIDAndVersionToSelectionSets(query string) (string, error) {
	mainBracePos := findMainBracePos(query)
	if mainBracePos == -1 {
		return query, nil
	}

	closeBracePos := findMatchingCloseBrace(query, mainBracePos)
	if closeBracePos == -1 {
		return query, nil
	}

	contentStart := mainBracePos + 1
	contentEnd := closeBracePos
	content := query[contentStart:contentEnd]

	const completeVersionPattern = "_version { cid signature { type identity value } }"
	hasDocID := strings.Contains(content, "_docID")
	hasCompleteVersion := strings.Contains(content, completeVersionPattern)

	if hasDocID && hasCompleteVersion {
		return query, nil
	}

	newContent := buildNewSelectionSetContent(content, hasDocID, hasCompleteVersion, completeVersionPattern)
	return query[:contentStart] + newContent + query[contentEnd:], nil
}

// findMainBracePos finds the position of the first opening brace not inside parentheses.
func findMainBracePos(query string) int {
	for i, char := range query {
		if char != '{' {
			continue
		}
		parenCount := 0
		for _, c := range query[:i] {
			switch c {
			case '(':
				parenCount++
			case ')':
				parenCount--
			}
		}
		if parenCount == 0 {
			return i
		}
	}
	return -1
}

// buildNewSelectionSetContent constructs the new selection set content with _docID and _version.
func buildNewSelectionSetContent(content string, hasDocID, hasCompleteVersion bool, completeVersionPattern string) string {
	needDocID := !hasDocID
	needVersion := !hasCompleteVersion
	hasVersion := strings.Contains(content, "_version")

	trimmedContent := strings.TrimSpace(content)
	var b strings.Builder

	if needDocID || hasDocID {
		b.WriteString("_docID ")
	}
	if needVersion {
		b.WriteString(completeVersionPattern)
	}

	remainingContent := trimmedContent
	if hasDocID && !needDocID {
		remainingContent = removeDocIDFromContent(remainingContent)
	}
	if hasVersion && needVersion {
		remainingContent = removeVersionFromContent(remainingContent, completeVersionPattern)
	}

	hasRemaining := remainingContent != ""
	if (needVersion && hasRemaining) || (hasVersion && needDocID) {
		b.WriteString(" ")
	}
	if hasRemaining {
		b.WriteString(remainingContent)
	}

	return b.String()
}

// removeDocIDFromContent removes _docID from the beginning of content.
func removeDocIDFromContent(content string) string {
	content = strings.TrimSpace(content)
	switch {
	case strings.HasPrefix(content, "_docID "):
		return strings.TrimSpace(content[len("_docID "):])
	case content == "_docID":
		return ""
	case strings.HasPrefix(content, "_docID"):
		return strings.TrimSpace(content[len("_docID"):])
	}
	return content
}

// removeVersionFromContent removes the _version field (complete or incomplete) from content.
func removeVersionFromContent(content, completePattern string) string {
	content = strings.TrimSpace(content)

	switch {
	case strings.HasPrefix(content, completePattern+" "):
		return strings.TrimSpace(content[len(completePattern+" "):])
	case content == completePattern:
		return ""
	case strings.HasPrefix(content, completePattern):
		return strings.TrimSpace(content[len(completePattern):])
	}

	if result, ok := removeIncompleteVersion(content); ok {
		return result
	}

	return removeFallbackVersion(content, completePattern)
}

// removeIncompleteVersion attempts to find and remove an incomplete _version field.
func removeIncompleteVersion(content string) (string, bool) {
	versionStart := strings.Index(content, "_version")
	if versionStart == -1 {
		return content, false
	}

	afterVersion := versionStart + len("_version")
	for afterVersion < len(content) && (content[afterVersion] == ' ' || content[afterVersion] == '\n' || content[afterVersion] == '\t') {
		afterVersion++
	}

	if afterVersion < len(content) && content[afterVersion] == '{' {
		return removeVersionWithBraces(content, versionStart, afterVersion)
	}

	beforeVersion := content[:versionStart]
	afterVersionStr := content[afterVersion:]
	return strings.TrimSpace(beforeVersion + " " + afterVersionStr), true
}

// removeVersionWithBraces removes a _version field that has a nested brace structure.
func removeVersionWithBraces(content string, versionStart, openBracePos int) (string, bool) {
	braceCount := 1
	closeBrace := -1
	for i := openBracePos + 1; i < len(content); i++ {
		switch content[i] {
		case '{':
			braceCount++
		case '}':
			braceCount--
			if braceCount == 0 {
				closeBrace = i
			}
		}
		if closeBrace != -1 {
			break
		}
	}

	if closeBrace == -1 {
		return content, false
	}

	beforeVersion := content[:versionStart]
	afterVersionField := ""
	if closeBrace+1 < len(content) {
		afterVersionField = content[closeBrace+1:]
	}
	return strings.TrimSpace(beforeVersion + " " + afterVersionField), true
}

// removeFallbackVersion tries simple index-based pattern removal.
func removeFallbackVersion(content, completePattern string) string {
	if idx := strings.Index(content, " "+completePattern+" "); idx != -1 {
		return strings.TrimSpace(content[:idx] + content[idx+len(" "+completePattern+" "):])
	}
	if idx := strings.Index(content, " "+completePattern); idx != -1 {
		return strings.TrimSpace(content[:idx] + content[idx+len(" "+completePattern):])
	}
	return content
}

// findMatchingCloseBrace finds the matching closing brace for an opening brace.
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
