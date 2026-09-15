package host

import (
	"strings"
	"unicode/utf8"
)

const boxMaxValueWidth = 60

func truncateValue(s string) string {
	if utf8.RuneCountInString(s) <= boxMaxValueWidth {
		return s
	}
	runes := []rune(s)
	return string(runes[:boxMaxValueWidth-1]) + "…"
}

func renderBox(title string, rows [][2]string) string {
	truncated := make([][2]string, len(rows))
	keyWidth := 0
	valWidth := 0
	for i, r := range rows {
		truncated[i] = [2]string{r[0], truncateValue(r[1])}
		keyWidth = max(keyWidth, utf8.RuneCountInString(truncated[i][0]))
		valWidth = max(valWidth, utf8.RuneCountInString(truncated[i][1]))
	}
	rows = truncated

	contentWidth := keyWidth + 2 + valWidth
	contentWidth = max(contentWidth, utf8.RuneCountInString(title)+2)

	var b strings.Builder

	b.WriteString("╭─ ")
	b.WriteString(title)
	b.WriteString(" ")
	b.WriteString(strings.Repeat("─", contentWidth-utf8.RuneCountInString(title)-1))
	b.WriteString("╮\n")

	for _, r := range rows {
		b.WriteString("│ ")
		b.WriteString(r[0])
		b.WriteString(strings.Repeat(" ", keyWidth-utf8.RuneCountInString(r[0])))
		b.WriteString("  ")
		b.WriteString(r[1])
		b.WriteString(strings.Repeat(" ", valWidth-utf8.RuneCountInString(r[1])))
		b.WriteString(" │\n")
	}

	b.WriteString("╰")
	b.WriteString(strings.Repeat("─", contentWidth+2))
	b.WriteString("╯")

	return b.String()
}
