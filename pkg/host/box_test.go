package host

import (
	"strings"
	"testing"
	"unicode/utf8"
)

func TestRenderBox_AllLinesShareTheSameWidth(t *testing.T) {
	box := renderBox("Routes", [][2]string{
		{"health", "http://localhost:8080/health"},
		{"metrics", "http://localhost:8080/metrics"},
		{"playground", "http://localhost:8080/playground"},
	})

	widths := map[int]bool{}
	for _, line := range strings.Split(box, "\n") {
		widths[utf8.RuneCountInString(line)] = true
	}
	if len(widths) != 1 {
		t.Fatalf("expected every line to share one width, got widths: %v\n%s", widths, box)
	}
}

func TestRenderBox_TruncatesLongValuesButKeepsAlignment(t *testing.T) {
	longValue := strings.Repeat("a", boxMaxValueWidth*2)
	box := renderBox("Identity", [][2]string{
		{"short", "x"},
		{"long", longValue},
	})

	widths := map[int]bool{}
	for _, line := range strings.Split(box, "\n") {
		widths[utf8.RuneCountInString(line)] = true
	}
	if len(widths) != 1 {
		t.Fatalf("expected every line to share one width after truncation, got widths: %v\n%s", widths, box)
	}
	if strings.Contains(box, longValue) {
		t.Fatal("expected the oversized value to be truncated, found it in full")
	}
	if !strings.Contains(box, "…") {
		t.Fatal("expected a truncation marker in the box")
	}
}

func TestRenderBox_ContainsTitleAndAllRows(t *testing.T) {
	box := renderBox("Identity", [][2]string{
		{"shinzo", "shinzo1abc"},
		{"evm", "0xdeadbeef"},
	})

	for _, want := range []string{"Identity", "shinzo", "shinzo1abc", "evm", "0xdeadbeef"} {
		if !strings.Contains(box, want) {
			t.Fatalf("expected box to contain %q, got:\n%s", want, box)
		}
	}
}

func TestRenderBox_EmptyRowsStillRenders(t *testing.T) {
	box := renderBox("Empty", nil)
	if !strings.Contains(box, "Empty") {
		t.Fatalf("expected the title to still render with no rows, got:\n%s", box)
	}
}

func TestTruncateValue_LeavesShortValuesUntouched(t *testing.T) {
	short := "not too long"
	if got := truncateValue(short); got != short {
		t.Fatalf("expected short values to pass through unchanged, got %q", got)
	}
}
