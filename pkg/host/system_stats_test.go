package host

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDirSize_SumsFileSizes(t *testing.T) {
	dir := t.TempDir()
	writeSizedFile(t, filepath.Join(dir, "a.txt"), 100)
	writeSizedFile(t, filepath.Join(dir, "sub", "b.txt"), 250)

	if got, want := dirSize(dir), uint64(350); got != want {
		t.Fatalf("dirSize = %d, want %d", got, want)
	}
}

func TestDirSize_MissingPath(t *testing.T) {
	if got := dirSize(filepath.Join(t.TempDir(), "does-not-exist")); got != 0 {
		t.Fatalf("expected 0 for a missing path, got %d", got)
	}
}

func TestProcessRSS_ReportsNonZero(t *testing.T) {
	if got := processRSS(); got == 0 {
		t.Fatal("expected a nonzero RSS for the running test process")
	}
}

func TestCollectSystemStats(t *testing.T) {
	dir := t.TempDir()
	writeSizedFile(t, filepath.Join(dir, "data.bin"), 4096)

	stats, err := collectSystemStats(dir)
	if err != nil {
		t.Fatalf("collectSystemStats: %v", err)
	}

	if stats.Disk.Path != dir {
		t.Fatalf("expected disk path %q, got %q", dir, stats.Disk.Path)
	}
	if stats.Disk.TotalBytes == 0 {
		t.Fatal("expected a nonzero total disk size")
	}
	if stats.Disk.UsedBytes > stats.Disk.TotalBytes {
		t.Fatalf("used (%d) exceeds total (%d)", stats.Disk.UsedBytes, stats.Disk.TotalBytes)
	}
	if stats.Disk.ShinzoBytes != 4096 {
		t.Fatalf("expected shinzo_bytes 4096, got %d", stats.Disk.ShinzoBytes)
	}

	if stats.Memory.TotalBytes == 0 {
		t.Fatal("expected a nonzero total memory size")
	}
	if stats.Memory.ShinzoBytes == 0 {
		t.Fatal("expected a nonzero process RSS")
	}
}

func TestCollectSystemStats_MissingPath(t *testing.T) {
	if _, err := collectSystemStats(filepath.Join(t.TempDir(), "does-not-exist")); err == nil {
		t.Fatal("expected an error for a disk path that doesn't exist")
	}
}

func writeSizedFile(t *testing.T, path string, size int) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.WriteFile(path, make([]byte, size), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
}
