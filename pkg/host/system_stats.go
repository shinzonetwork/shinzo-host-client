package host

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/shirou/gopsutil/v4/disk"
	"github.com/shirou/gopsutil/v4/mem"
	"github.com/shirou/gopsutil/v4/process"
)

type SystemStats struct {
	Disk   DiskStats   `json:"disk"`
	Memory MemoryStats `json:"memory"`
}

type DiskStats struct {
	Path        string `json:"path"`
	TotalBytes  uint64 `json:"total_bytes"`
	FreeBytes   uint64 `json:"free_bytes"`
	UsedBytes   uint64 `json:"used_bytes"`
	ShinzoBytes uint64 `json:"shinzo_bytes"`
}

type MemoryStats struct {
	TotalBytes  uint64 `json:"total_bytes"`
	FreeBytes   uint64 `json:"free_bytes"`
	UsedBytes   uint64 `json:"used_bytes"`
	ShinzoBytes uint64 `json:"shinzo_bytes"`
}

func collectSystemStats(storePath string) (SystemStats, error) {
	var stats SystemStats

	diskUsage, err := disk.Usage(storePath)
	if err != nil {
		return stats, fmt.Errorf("disk usage: %w", err)
	}
	stats.Disk = DiskStats{
		Path:        storePath,
		TotalBytes:  diskUsage.Total,
		FreeBytes:   diskUsage.Free,
		UsedBytes:   diskUsage.Used,
		ShinzoBytes: dirSize(storePath),
	}

	vmem, err := mem.VirtualMemory()
	if err != nil {
		return stats, fmt.Errorf("memory usage: %w", err)
	}
	stats.Memory = MemoryStats{
		TotalBytes:  vmem.Total,
		FreeBytes:   vmem.Available,
		UsedBytes:   vmem.Used,
		ShinzoBytes: processRSS(),
	}

	return stats, nil
}

func dirSize(path string) uint64 {
	var total uint64
	_ = filepath.WalkDir(path, func(_ string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil //nolint:nilerr // skip and keep walking
		}
		info, err := d.Info()
		if err != nil {
			return nil //nolint:nilerr // skip and keep walking
		}
		total += uint64(info.Size()) //nolint:gosec // file sizes are never negative
		return nil
	})
	return total
}

func processRSS() uint64 {
	proc, err := process.NewProcess(int32(os.Getpid())) //nolint:gosec // pid fits in int32
	if err != nil {
		return 0
	}
	info, err := proc.MemoryInfo()
	if err != nil || info == nil {
		return 0
	}
	return info.RSS
}
