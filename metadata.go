package lakehouse

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
)

// 存储目录布局：
//
//	_root/
//	  metadata/
//	    latest.json          当前最新快照指针（含校验和）
//	    snap-0000000001.json 不可变快照元数据（清单 + 结构 + 父版本）
//	  data/
//	    <version>-<seq>.jsonl  不可变数据文件
//	  txn/                     临时目录：提交中断后遗留的未完成文件

const (
	dirMetadata = "metadata"
	dirData     = "data"
	dirTxn      = "txn"
	latestFile  = "latest.json"
)

// ManifestEntry 描述快照可见的一个数据文件。
type ManifestEntry struct {
	Path string `json:"path"`
	// MinPK/MaxPK 便于解释与调试；主键按字符串序（JSON key）比较。
	MinPK string `json:"min_pk"`
	MaxPK string `json:"max_pk"`
	Rows  int    `json:"rows"` // 文件内记录条数（含墓碑）
}

// Snapshot 是不可变表快照的完整元数据。
type Snapshot struct {
	Version    int64           `json:"version"`
	Parent     int64           `json:"parent"` // 0 表示初始快照
	Schema     *Schema         `json:"schema"` // 全量结构，历史快照永远自带当时结构
	Files      []ManifestEntry `json:"files"`
	Operation  string          `json:"operation"` // append/upsert/delete/evolve/compact
	CommitTime string          `json:"commit_time"`
}

// Latest 指针：原子 rename 落地，带快照文件校验和以发现损坏。
type Latest struct {
	Version  int64  `json:"version"`
	Path     string `json:"path"`
	Checksum string `json:"checksum"`
}

// Record 是数据文件中的一行物理记录：按列 ID 存储，避免改名/重加列串列。
type Record struct {
	Values map[int64]any `json:"v"`
	Tomb   bool          `json:"t,omitempty"`
}

func snapPath(root string, version int64) string {
	return filepath.Join(root, dirMetadata, fmt.Sprintf("snap-%010d.json", version))
}

func latestPath(root string) string {
	return filepath.Join(root, dirMetadata, latestFile)
}

// checksumFile 返回文件内容 sha256。
func checksumFile(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func writeJSONFileAtomic(finalPath string, v any) error {
	if err := os.MkdirAll(filepath.Dir(finalPath), 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	tmp := finalPath + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, finalPath)
}

func readJSONFile(path string, v any) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(data, v); err != nil {
		return fmt.Errorf("%w: %v: %s", ErrTableCorrupted, err, path)
	}
	return nil
}

// loadSnapshot 读取并校验快照。
func loadSnapshot(root string, version int64) (*Snapshot, error) {
	path := snapPath(root, version)
	if _, err := checksumFile(path); err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("%w: version %d", ErrSnapshotNotFound, version)
		}
		return nil, err
	}
	s := &Snapshot{}
	if err := readJSONFile(path, s); err != nil {
		return nil, err
	}
	return s, nil
}

// loadLatest 读取最新指针，并校验其指向的快照文件。
func loadLatest(root string) (*Latest, *Snapshot, error) {
	var ptr Latest
	if err := readJSONFile(latestPath(root), &ptr); err != nil {
		return nil, nil, err
	}
	s, err := loadSnapshot(root, ptr.Version)
	if err != nil {
		return nil, nil, err
	}
	sum, err := checksumFile(snapPath(root, ptr.Version))
	if err != nil {
		return nil, nil, err
	}
	if ptr.Checksum != sum {
		return nil, nil, fmt.Errorf("%w: latest pointer checksum mismatch for version %d", ErrTableCorrupted, ptr.Version)
	}
	return &ptr, s, nil
}

// sortManifest 保证清单按路径确定排序，输出可解释。
func sortManifest(files []ManifestEntry) {
	sort.Slice(files, func(i, j int) bool { return files[i].Path < files[j].Path })
}

// fsyncDir 让 rename/新建文件在目录项层面落盘（崩溃恢复关键）。
func fsyncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	err = f.Sync()
	f.Close()
	return err
}
