package pathutil

import (
	"os"
	"path/filepath"
)

const (
	DirPerm = 0700
)

func VarDir() string {
	if dir := os.Getenv("BLOBFS_VAR_DIR"); dir != "" {
		return dir
	}
	return filepath.Join(os.Getenv("HOME"), "var", "blobfs")
}

func InitVarDir() error {
	path := VarDir()
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return os.MkdirAll(path, DirPerm)
	}
	return nil
}
