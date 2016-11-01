package blobstore

import (
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/tsileo/blobstash/pkg/config/pathutil"
)

type BlobStore struct {
	path string
	fs   string
}

func New(path, fsName string) (*BlobStore, error) {
	bs := &BlobStore{path: path, fs: fsName}
	if err := os.MkdirAll(filepath.Join(pathutil.VarDir(), "blobfs", "blobstore", fsName), 0700); err != nil {
		return nil, err
	}

	return bs, nil
}

func (bs *BlobStore) Close() error {
	return nil
}

func (bs *BlobStore) Iter(walkFunc filepath.WalkFunc) error {
	return filepath.Walk(filepath.Join(pathutil.VarDir(), "blobfs", "blobstore", bs.fs), walkFunc)
}

func (bs *BlobStore) Put(hash string, data []byte) error {
	if err := os.MkdirAll(filepath.Join(pathutil.VarDir(), "blobfs", "blobstore", bs.fs, hash[0:2]), 0700); err != nil {
		return err
	}
	return ioutil.WriteFile(filepath.Join(pathutil.VarDir(), "blobfs", "blobstore", bs.fs, hash[0:2], hash), data, 0644)
}

func (bs *BlobStore) Get(hash string) ([]byte, error) {
	return ioutil.ReadFile(filepath.Join(pathutil.VarDir(), "blobfs", "blobstore", bs.fs, hash[0:2], hash))
}

func (bs *BlobStore) Remove(hash string) error {
	return os.Remove(filepath.Join(pathutil.VarDir(), "blobfs", "blobstore", bs.fs, hash[0:2], hash))
}

func (bs *BlobStore) Stat(hash string) (bool, error) {
	f, err := os.Open(filepath.Join(pathutil.VarDir(), "blobfs", "blobstore", bs.fs, hash[0:2], hash))
	defer f.Close()
	switch {
	case err == nil:
		return true, nil
	case os.IsNotExist(err):
		return false, nil
	default:
		return false, err
	}
}
