package blobstore

import (
	"errors"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/tsileo/blobstash/pkg/config/pathutil"
)

var ErrBlobNotFound = errors.New("Blob not found")

type BlobStore struct {
	path string
	fs   string
}

func New(path, fsName string) (*BlobStore, error) {
	if path == "" {
		path = filepath.Join(pathutil.VarDir(), "blobfs", "blobstore")
	}
	bs := &BlobStore{path: path, fs: fsName}
	if err := os.MkdirAll(filepath.Join(path, fsName), 0700); err != nil {
		return nil, err
	}

	return bs, nil
}

func (bs *BlobStore) Destroy() error {
	return os.RemoveAll(bs.path)
}

func (bs *BlobStore) Close() error {
	return nil
}

func (bs *BlobStore) iter(wfunc func(string, string, error) error) func(path string, fi os.FileInfo, err error) error {
	return func(path string, fi os.FileInfo, err error) error {
		if len(fi.Name()) == 64 { // The name looks like a hash it must be blob
			return wfunc(path, fi.Name(), err)
		}
		return nil
	}
}

func (bs *BlobStore) Iter(walkFunc func(string, string, error) error) error {
	return filepath.Walk(filepath.Join(pathutil.VarDir(), "blobfs", "blobstore", bs.fs), bs.iter(walkFunc))
}

func (bs *BlobStore) Put(hash string, data []byte) error {
	if err := os.MkdirAll(filepath.Join(pathutil.VarDir(), "blobfs", "blobstore", bs.fs, hash[0:2]), 0700); err != nil {
		return err
	}
	return ioutil.WriteFile(filepath.Join(pathutil.VarDir(), "blobfs", "blobstore", bs.fs, hash[0:2], hash), data, 0644)
}

func (bs *BlobStore) Get(hash string) ([]byte, error) {
	blob, err := ioutil.ReadFile(filepath.Join(pathutil.VarDir(), "blobfs", "blobstore", bs.fs, hash[0:2], hash))
	switch {
	case err == nil:
		return blob, nil
	case os.IsNotExist(err):
		return nil, ErrBlobNotFound
	default:
		return nil, err
	}

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
