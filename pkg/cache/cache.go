package cache

import (
	"os"
	"path/filepath"

	"golang.org/x/net/context"

	localblobstore "github.com/tsileo/blobfs/pkg/blobstore"
	"github.com/tsileo/blobfs/pkg/pathutil"

	"github.com/tsileo/blobstash/pkg/client/blobstore"
	"github.com/tsileo/blobstash/pkg/client/clientutil"
	"github.com/tsileo/blobstash/pkg/vkv"
)

// TODO(tsileo): add Clean/Reset/Remove methods

type Cache struct {
	lbs *localblobstore.BlobStore
	bs  *blobstore.BlobStore // Remote BlobStore client for BlobStash
	kv  *vkv.DB
	// TODO(tsileo): embed a kvstore too (but witouth sync/), may be make it optional?
}

// TODO(tsileo): return (*Cache, error)
func New(opts *clientutil.Opts, name string) (*Cache, error) {
	path := filepath.Join(pathutil.VarDir(), name)
	if err := os.MkdirAll(path, 0700); err != nil {
		return nil, err
	}
	kv, err := vkv.New(filepath.Join(path, "kv"))
	if err != nil {
		return nil, err
	}
	lbs, err := localblobstore.New("", name)
	if err != nil {
		return nil, err
	}
	return &Cache{
		kv:  kv,
		bs:  blobstore.New(opts),
		lbs: lbs,
	}, nil
}

func (c *Cache) Close() error {
	c.lbs.Close()
	return c.kv.Close()
}

func (c *Cache) Vkv() *vkv.DB {
	return c.kv
}

func (c *Cache) Client() *clientutil.Client {
	return c.bs.Client()
}

func (c *Cache) PutRemote(hash string, blob []byte) error {
	return c.bs.Put(hash, blob)
}

func (c *Cache) Put(hash string, blob []byte) error {
	return c.lbs.Put(hash, blob)
}

func (c *Cache) StatRemote(hash string) (bool, error) {
	return c.bs.Stat(hash)
}

func (c *Cache) Stat(hash string) (bool, error) {
	exists, err := c.lbs.Stat(hash)
	if err != nil {
		return false, err
	}
	if !exists {
		return c.bs.Stat(hash)
	}
	return exists, err
}

func (c *Cache) Get(ctx context.Context, hash string) ([]byte, error) {
	blob, err := c.lbs.Get(hash)
	switch err {
	// If the blob is not found locally, try to fetch it from the remote blobstore
	case clientutil.ErrBlobNotFound:
		blob, err = c.bs.Get(ctx, hash)
		if err != nil {
			return nil, err
		}
		// Save the blob locally for future fetch
		if err := c.lbs.Put(hash, blob); err != nil {
			return nil, err
		}
	case nil:
	default:
		return nil, err
	}
	return blob, nil
}

func (c *Cache) Sync(syncfunc func()) error {
	// TODO(tsileo): a way to sync a subtree to the remote blobstore `bs`
	// Passing a func may not be the optimal way, better to expose an Enumerate? maybe not even needed?
	return nil
}
