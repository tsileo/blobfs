package cache

import (
	"os"
	"path/filepath"

	"golang.org/x/net/context"
	log "gopkg.in/inconshreveable/log15.v2"

	localblobstore "github.com/tsileo/blobfs/pkg/blobstore"
	"github.com/tsileo/blobfs/pkg/pathutil"
	"github.com/tsileo/blobstash/pkg/client/blobstore"
	"github.com/tsileo/blobstash/pkg/client/clientutil"
)

type Cache struct {
	lbs *localblobstore.BlobStore
	rbs *blobstore.BlobStore // Remote BlobStore client for BlobStash
	log log.Logger
}

// TODO(tsileo): return (*Cache, error)
func New(logger log.Logger, opts *clientutil.Opts, name string) (*Cache, error) {
	path := filepath.Join(pathutil.VarDir(), name)
	if err := os.MkdirAll(path, 0700); err != nil {
		return nil, err
	}
	lbs, err := localblobstore.New("", name)
	if err != nil {
		return nil, err
	}
	return &Cache{
		rbs: blobstore.New(opts),
		lbs: lbs,
		log: logger,
	}, nil
}

func (c *Cache) Close() error {
	return c.lbs.Close()
}

func (c *Cache) Client() *clientutil.Client {
	return c.rbs.Client()
}

func (c *Cache) PutRemote(hash string, blob []byte) error {
	c.log.Debug("OP Put remote", "hash", hash)
	return c.rbs.Put(hash, blob)
}

func (c *Cache) Put(hash string, blob []byte) error {
	c.log.Debug("OP Put", "hash", hash)
	return c.lbs.Put(hash, blob)
}

func (c *Cache) StatRemote(hash string) (bool, error) {
	return c.rbs.Stat(hash)
}

func (c *Cache) Stat(hash string) (bool, error) {
	exists, err := c.lbs.Stat(hash)
	if err != nil {
		return false, err
	}
	if !exists {
		return c.rbs.Stat(hash)
	}
	return exists, err
}

func (c *Cache) Get(ctx context.Context, hash string) ([]byte, error) {
	c.log.Debug("OP Get", "hash", hash)
	blob, err := c.lbs.Get(hash)
	switch err {
	// If the blob is not found locally, try to fetch it from the remote blobstore
	case clientutil.ErrBlobNotFound:
		blob, err = c.rbs.Get(ctx, hash)
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
