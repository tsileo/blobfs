package blobstore

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/dchest/blake2b"
)

type Blob struct {
	Hash string
	Data []byte
}

func TestBlobStore(t *testing.T) {
	dir, err := ioutil.TempDir("", "blobfs")
	if err != nil {
		panic(err)
	}

	defer os.RemoveAll(dir) // clean up

	bs, err := New(dir, "testfs")
	if err != nil {
		panic(err)
	}
	defer func() {
		bs.Close()
		bs.Destroy()
	}()
	blobs := []*Blob{}
	blobsIndex := map[string]struct{}{}

	// Create fixtures
	for i := 0; i < 10; i++ {
		blob := make([]byte, 1024*8)
		if _, err := rand.Read(blob); err != nil {
			panic(err)
		}
		hash := fmt.Sprintf("%x", blake2b.Sum256(blob))

		blobs = append(blobs, &Blob{hash, blob})
		blobsIndex[hash] = struct{}{}

		// Put the just created blob
		if err := bs.Put(hash, blob); err != nil {
			panic(err)
		}
	}
	for _, blob := range blobs {
		ok, err := bs.Stat(blob.Hash)
		if !ok {
			t.Errorf("failed to stat blob %s", blob.Hash)
		}
		if err != nil {
			panic(err)
		}
		blob2, err := bs.Get(blob.Hash)
		if err != nil {
			panic(err)
		}

		if !bytes.Equal(blob.Data, blob2) {
			t.Errorf("failed to fetch blob %s", blob.Hash)
		}

	}

	// Delete a blobs
	blob1 := blobs[0]
	blobs = blobs[1:len(blobs)]

	if err := bs.Remove(blob1.Hash); err != nil {
		panic(err)
	}

	// Ensure the stat return false
	ok, err := bs.Stat(blob1.Hash)
	if err != nil {
		panic(err)
	}
	if ok {
		t.Errorf("blob %s should be removed", blob1.Hash)
	}

	if _, err := bs.Get(blob1.Hash); err != ErrBlobNotFound {
		t.Errorf("Blob %s should not be present", blob1.Hash)
	}

	cnt := 0
	walkFunc := func(path, hash string, err error) error {
		cnt++
		if _, ok := blobsIndex[hash]; !ok {
			t.Errorf("blob %s not in index", hash)
		}
		return nil
	}

	if err := bs.Iter(walkFunc); err != nil {
		panic(err)
	}

	if cnt != 9 {
		t.Errorf("9 blobs expected, got %d", cnt)
	}
}
