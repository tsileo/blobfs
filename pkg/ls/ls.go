package ls

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/tsileo/blobfs/pkg/root"
	"github.com/tsileo/blobstash/client/kvstore"
)

func Ls(kvs *kvstore.KvStore) {
	res, err := kvs.Keys("blobfs:root:", "blobfs:root:\xff", 0)
	if err != nil {
		fmt.Printf("failed to list fs: %v", err)
		os.Exit(1)
	}
	for _, r := range res {
		t := time.Unix(0, int64(r.Version))
		rdata := strings.Split(r.Key, ":")
		root, err := root.NewFromJSON([]byte(r.Value))
		if err != nil {
			panic(err)
		}
		fmt.Printf("%v\t%v\t%v\t%v\n", rdata[2], t, root.Hostname, root.Ref[:16])
	}
}
