package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	_ "io"
	"io/ioutil"
	"net/http"
	_ "net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"sync"
	"syscall"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"bazil.org/fuse/fuseutil"
	"github.com/tsileo/blobfs/pkg/cache"
	"github.com/tsileo/blobfs/pkg/root"
	"github.com/tsileo/blobstash/ext/filetree/filetreeutil/meta"
	"github.com/tsileo/blobstash/ext/filetree/reader/filereader"
	"github.com/tsileo/blobstash/ext/filetree/writer"
	"github.com/tsileo/blobstash/pkg/client/blobstore"
	"github.com/tsileo/blobstash/pkg/client/kvstore"
	"github.com/tsileo/blobstash/vkv"
	"golang.org/x/net/context"
	"gopkg.in/inconshreveable/log15.v2"
)

// XXX(tsileo): consider rewriting the init using the filetree API
// FIXME(tsileo): remove Dir.Children and rename Children2 to Children
// TODO(tsileo): embed an HTTP server to:
// - cache all the FS' blobs locally
// - clean the cache
// + a cli tool like `blobfs volume home sync` and in the future even sharing
// TODO(tsileo): react on remote changes by:
// - polling?
// - SSE (e.g. the VKV watch endpoint)
// TODO(tsileo): find a secure way to provide semi-private (Bewit signed) link

const maxInt = int(^uint(0) >> 1)

var virtualXAttrs = map[string]func(*meta.Meta) []byte{
	"ref": func(m *meta.Meta) []byte {
		return []byte(m.Hash)
	},
	"url": nil, // Will be computed dynamically
	// "last_sync": func(_ *meta.Meta) []byte {
	// 	stats.Lock()
	// 	defer stats.Unlock()
	// 	// TODO(tsileo): implement the lat_sync
	// 	return []byte("")
	// },
}

var wg sync.WaitGroup
var bfs *FS

// XXX(tsileo): move this from blobfs to blobfs-mount

var Usage = func() {
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  %s NAME MOUNTPOINT\n", os.Args[0])
	flag.PrintDefaults()
}

var Log = log15.New()
var stats *Stats

func WriteJSON(w http.ResponseWriter, data interface{}) {
	js, err := json.Marshal(data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Write(js)
}

type API struct {
}

func (api *API) Serve() error {
	http.HandleFunc("/", apiIndexHandler)
	http.HandleFunc("/stats", apiStatsHandler)
	http.HandleFunc("/checkout", apiCheckoutHandler)
	http.HandleFunc("/status", apiStatusHandler)
	http.HandleFunc("/sync", apiSyncHandler)
	http.HandleFunc("/log", apiLogHandler)
	http.HandleFunc("/public", apiPublicHandler)
	return http.ListenAndServe("localhost:8049", nil)
}

func apiIndexHandler(w http.ResponseWriter, r *http.Request) {
	WriteJSON(w, map[string]interface{}{
		"stats":    bfs.host + "/stats",
		"sync":     bfs.host + "/sync",
		"public":   bfs.host + "/public",
		"checkout": bfs.host + "/checkout",
		"log":      bfs.host + "/log",
	})
}

func apiStatsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
	stats.Lock()
	defer stats.Unlock()
	WriteJSON(w, stats)
}

type NodeStatus struct {
	Type string
	Path string
	Ref  string
}

func DirToStatus(d *Dir) ([]*NodeStatus, map[string]*NodeStatus) {
	root := []*NodeStatus{}
	index := map[string]*NodeStatus{}
	if err := iterDir(d, func(node fs.Node) error {
		switch n := node.(type) {
		case *File:
			path := ""
			if n.parent.Name != "_root" {
				p1 := n.parent
				for p1.parent != nil {
					path = filepath.Join(path, p1.Name)
					p1 = p1.parent
				}
			}
			p := filepath.Join(path, n.Meta.Name)
			nd := &NodeStatus{Type: "file", Path: p, Ref: n.Meta.Hash}
			root = append(root, nd)
			index[p] = nd
		case *Dir:
			path := ""
			if n.Name == "_root" {
				return nil
			}
			if n.parent != nil {
				p1 := n.parent
				for p1.parent != nil {
					path = filepath.Join(path, p1.Name)
					p1 = p1.parent
				}
			}
			p := filepath.Join(path, n.Name) + "/"
			nd := &NodeStatus{Type: "dir", Path: p, Ref: n.meta.Hash}
			root = append(root, nd)
			index[p] = nd
		}
		return nil
	}); err != nil {
		panic(err)
	}
	return root, index
}

func apiStatusHandler(w http.ResponseWriter, r *http.Request) {
	if bfs.mount.ref != bfs.staging.ref {
		// No changes, returns a 204
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if bfs.mount.ref != bfs.staging.ref {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	added := []string{}
	deleted := []string{}
	modified := []string{}
	_, latestIndex := DirToStatus(bfs.latest.root.(*Dir))
	_, stagingIndex := DirToStatus(bfs.staging.root.(*Dir))

	// XXX(tsileo): what abour **R**enamed

	for path, ns := range stagingIndex {
		if lns, ok := latestIndex[path]; ok {
			if ns.Ref != lns.Ref {
				modified = append(modified, ns.Path)
			}
		} else {
			added = append(added, ns.Path)
		}
	}
	for path, ns := range latestIndex {
		if _, ok := stagingIndex[path]; !ok {
			deleted = append(deleted, ns.Path)
		}
	}

	WriteJSON(w, map[string]interface{}{
		"ref":      bfs.mount.ref,
		"added":    added,
		"modified": modified,
		"deleted":  deleted,
		// "latest":   latest,
		// "staging":  staging,
	})
}

type CheckoutReq struct {
	Ref string `json:"ref"`
}

func apiCheckoutHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "POST request expected", http.StatusMethodNotAllowed)
		return
	}
	cr := &CheckoutReq{}
	if err := json.NewDecoder(r.Body).Decode(cr); err != nil {
		if err != nil {
			panic(err)
		}
	}
	switch cr.Ref {
	case "LATEST":
		cr.Ref = bfs.latest.ref
	case "STAGING":
		if bfs.staging == nil {
			cr.Ref = bfs.latest.ref
		}
		cr.Ref = bfs.staging.ref
	}
	// FIXME(tsileo): Check if cr.Ref == lastRef, and if so make it mutable
	if err := bfs.setRoot(cr.Ref, true); err != nil {
		panic(err)
	}
	WriteJSON(w, cr)
}

type SyncReq struct {
	Comment string `json:"comment"`
}

func apiSyncHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "POST request expected", http.StatusMethodNotAllowed)
		return
	}
	sr := &SyncReq{}
	if err := json.NewDecoder(r.Body).Decode(sr); err != nil {
		if err != nil {
			panic(err)
		}
	}
	// FIXME(tsileo): sync with an optional comment read from the body
	bfs.sync <- sr
	w.WriteHeader(http.StatusNoContent)
}

func apiPublicHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	out := map[string]*meta.Meta{}
	root, err := bfs.Root()
	if err != nil {
		panic(err)
	}
	rootDir := root.(*Dir)
	if err := iterDir(rootDir, func(node fs.Node) error {
		switch n := node.(type) {
		case *File:
			if n.Meta.IsPublic() {
				out[n.Meta.Hash] = n.Meta
			}
		case *Dir:
			if n.meta.IsPublic() {
				out[n.meta.Hash] = n.meta
			}
		}
		return nil
	}); err != nil {
		panic(err)
	}
	WriteJSON(w, out)
}

type CommitLog struct {
	T       string `json:"t"`
	Ref     string `json:"ref"`
	Comment string `json:"comment"`
	Current bool   `json:"current"`
}

func apiLogHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	versions, err := bfs.kvs.Versions(fmt.Sprintf(rootKeyFmt, bfs.name), 0, -1, 0)
	if err != nil {
		panic(err)
	}
	out := []*CommitLog{}
	if bfs.staging != nil && bfs.staging.ref != bfs.latest.ref {
		cl := &CommitLog{
			Ref:     "STAGING",
			Comment: "",
			T:       "",
		}
		if bfs.mount.ref == bfs.staging.ref {
			cl.Current = true
		}
		out = append(out, cl)
	}
	for _, v := range versions.Versions {
		croot := &root.Root{}
		if err := json.Unmarshal([]byte(v.Value), croot); err != nil {
			panic(err)
		}
		cl := &CommitLog{
			T:       time.Unix(0, int64(v.Version)).Format(time.RFC3339),
			Ref:     croot.Ref,
			Comment: croot.Comment,
		}
		// if cl.Ref == bfs.latest.ref {
		// 	cl.Ref = "LATEST"
		// }
		if bfs.mount.ref == cl.Ref {
			cl.Current = true
		}
		out = append(out, cl)
	}
	WriteJSON(w, out)
}

func apiCacheHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	// TODO(tsileo): iter the FS and output the public nodes
	root, err := bfs.Root()
	if err != nil {
		panic(err)
	}
	rootDir := root.(*Dir)
	if err := iterDir(rootDir, func(node fs.Node) error {
		switch n := node.(type) {
		case *File:
			for _, m := range n.Meta.Refs {
				data := m.([]interface{})
				hash := data[1].(string)
				if _, err := bfs.bs.Get(hash); err != nil {
					return err
				}
			}
		}
		return nil
	}); err != nil {
		panic(err)
	}
	WriteJSON(w, map[string]interface{}{})
}

// iterDir executes the given callback `cb` on each nodes (file or dir) recursively.
func iterDir(dir *Dir, cb func(n fs.Node) error) error {
	for _, node := range dir.Children2 {
		switch n := node.(type) {
		case *File:
			if err := cb(n); err != nil {
				return err
			}
		case *Dir:
			if err := iterDir(n, cb); err != nil {
				return err
			}
		}
	}
	return cb(dir)
}

// Borrowed from https://github.com/ipfs/go-ipfs/blob/master/fuse/mount/mount.go
func unmount(mountpoint string) error {
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "darwin":
		cmd = exec.Command("diskutil", "umount", "force", mountpoint)
	case "linux":
		cmd = exec.Command("fusermount", "-u", mountpoint)
	default:
		return fmt.Errorf("unmount: unimplemented")
	}

	errc := make(chan error, 1)
	go func() {
		defer close(errc)

		// try vanilla unmount first.
		if err := exec.Command("umount", mountpoint).Run(); err == nil {
			return
		}

		// retry to unmount with the fallback cmd
		errc <- cmd.Run()
	}()

	select {
	case <-time.After(5 * time.Second):
		return fmt.Errorf("umount timeout")
	case err := <-errc:
		return err
	}
}

func main() {
	hostPtr := flag.String("host", "", "remote host, default to http://localhost:8050")
	loglevelPtr := flag.String("loglevel", "info", "logging level (debug|info|warn|crit)")
	immutablePtr := flag.Bool("immutable", false, "make the filesystem immutable")
	hostnamePtr := flag.String("hostname", "", "default to system hostname")

	flag.Usage = Usage
	flag.Parse()

	if flag.NArg() != 2 {
		Usage()
		os.Exit(2)
	}
	name := flag.Arg(0)
	mountpoint := flag.Arg(1)

	var err error
	root.Hostname = *hostnamePtr
	if root.Hostname == "" {
		root.Hostname, err = os.Hostname()
		if err != nil {
			fmt.Printf("failed to retrieve hostname, set one manually: %v", err)
		}
	}

	lvl, err := log15.LvlFromString(*loglevelPtr)
	if err != nil {
		panic(err)
	}
	Log.SetHandler(log15.LvlFilterHandler(lvl, log15.StreamHandler(os.Stdout, log15.TerminalFormat())))
	fslog := Log.New("name", name)

	stats = &Stats{LastReset: time.Now()}
	go func() {
		t := time.NewTicker(10 * time.Second)
		for _ = range t.C {
			if stats.updated {
				fslog.Info(stats.String())
				fslog.Debug("Flushing stats")
				stats.Reset()
			}
		}
	}()

	go func() {
		api := &API{}
		// TODO(tsileo): make the API port configurable
		fslog.Info("Starting API at localhost:8049")
		if err := api.Serve(); err != nil {
			fslog.Crit("failed to start API")
		}
	}()

	fslog.Info("Mouting fs...", "mountpoint", mountpoint, "immutable", *immutablePtr)
	bsOpts := blobstore.DefaultOpts().SetHost(*hostPtr, os.Getenv("BLOBSTASH_API_KEY"))
	bsOpts.SnappyCompression = false
	bs := cache.New(bsOpts, "blobfs_cache")
	kvsOpts := kvstore.DefaultOpts().SetHost(*hostPtr, os.Getenv("BLOBSTASH_API_KEY"))
	kvsOpts.SnappyCompression = false
	kvs := kvstore.New(kvsOpts)

	// Fetch the Hawk key for creating sharing URL (using Bewit)
	// hawkResp, err := bs.Client().DoReq("GET", "/api/v1/perms/hawk", nil, nil)
	// if err != nil {
	// 	panic(err)
	// }
	// if hawkResp.StatusCode != 200 {
	// 	panic("failed to fetch Hawk key")
	// }
	// k := struct {
	// 	Key string `json:"key"`
	// }{}
	// if err := json.NewDecoder(hawkResp.Body).Decode(&k); err != nil {
	// 	panic(err)
	// }

	c, err := fuse.Mount(
		mountpoint,
		fuse.FSName(name),
		fuse.Subtype("blobfs"),
		// fuse.LocalVolume(),
		fuse.VolumeName(name),
	)
	defer c.Close()
	if err != nil {
		fslog.Crit("failed to mount", "err", err)
		os.Exit(1)
	}
	// FIXME(tsileo): handle shutdown
	// go func() {
	// 	cs := make(chan os.Signal, 1)
	// 	signal.Notify(cs, os.Interrupt,
	// 		syscall.SIGHUP,
	// 		syscall.SIGINT,
	// 		syscall.SIGTERM,
	// 		syscall.SIGQUIT)
	// 	<-cs
	// 	// c.Close()
	// 	fslog.Info("Unmounting...")
	// 	// bfs.bs.Close()
	// 	c.Close()
	// 	fuse.Unmount(mountpoint)
	// 	os.Exit(0)
	// }()
	bfs = &FS{
		log:       fslog,
		name:      name,
		bs:        bs,
		kvs:       kvs,
		uploader:  writer.NewUploader(bs),
		immutable: *immutablePtr,
		host:      bsOpts.Host,
		sync:      make(chan *SyncReq),
	}

	go func() {
		for {
			sync := func(sr *SyncReq) {
				wg.Add(1)
				defer wg.Done()
				l := fslog.New("module", "sync")
				l.Debug("Sync triggered")

				// Keep some basic stats about the on-going sync
				stats := &SyncStats{}

				if bfs == nil {
					l.Debug("bfs is nil")
					return
				}
				bfs.mu.Lock()
				defer func() {
					l.Info("Sync done", "blobs_uploaded", stats.BlobsUploaded, "blobs_skipped", stats.BlobsSkipped)
					bfs.mu.Unlock()
				}()
				kv, err := bfs.bs.Vkv().Get(fmt.Sprintf(rootKeyFmt, bfs.Name()), -1)
				if err != nil {
					l.Error("Sync failed (failed to fetch the local vkv entry)", "err", err)
					return
				}
				l.Debug("last sync info", "current version", kv.Version, "lastRootVersion", bfs.lastRootVersion)
				if kv.Version == bfs.lastRootVersion {
					l.Info("Already in sync")
					return
				}
				rkv, err := bfs.kvs.Get(fmt.Sprintf(rootKeyFmt, bfs.Name()), -1)
				if err != nil {
					if err != kvstore.ErrKeyNotFound {
						l.Error("Sync failed (failed to fetch the remote vkv entry)", "err", err)
						return
					}
				}

				if rkv != nil && rkv.Version == kv.Version {
					l.Info("Already in sync")
					return
				}

				oroot, err := bfs.Root()
				if err != nil {
					l.Error("Failed to fetch root", "err", err)
					return
				}
				rootDir := oroot.(*Dir)
				// putBlob will try to upload all missing blobs to the remote BlobStash instance
				putBlob := func(l log15.Logger, hash string, blob []byte) error {
					mexists, err := bfs.bs.StatRemote(hash)
					if err != nil {
						l.Error("stat failed", "err", err)
						return err
					}
					if mexists {
						stats.BlobsSkipped++
					} else {
						// Fetch the blob locally via the cache if needed
						if blob == nil {
							blob, err = bfs.bs.Get(hash)
							if err != nil {
								return err
							}

						}
						if err := bfs.bs.PutRemote(hash, blob); err != nil {
							l.Error("put failed", "err", err)
							return err
						}
						stats.BlobsUploaded++
					}
					return nil
				}
				if err := iterDir(rootDir, func(node fs.Node) error {
					switch n := node.(type) {
					case *File:
						// n.mu.Lock()
						// defer n.mu.Lock()
						// Save the meta
						mhash, mjs := n.Meta.Json()
						if putBlob(n.log, mhash, mjs); err != nil {
							return err
						}
						// Save all the parts of the file
						for _, m := range n.Meta.Refs {
							data := m.([]interface{})
							hash := data[1].(string)
							if err := putBlob(n.log, hash, nil); err != nil {
								return err
							}
						}
					case *Dir:
						// TODO(tsileo): fix the locking here and above
						// n.mu.Lock()
						// defer n.mu.Lock()
						mhash, mjs := n.meta.Json()
						if putBlob(n.log, mhash, mjs); err != nil {
							return err
						}
					}
					return nil
				}); err != nil {
					l.Error("iterDir failed", "err", err)
					return
				}
				bfs.lastRootVersion = kv.Version
				// Save the vkv entry in the remote vkv API
				newRoot := &root.Root{}
				if err := json.Unmarshal([]byte(kv.Value), newRoot); err != nil {
					return
				}
				newRoot.Comment = sr.Comment
				newValue, err := json.Marshal(newRoot)
				if err != nil {
					return
				}
				if _, err := bfs.kvs.Put(kv.Key, string(newValue), kv.Version); err != nil {
					l.Error("Sync failed (failed to update the remote vkv entry)", "err", err)
					return
				}
				if bfs.latest == nil {
					bfs.latest = &Mount{}
				}
				bfs.latest.ref = newRoot.Ref
			}
			select {
			case sr := <-bfs.sync:
				fslog.Info("Sync triggered")
				sync(sr)
			}
		}
	}()

	go func() {
		wg.Add(1)
		err = fs.Serve(c, bfs)
		if err != nil {
			fslog.Crit("failed to serve", "err", err)
			os.Exit(1)
		}

		// check if the mount process has an error to report
		<-c.Ready
		fslog.Debug("ready")
		if err := c.MountError; err != nil {
			fslog.Crit("mount error", "err", err)
			os.Exit(1)
		}
		if err := c.Close(); err != nil {
			fslog.Crit("failed to close connection", "err", err)
		}
		bfs.bs.Close()
		wg.Done()
	}()

	cs := make(chan os.Signal, 1)
	signal.Notify(cs, os.Interrupt,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	<-cs
	fslog.Info("Unmounting...")
	if err := unmount(mountpoint); err != nil {
		fslog.Crit("failed to unmount", "err", err)
		os.Exit(1)
	}
	wg.Wait()
	os.Exit(0)
}

type SyncStats struct {
	BlobsUploaded int
	BlobsSkipped  int
}

type Stats struct {
	LastReset    time.Time
	FilesCreated int
	DirsCreated  int
	FilesUpdated int
	DirsUpdated  int
	updated      bool
	sync.Mutex
}

func (s *Stats) Reset() {
	s.LastReset = time.Now()
	s.FilesCreated = 0
	s.DirsCreated = 0
	s.FilesUpdated = 0
	s.DirsUpdated = 0
	s.updated = false
}
func (s *Stats) String() string {
	return fmt.Sprintf("%d files created, %d dirs created", s.FilesCreated, s.DirsCreated)
}

// debugFile is a dummy file that hold a string
type debugFile struct {
	data []byte
}

func newDebugFile(data string) *debugFile {
	return &debugFile{
		data: []byte(data),
	}
}

func (f *debugFile) Attr(ctx context.Context, a *fuse.Attr) error {
	a.Inode = 2
	a.Mode = 0444
	a.Size = uint64(len(f.data))
	return nil
}

func (f *debugFile) ReadAll(ctx context.Context) ([]byte, error) {
	return f.data, nil
}

type FS struct {
	log             log15.Logger
	kvs             *kvstore.KvStore
	bs              *cache.Cache // blobstore.BlobStore
	uploader        *writer.Uploader
	immutable       bool
	name            string
	host            string
	mu              sync.Mutex // Guard for the sync goroutine
	lastRootVersion int
	sync            chan *SyncReq
	lastWrite       time.Time
	root            fs.Node
	mount           *Mount
	staging         *Mount
	latest          *Mount
}

type Mount struct {
	immutable bool
	root      fs.Node
	ref       string
}

// newBewit returns a `bewit` token valid for the given delay
// func (fs *FS) newBewit(url string, delay time.Duration) (string, error) {
// 	auth, err := hawk.NewURLAuth(url, &hawk.Credentials{
// 		ID:   appID,
// 		Key:  fs.hawkKey,
// 		Hash: sha256.New,
// 	}, delay)
// 	if err != nil {
// 		return "", err
// 	}
// 	return auth.Bewit(), nil
// }

func (f *FS) Immutable() bool {
	// TODO(tsileo): check the mount
	return f.immutable
}

func (f *FS) Name() string {
	return f.name
}

var rootKeyFmt = "blobfs:root:%v"

func (f *FS) Root() (fs.Node, error) {
	f.log.Info("OP Root")
	if f.mount == nil {
		f.log.Info("loadRoot")
		d, err := f.loadRoot()
		if err != nil {
			return nil, err
		}
		f.root = d
		return d, nil
	}
	f.log.Info("Root OP", "root", f.mount.root)
	return f.mount.root, nil
}

func (f *FS) setRoot(ref string, immutable bool) error {
	f.log.Info("setRoot", "ref", ref)
	blob, err := f.bs.Get(ref)
	if err != nil {
		return err
	}
	m, err := meta.NewMetaFromBlob(ref, blob)
	d, err := NewDir(f, m, nil)
	if err != nil {
		return err
	}
	f.mount = &Mount{immutable: immutable, root: d, ref: ref}
	*f.root.(*Dir) = *d
	// f.root.(*Dir).log = f.root.(*Dir).log.New("ref", d.meta.Hash)
	// XXX(tsileo): keep the real root meta somewhere to get back to HEAD
	return nil
}

func (f *FS) loadRoot() (fs.Node, error) {
	var saveLocally bool
	lkv, err := f.bs.Vkv().Get(fmt.Sprintf(rootKeyFmt, f.Name()), -1)
	switch err {
	case nil:
		root, err := root.NewFromJSON([]byte(lkv.Value))
		if err != nil {
			return nil, err
		}
		blob, err := f.bs.Get(root.Ref)
		if err != nil {
			return nil, err
		}
		m, err := meta.NewMetaFromBlob(root.Ref, blob)
		if err != nil {
			return nil, err
		}
		f.log.Debug("loaded meta root", "ref", m.Hash)
		d, err := NewDir(f, m, nil)
		if err != nil {
			return nil, err
		}
		f.mount = &Mount{immutable: f.immutable, root: d, ref: m.Hash}
		f.staging = f.mount
		// return d, nil
	case vkv.ErrNotFound:
		saveLocally = true
	default:
		return nil, err
	}

	kv, err := f.kvs.Get(fmt.Sprintf(rootKeyFmt, f.Name()), -1)
	switch err {
	case nil:
		f.lastRootVersion = kv.Version
		if saveLocally {
			if f.bs.Vkv().Put(kv.Key, kv.Value, kv.Version); err != nil {
				return nil, err
			}
		}
		root, err := root.NewFromJSON([]byte(kv.Value))
		if err != nil {
			return nil, err
		}
		blob, err := f.bs.Get(root.Ref)
		if err != nil {
			return nil, err
		}
		m, err := meta.NewMetaFromBlob(root.Ref, blob)
		if err != nil {
			return nil, err
		}
		f.log.Debug("loaded meta root", "ref", m.Hash)
		d, err := NewDir(f, m, nil)
		if err != nil {
			return nil, err
		}
		cmount := &Mount{immutable: f.immutable, root: d, ref: m.Hash}
		if f.staging == nil {
			f.mount = cmount
		}
		f.latest = cmount
		// return d, nil
	case kvstore.ErrKeyNotFound:
		root := &Dir{
			fs:        f,
			Name:      "_root",
			Children:  map[string]*meta.Meta{},
			Children2: map[string]fs.Node{},
			meta:      &meta.Meta{},
		}
		root.log = f.log.New("ref", "undefined", "name", "_root", "type", "dir")
		if err := root.save(false); err != nil {
			return nil, err
		}
		f.log.Debug("Creating a new root", "ref", root.meta.Hash)
		root.log = root.log.New("ref", root.meta.Hash)
		f.mount = &Mount{immutable: f.immutable, root: root, ref: root.meta.Hash}
		f.latest = f.mount
	default:
		return nil, err
	}
	f.log.Info("initial load", "mount", f.mount, "latest", f.latest, "staging", f.staging)
	if f.staging != nil {
		return f.staging.root, nil
	}
	return f.latest.root, nil
}

// Dir implements both Node and Handle for the root directory.
type Dir struct {
	fs        *FS
	meta      *meta.Meta
	parent    *Dir
	Name      string
	Children  map[string]*meta.Meta
	Children2 map[string]fs.Node
	log       log15.Logger
	mu        sync.Mutex
}

func NewDir(rfs *FS, m *meta.Meta, parent *Dir) (*Dir, error) {
	d := &Dir{
		fs:        rfs,
		meta:      m,
		parent:    parent,
		Name:      m.Name,
		Children:  map[string]*meta.Meta{},
		Children2: map[string]fs.Node{},
		log:       rfs.log.New("ref", m.Hash, "name", m.Name, "type", "dir"),
	}
	if err := d.reload(); err != nil {
		return nil, err
	}
	return d, nil
}

func (d *Dir) reload() error {
	d.log.Info("Reload")
	for _, ref := range d.meta.Refs {
		blob, err := d.fs.bs.Get(ref.(string))
		if err != nil {
			return err
		}
		m, err := meta.NewMetaFromBlob(ref.(string), blob)
		if err != nil {
			return err
		}
		d.Children[m.Name] = m
		if m.IsDir() {
			ndir, err := NewDir(d.fs, m, d)
			if err != nil {
				return err
			}
			d.Children2[m.Name] = ndir
		} else {
			nfile, err := NewFile(d.fs, m, d)
			if err != nil {
				return err
			}
			d.Children2[m.Name] = nfile
		}
	}
	return nil
}

func (d *Dir) Attr(ctx context.Context, a *fuse.Attr) error {
	a.Inode = 1
	a.Mode = os.ModeDir | 0555
	return nil
}

func makePublic(n fs.Node, value string) error {
	switch node := n.(type) {
	case *Dir:
		if value == "1" {
			node.meta.XAttrs["public"] = value
		} else {
			delete(node.meta.XAttrs, "public")
		}
		if err := node.save(false); err != nil {
			return err
		}
		for _, child := range node.Children2 {
			if err := makePublic(child, value); err != nil {
				return err
			}
		}
	case *File:
		if value == "1" {
			node.Meta.XAttrs["public"] = value
		} else {
			delete(node.Meta.XAttrs, "public")
		}
		if err := node.save(); err != nil {
			return err
		}
	default:
		panic("shouldn't happen")
	}
	return nil
}

func (d *Dir) Setxattr(ctx context.Context, req *fuse.SetxattrRequest) error {
	d.log.Debug("OP Setxattr", "name", req.Name, "xattr", string(req.Xattr))
	d.mu.Lock()
	defer d.mu.Unlock()

	// If the request is to make the dir public, make it recursively
	if req.Name == "public" {
		return makePublic(d, string(req.Xattr))
	}

	// Prevent writing attributes name that are virtual attributes
	if _, exists := virtualXAttrs[req.Name]; exists {
		return nil
	}

	if d.meta.XAttrs == nil {
		d.meta.XAttrs = map[string]string{}
	}
	d.meta.XAttrs[req.Name] = string(req.Xattr)
	if err := d.save(false); err != nil {
		return err
	}

	// // Trigger a sync so the file will be (un)available for BlobStash right now
	// if req.Name == "public" {
	// 	bfs.sync <- struct{}{}
	// }
	return nil
}

func (d *Dir) Removexattr(ctx context.Context, req *fuse.RemovexattrRequest) error {
	d.log.Debug("OP Removexattr", "name", req.Name)
	d.mu.Lock()
	defer d.mu.Unlock()

	// Can't delete virtual attributes
	if _, exists := virtualXAttrs[req.Name]; exists {
		return fuse.ErrNoXattr
	}

	if d.meta.XAttrs == nil {
		return fuse.ErrNoXattr
	}

	if _, ok := d.meta.XAttrs[req.Name]; ok {
		// Delete the attribute
		delete(d.meta.XAttrs, req.Name)
		if err := d.save(false); err != nil {
			return err
		}

		// // Trigger a sync so the file won't be available via BlobStash
		// if req.Name == "public" {
		// 	bfs.sync <- struct{}{}
		// }

		return nil
	}
	return fuse.ErrNoXattr
}

func (d *Dir) Forget() {
	d.log.Debug("OP Forget")
}

func (d *Dir) Listxattr(ctx context.Context, req *fuse.ListxattrRequest, resp *fuse.ListxattrResponse) error {
	d.log.Debug("OP Listxattr")
	d.mu.Lock()
	defer d.mu.Unlock()
	return handleListxattr(d.meta, resp)
}

func (d *Dir) Getxattr(ctx context.Context, req *fuse.GetxattrRequest, resp *fuse.GetxattrResponse) error {
	d.log.Debug("OP Getxattr", "name", req.Name)
	d.mu.Lock()
	defer d.mu.Unlock()
	return handleGetxattr(d.fs, d.meta, req, resp)
}

func (d *Dir) Rename(ctx context.Context, req *fuse.RenameRequest, newDir fs.Node) error {
	d.log.Debug("OP Rename", "name", req.OldName, "new_name", req.NewName)
	defer d.log.Debug("OP Rename end", "name", req.OldName, "new_name", req.NewName)
	if node, ok := d.Children2[req.OldName]; ok {
		// FIXME(tsileo): update the name of the meta
		m := d.Children[req.OldName]
		m2, err := d.fs.uploader.RenameMeta(m, req.NewName)
		if err != nil {
			return err
		}
		d.mu.Lock()
		defer d.mu.Unlock()
		// Delete the source
		delete(d.Children, req.OldName)
		delete(d.Children2, req.OldName)
		if err := d.save(false); err != nil {
			return err
		}
		ndir := newDir.(*Dir)
		if d != ndir {
			ndir.mu.Lock()
			defer ndir.mu.Unlock()
		}
		ndir.Children[req.NewName] = m2
		ndir.Children2[req.NewName] = node
		switch n := node.(type) {
		case *Dir:
			n.Name = req.NewName
			n.meta = m2
		case *File:
			n.Meta = m2
		}
		ndir.Children2[req.NewName] = node
		if err := ndir.save(false); err != nil {
			return err
		}
		return nil
	}
	return fuse.EIO
}

func (d *Dir) Lookup(ctx context.Context, name string) (fs.Node, error) {
	d.log.Debug("OP Lookup", "name", name)
	defer d.log.Debug("OP Lookup END", "name", name)
	// normal lookup operation
	if name == ".blobfs_url" {
		// FIXME(tsileo): set the value dynamically
		return newDebugFile("http://localhost:8049"), nil
	}
	if c, ok := d.Children2[name]; ok {
		return c, nil
		// if c.IsFile() {
		// 	return NewFile(d.fs, c, d)
		// } else {
		// 	return NewDir(d.fs, c, d)
		// }
	}
	return nil, fuse.ENOENT
}

func (d *Dir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	d.log.Debug("OP ReadDirAll")
	defer d.log.Debug("OP ReadDirAll END")
	dirs := []fuse.Dirent{}
	for _, c := range d.Children {
		if c.IsDir() {
			dirs = append(dirs, fuse.Dirent{
				Inode: 1,
				Name:  c.Name,
				Type:  fuse.DT_Dir,
			})
		} else {
			dirs = append(dirs, fuse.Dirent{
				Inode: 2,
				Name:  c.Name,
				Type:  fuse.DT_File,
			})
		}
	}
	return dirs, nil
}

func (d *Dir) Mkdir(ctx context.Context, req *fuse.MkdirRequest) (fs.Node, error) {
	d.log.Debug("OP Mkdir", "name", req.Name)
	defer d.log.Debug("OP Mkdir END", "name", req.Name)
	if d.fs.Immutable() {
		return nil, fuse.EPERM
	}
	newdir := &Dir{
		fs:        d.fs,
		parent:    d,
		Name:      req.Name,
		Children:  map[string]*meta.Meta{},
		Children2: map[string]fs.Node{},
		meta:      &meta.Meta{},
	}
	newdir.log = d.fs.log.New("ref", "unknown", "name", req.Name, "type", "dir")
	if err := newdir.save(false); err != nil {
		return nil, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.Children[newdir.Name] = newdir.meta
	d.Children2[newdir.Name] = newdir
	if err := d.save(false); err != nil {
		return nil, err
	}
	newdir.log = newdir.log.New("ref", newdir.meta.Hash)

	stats.Lock()
	stats.updated = true
	stats.DirsCreated++
	stats.Unlock()

	return newdir, nil
}

func (d *Dir) Remove(ctx context.Context, req *fuse.RemoveRequest) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.log.Debug("OP Remove", "name", req.Name)
	defer d.log.Debug("OP Remove END", "name", req.Name)
	if d.fs.Immutable() {
		return fuse.EPERM
	}
	delete(d.Children, req.Name)
	if err := d.save(false); err != nil {
		d.log.Error("Failed to saved", "err", err)
		return err
	}
	return nil
}

func (d *Dir) save(sync bool) error {
	d.log.Debug("saving")
	m := meta.NewMeta()
	m.Type = "dir"
	m.Name = d.Name
	m.Mode = uint32(os.ModeDir | 0555)
	m.ModTime = time.Now().Format(time.RFC3339)
	if d.meta.ModTime != "" {
		m.ModTime = d.meta.ModTime
	}
	for _, c := range d.Children {
		m.AddRef(c.Hash)
	}
	mhash, mjs := m.Json()
	if sync && d.meta.Hash != mhash {
		return fmt.Errorf("different meta for dir %+v", d)
	}
	m.Hash = mhash
	d.meta = m
	// if sync {
	// 	d.log.Debug("sync")
	mexists, err := d.fs.bs.Stat(mhash)
	if err != nil {
		d.log.Error("stat failed", "err", err)
		return err
	}
	if !mexists {
		if err := d.fs.bs.Put(mhash, mjs); err != nil {
			d.log.Error("put failed", "err", err)
			return err
		}
	}
	if d.parent == nil {
		// If no parent, this is the root so save the ref
		root := root.New(mhash)
		js, err := json.Marshal(root)
		if err != nil {
			return err
		}
		// XXX(tsileo): mark1
		if _, err := d.fs.bs.Vkv().Put(fmt.Sprintf(rootKeyFmt, d.fs.Name()), string(js), -1); err != nil {
			return err
		}
		if d.fs.staging == nil {
			d.fs.staging = &Mount{}
		}
		d.fs.staging.ref = mhash
	}
	if d.parent != nil {
		d.parent.mu.Lock()
		defer d.parent.mu.Unlock()
		d.parent.Children[d.Name] = m
		if err := d.parent.save(sync); err != nil {
			return err
		}
	}
	return nil
}

func (d *Dir) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.log.Debug("OP Create", "name", req.Name)
	defer d.log.Debug("OP Create END", "name", req.Name)
	if d.fs.Immutable() {
		return nil, nil, fuse.EPERM
	}
	m := meta.NewMeta()
	m.Type = "file"
	m.Name = req.Name
	m.Mode = uint32(req.Mode)
	m.ModTime = time.Now().Format(time.RFC3339)

	// If the parent directory is public, the new file should to
	if d.meta.IsPublic() {
		m.XAttrs = map[string]string{"public": "1"}
	}

	// Save the meta
	mhash, mjs := m.Json()
	m.Hash = mhash
	mexists, err := d.fs.bs.Stat(mhash)
	if err != nil {
		return nil, nil, err
	}
	if !mexists {
		if err := d.fs.bs.Put(mhash, mjs); err != nil {
			return nil, nil, err
		}
	}
	// FIXME blobstash async mode
	// try if the meta hash is not already indexed
	var f *File
	for i := 0; i < 5; i++ {
		f, err = NewFile(d.fs, m, d)
		if err != nil {
			d.log.Debug("failed to fetch file", "attempt", i+1, "err", err)
			time.Sleep(50 * time.Millisecond)
			continue
		}
		break
	}
	if f == nil {
		return nil, nil, err
	}
	d.Children[m.Name] = m
	d.Children2[m.Name] = f
	if err := d.save(false); err != nil {
		return nil, nil, err
	}
	stats.Lock()
	stats.updated = true
	stats.FilesCreated++
	stats.Unlock()
	return f, f, nil
}

type File struct {
	fs       *FS
	data     []byte // FIXME(tsileo): if data grows too much, use a temp file
	Meta     *meta.Meta
	FakeFile *filereader.File
	log      log15.Logger
	parent   *Dir
	flushed  bool
	mu       sync.Mutex
}

func NewFile(fs *FS, m *meta.Meta, parent *Dir) (*File, error) {
	// blob, err := fs.bs.Get(m.Hash)
	// if err != nil {
	// 	return nil, err
	// }
	// m2, err := meta.NewMetaFromBlob(m.Hash, blob)
	// if err != nil {
	// 	return nil, err
	// }
	return &File{
		parent:  parent,
		fs:      fs,
		Meta:    m,
		log:     fs.log.New("ref", m.Hash, "name", m.Name, "type", "file"),
		flushed: true,
	}, nil
}

func (f *File) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(req.Data) > 0 {
		f.flushed = false
	}
	f.log.Debug("OP Write", "offset", req.Offset, "size", len(req.Data))
	defer f.log.Debug("OP Write END", "offset", req.Offset, "size", len(req.Data))
	if f.fs.Immutable() {
		return fuse.EPERM
	}
	newLen := req.Offset + int64(len(req.Data))
	if newLen > int64(maxInt) {
		return fuse.Errno(syscall.EFBIG)
	}

	n := copy(f.data[req.Offset:], req.Data)
	if n < len(req.Data) {
		f.data = append(f.data, req.Data[n:]...)
	}

	resp.Size = len(req.Data)
	return nil
}

type ClosingBuffer struct {
	*bytes.Buffer
}

func (*ClosingBuffer) Close() error {
	return nil
}

func (f *File) Flush(ctx context.Context, req *fuse.FlushRequest) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.log.Debug("OP Flush")
	defer f.log.Debug("OP Flush END")
	if f.fs.Immutable() {
		return nil
	}
	f.Meta.Size = len(f.data)
	if f.data != nil && len(f.data) > 0 {
		// XXX(tsileo): data will be saved once the tree will be synced
		buf := bytes.NewBuffer(f.data)
		m2, err := f.fs.uploader.PutReader(f.Meta.Name, &ClosingBuffer{buf})
		f.log.Debug("new meta", "meta", fmt.Sprintf("%+v", m2))
		// f.log.Debug("WriteResult", "wr", wr)
		if err != nil {
			return err
		}
		f.parent.mu.Lock()
		defer f.parent.mu.Unlock()
		f.parent.Children[m2.Name] = m2
		if err := f.parent.save(false); err != nil {
			return err
		}
		f.Meta = m2
		// f.log = f.log.New("ref", m2.Hash[:10])
		f.log.Debug("Flushed", "data_len", len(f.data))
		f.flushed = true
	}
	return nil
}

func (f *File) Setxattr(ctx context.Context, req *fuse.SetxattrRequest) error {
	f.log.Debug("OP Setxattr", "name", req.Name, "xattr", string(req.Xattr))
	f.mu.Lock()
	defer f.mu.Unlock()

	// Prevent writing attributes name that are virtual attributes
	if _, exists := virtualXAttrs[req.Name]; exists {
		return nil
	}

	if f.Meta.XAttrs == nil {
		f.Meta.XAttrs = map[string]string{}
	}
	f.Meta.XAttrs[req.Name] = string(req.Xattr)
	// XXX(tsileo): check thath the parent get the updated hash?
	if err := f.save(); err != nil {
		return err
	}
	// Trigger a sync so the file will be (un)available for BlobStash right now
	// if req.Name == "public" {
	// 	bfs.sync <- struct{}{}
	// }
	return nil
}

func (f *File) save() error {
	// Update the new `Meta`
	f.parent.fs.uploader.PutMeta(f.Meta)
	// And save the parent
	f.parent.mu.Lock()
	defer f.parent.mu.Unlock()
	if err := f.parent.save(false); err != nil {
		return err
	}
	return nil
}

func handleListxattr(m *meta.Meta, resp *fuse.ListxattrResponse) error {
	// Add the "virtual" eXtended Attributes
	for vattr, xattrFunc := range virtualXAttrs {
		if xattrFunc != nil {
			resp.Append(vattr)
		}
	}

	if m.XAttrs == nil {
		return nil
	}
	for k, _ := range m.XAttrs {
		resp.Append(k)
	}

	if m.IsPublic() {
		resp.Append("url")
	}
	return nil
}

func (f *File) Listxattr(ctx context.Context, req *fuse.ListxattrRequest, resp *fuse.ListxattrResponse) error {
	f.log.Debug("OP Listxattr")
	f.mu.Lock()
	defer f.mu.Unlock()
	return handleListxattr(f.Meta, resp)
}

func (f *File) Forget() {
	f.log.Debug("OP Forget")
}

func handleGetxattr(fs *FS, m *meta.Meta, req *fuse.GetxattrRequest, resp *fuse.GetxattrResponse) error {
	// Check if the request match a virtual extended attributes
	if xattrFunc, ok := virtualXAttrs[req.Name]; ok && xattrFunc != nil {
		resp.Xattr = xattrFunc(m)
		return nil
	}

	if m.XAttrs == nil {
		return fuse.ErrNoXattr
	}

	if req.Name == "url" {
		// Ensure the node is public
		if m.IsPublic() {
			// FIXME(tsileo): fetch the hostname from `bfs` to reconstruct an absolute URL
			// Output the URL
			raw_url := fmt.Sprintf("%s/%s/%s", fs.host, m.Type[0:1], m.Hash)
			resp.Xattr = []byte(raw_url)
			return nil
		}
	}

	if _, ok := m.XAttrs[req.Name]; ok {
		resp.Xattr = []byte(m.XAttrs[req.Name])
		return nil
	}
	return fuse.ErrNoXattr
}

func (f *File) Getxattr(ctx context.Context, req *fuse.GetxattrRequest, resp *fuse.GetxattrResponse) error {
	f.log.Debug("OP Getxattr", "name", req.Name)
	f.mu.Lock()
	defer f.mu.Unlock()
	return handleGetxattr(f.parent.fs, f.Meta, req, resp)
}

func (f *File) Removexattr(ctx context.Context, req *fuse.RemovexattrRequest) error {
	f.log.Debug("OP Removexattr", "name", req.Name)
	f.mu.Lock()
	defer f.mu.Unlock()

	// Can't delete virtual attributes
	if _, exists := virtualXAttrs[req.Name]; exists {
		return fuse.ErrNoXattr
	}

	if f.Meta.XAttrs == nil {
		return fuse.ErrNoXattr
	}

	if _, ok := f.Meta.XAttrs[req.Name]; ok {
		// Delete the attribute
		delete(f.Meta.XAttrs, req.Name)

		// Save the meta
		if err := f.save(); err != nil {
			return err
		}
		// Trigger a sync so the file won't be available via BlobStash
		// if req.Name == "public" {
		// 	bfs.sync <- struct{}{}
		// }
		return nil

	}
	return fuse.ErrNoXattr
}

func (f *File) Attr(ctx context.Context, a *fuse.Attr) error {
	f.log.Debug("OP Attr")
	defer f.log.Debug("OP Attr END")
	a.Inode = 2
	a.Mode = os.FileMode(f.Meta.Mode)
	a.Size = uint64(f.Meta.Size)
	if f.Meta.ModTime != "" {
		t, err := time.Parse(time.RFC3339, f.Meta.ModTime)
		if err != nil {
			panic(fmt.Errorf("error parsing mtime for %v: %v", f, err))
		}
		a.Mtime = t
	}
	return nil
}

func (f *File) Setattr(ctx context.Context, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {
	f.log.Debug("OP Setattr")
	defer f.log.Debug("OP Setattr END")
	if f.fs.Immutable() {
		return fuse.EPERM
	}
	//if req.Valid&fuse.SetattrMode != 0 {
	//if err := os.Chmod(n.path, req.Mode); err != nil {
	//	return err
	//}
	//	log.Printf("Setattr %v chmod", f.Meta.Name)
	//}
	//if req.Valid&(fuse.SetattrUid|fuse.SetattrGid) != 0 {
	//	if req.Valid&fuse.SetattrUid&fuse.SetattrGid == 0 {
	//fi, err := os.Stat(n.path)
	//if err != nil {
	//	return err
	//}
	//st, ok := fi.Sys().(*syscall.Stat_t)
	//if !ok {
	//	return fmt.Errorf("unknown stat.Sys %T", fi.Sys())
	//}
	//if req.Valid&fuse.SetattrUid == 0 {
	//	req.Uid = st.Uid
	//} else {
	//	req.Gid = st.Gid
	//}
	//	}
	//	if err := os.Chown(n.path, int(req.Uid), int(req.Gid)); err != nil {
	//		return err
	//	}
	//}
	//if req.Valid&fuse.SetattrSize != 0 {
	//if err := os.Truncate(n.path, int64(req.Size)); err != nil {
	//	return err
	//}
	//log.Printf("Setattr %v size %v", f.Meta.Name, req.Size)
	//}

	//if req.Valid&fuse.SetattrAtime != 0 {
	//log.Printf("Setattr %v canot set atime", f.Meta.Name)
	//}
	//if req.Valid&fuse.SetattrMtime != 0 {
	//	log.Printf("Setattr %v cannot set mtime", f.Meta.Name)
	//}
	return nil
}

func (f *File) Open(ctx context.Context, req *fuse.OpenRequest, res *fuse.OpenResponse) (fs.Handle, error) {
	f.log.Debug("OP Open")
	defer f.log.Debug("OP Open END")
	if (f.data == nil || len(f.data) == 0) && len(f.Meta.Refs) > 0 {
		if req.Flags.IsReadOnly() || req.Flags.IsReadWrite() {
			f.log.Debug("Open with fakefile")
			f.FakeFile = filereader.NewFile(f.fs.bs, f.Meta)
			var err error
			f.data, err = ioutil.ReadAll(f.FakeFile)
			if err != nil {
				return nil, err
			}
		}
	}
	return f, nil
}

func (f *File) Release(ctx context.Context, req *fuse.ReleaseRequest) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.log.Debug("OP Release")
	defer f.log.Debug("OP Release END")
	if f.FakeFile != nil {
		f.FakeFile.Close()
		f.FakeFile = nil
	}
	// TODO(tsileo): maybe set the data to nil on close?
	// f.data = nil
	return nil
}

func (f *File) Fsync(ctx context.Context, req *fuse.FsyncRequest) error {
	f.log.Debug("OP Fsync")
	return nil
}

func (f *File) Read(ctx context.Context, req *fuse.ReadRequest, res *fuse.ReadResponse) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.log.Debug("OP Read", "offset", req.Offset, "size", req.Size)
	// if f.flushed && f.FakeFile != nil {
	// 	// if (f.data == nil || len(f.data) == 0) && f.FakeFile != nil {
	// 	if req.Offset >= int64(f.Meta.Size) {
	// 		return nil
	// 	}
	// 	buf := make([]byte, req.Size)
	// 	n, err := f.FakeFile.ReadAt(buf, req.Offset)
	// 	if err == io.EOF {
	// 		err = nil
	// 	}
	// 	if err != nil {
	// 		return fuse.EIO
	// 	}
	// 	res.Data = buf[:n]
	// } else {
	fuseutil.HandleRead(req, res, f.data)
	// }
	return nil
}
