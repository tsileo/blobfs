package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"os/user"
	_ "path/filepath"
	"runtime"
	"strconv"
	"sync"
	"syscall"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"bazil.org/fuse/fuseutil"
	"github.com/tsileo/blobfs/pkg/cache"
	"github.com/tsileo/blobfs/pkg/root"
	"github.com/tsileo/blobstash/pkg/client/blobstore"
	"github.com/tsileo/blobstash/pkg/client/kvstore"
	_ "github.com/tsileo/blobstash/pkg/config/pathutil"
	"github.com/tsileo/blobstash/pkg/filetree/filetreeutil/meta"
	"github.com/tsileo/blobstash/pkg/filetree/reader/filereader"
	"github.com/tsileo/blobstash/pkg/filetree/writer"
	"github.com/tsileo/blobstash/pkg/vkv"
	"golang.org/x/net/context"
	"gopkg.in/inconshreveable/log15.v2"
)

// FIXME(tsileo): moving to an unexisting directory make the process hang?
// TODO(tsileo): use fs func for invalidating kernel cache
// TODO(tsileo): don't use defer for dir Mutex
// TODO(tsileo): no more name in Dir/File, read it from the meta
// TODO(tsileo): update the README (no more CVS feature like commit)
// TODO(tsileo): conditional request on the remote kvstore
// TODO(tsileo): cleanup the root handling
// TODO(tsileo): improve sync, better locking, check that x minutes without activity before sync
// and only scan the hash needed
// TODO(tsileo): use the garbage collector
// TODO(tsileo): handle setattr, user, ctime/atime, mode check by user

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

func (api *API) Serve(socketPath string) error {
	http.HandleFunc("/ref", apiRefHandler)
	http.HandleFunc("/sync", apiSyncHandler)
	http.HandleFunc("/log", apiLogHandler)
	http.HandleFunc("/public", apiPublicHandler)
	l, err := net.Listen("unix", socketPath)
	if err != nil {
		panic(err)
	}
	defer func() {
		l.Close()
		os.Remove(socketPath)
	}()
	if err := http.Serve(l, nil); err != nil {
		panic(err)
	}
	return nil
}

type NodeStatus struct {
	Type string
	Path string
	Ref  string
}

func apiRefHandler(w http.ResponseWriter, r *http.Request) {
	WriteJSON(w, map[string]string{"ref": bfs.mount.ref})
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
	var immutable bool
	if cr.Ref == bfs.latest.ref {
		immutable = true
	}
	if err := bfs.setRoot(cr.Ref, immutable); err != nil {
		panic(err)
	}
	WriteJSON(w, cr)
}

func apiSyncHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "POST request expected", http.StatusMethodNotAllowed)
		return
	}
	bfs.sync <- struct{}{}
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
	if err := iterDir(rootDir, func(node Node) error {
		if node.Meta().IsPublic() {
			out[node.Meta().Hash] = node.Meta()
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
	out := []*CommitLog{}

	versions, err := bfs.kvs.Versions(fmt.Sprintf(rootKeyFmt, bfs.name), 0, -1, 0)
	switch err {
	case kvstore.ErrKeyNotFound:
	case nil:
		for _, v := range versions.Versions {
			croot := &root.Root{}
			if err := json.Unmarshal(v.Data, croot); err != nil {
				panic(err)
			}
			cl := &CommitLog{
				T:       time.Unix(0, int64(v.Version)).Format(time.RFC3339),
				Ref:     croot.Ref,
				Comment: croot.Comment,
			}
			out = append(out, cl)
		}
	default:
		panic(err)
	}
	WriteJSON(w, out)
}

// iterDir executes the given callback `cb` on each nodes (file or dir) recursively.
func iterDir(dir *Dir, cb func(n Node) error) error {
	for _, node := range dir.Children {
		if node.IsDir() {
			if err := iterDir(node.(*Dir), cb); err != nil {
				return err
			}
		} else {
			if err := cb(node); err != nil {
				return err
			}
		}
	}
	return cb(dir)
}

func getLatestRemoteRoot() (*root.Root, error) {
	rkv, err := bfs.kvs.Get(fmt.Sprintf(rootKeyFmt, bfs.Name()), -1)
	if err != nil {
		return nil, err
	}
	return root.NewFromJSON(rkv.Data)
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
			// fslog.Info(fmt.Sprintf("latest=%+v,staging=%+v,mount=%+v", bfs.latest, bfs.staging, bfs.mount))
			if stats.updated {
				fslog.Info(stats.String())
				fslog.Debug("Flushing stats")
				stats.Reset()
			}
		}
	}()
	// FIXME(tsileo): re-enable, and do the update only if it's been 10 minutes without any activity
	// go func() {
	// 	t := time.NewTicker(10 * time.Minute)
	// 	for _ = range t.C {
	// 		fslog.Debug("trigger sync")
	// 		bfs.sync <- struct{}{}
	// 	}
	// }()

	sockPath := fmt.Sprintf("/tmp/blobfs_%s.sock", name)

	go func() {
		api := &API{}
		// TODO(tsileo): make the API port configurable
		fslog.Info("Starting API at localhost:8049")
		if err := api.Serve(sockPath); err != nil {
			fslog.Crit("failed to start API")
		}
	}()

	fslog.Info("Mouting fs...", "mountpoint", mountpoint, "immutable", *immutablePtr)
	bsOpts := blobstore.DefaultOpts().SetHost(*hostPtr, os.Getenv("BLOBSTASH_API_KEY"))
	bsOpts.SnappyCompression = false
	bs, err := cache.New(bsOpts, fmt.Sprintf("blobfs_cache_%s", name))
	if err != nil {
		fslog.Crit("failed to init cache", "err", err)
		os.Exit(1)
	}
	kvsOpts := kvstore.DefaultOpts().SetHost(*hostPtr, os.Getenv("BLOBSTASH_API_KEY"))
	kvsOpts.SnappyCompression = false
	kvs := kvstore.New(kvsOpts)

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

	// TODO(tsileo): switch to this kv
	// kv, err := vkv.New(filepath.Join(pathutil.VarDir(), "blobsfs", name, "lkv"))
	// defer kv.Close()
	// if err != nil {
	// 	panic(err)
	// }
	cuser, err := user.Current()
	if err != nil {
		fslog.Crit("failed to get current user", "err", err)
		os.Exit(1)
	}
	iuid, err := strconv.Atoi(cuser.Uid)
	if err != nil {
		panic(err)
	}
	igid, err := strconv.Atoi(cuser.Gid)
	if err != nil {
		panic(err)
	}

	bfs = &FS{
		log:        fslog,
		socketPath: sockPath,
		name:       name,
		bs:         bs,
		uid:        uint32(iuid),
		gid:        uint32(igid),
		// lkv:        kv,
		kvs:       kvs,
		uploader:  writer.NewUploader(bs),
		immutable: *immutablePtr,
		host:      bsOpts.Host,
		sync:      make(chan struct{}),
		latest:    &Mount{},
		mount:     &Mount{},
	}

	go func() {
		for {
			sync := func() ([]string, error) {
				wg.Add(1)
				defer wg.Done()
				l := fslog.New("module", "sync")
				l.Debug("Sync triggered")
				refs := []string{}

				// XXX(tsileo): a getRoot to DRY?
				// rroot, _ := getLatestRemoteRoot()

				// if err != kvstore.ErrKeyNotFound {
				// l.Error("failed to fetch latest remote vkv entry", "err", err)
				// return
				// }
				// // FIXME(tsileo): a way to output the sync status to the HTTP handler
				// like: "already in sync"/"synced"/"conflict"
				// if rroot != nil && rroot.Ref != bfs.latest.ref {
				// 	l.Error("conflicted tree")
				// 	// FIXME(tsileo): return a conflicted status
				// 	return
				// }

				// Keep some basic stats about the on-going sync
				stats := &SyncStats{}

				if bfs == nil {
					l.Debug("bfs is nil")
					return nil, nil
				}

				bfs.mu.Lock()
				defer func() {
					l.Info("Sync done", "blobs_uploaded", stats.BlobsUploaded, "blobs_skipped", stats.BlobsSkipped)
					bfs.mu.Unlock()
				}()

				// kv, err := bfs.bs.Vkv().Get(fmt.Sprintf(rootKeyFmt, bfs.Name()), -1)
				// if err != nil {
				// 	l.Error("Sync failed (failed to fetch the local vkv entry)", "err", err)
				// 	return
				// }
				// l.Debug("last sync info", "current version", kv.Version, "lastRootVersion", bfs.lastRootVersion)
				// if kv.Version == bfs.lastRootVersion {
				// 	l.Info("Already in sync")
				// 	return
				// }
				// rkv, err := bfs.kvs.Get(fmt.Sprintf(rootKeyFmt, bfs.Name()), -1)
				// if err != nil {
				// 	if err != kvstore.ErrKeyNotFound {
				// 		l.Error("Sync failed (failed to fetch the remote vkv entry)", "err", err)
				// 		return
				// 	}
				// }

				// if rkv != nil && rkv.Version == kv.Version {
				// 	l.Info("Already in sync")
				// 	return
				// }

				oroot, err := bfs.Root()
				if err != nil {
					l.Error("Failed to fetch root", "err", err)
					return nil, err
				}
				rootDir := oroot.(*Dir)
				// putBlob will try to upload all missing blobs to the remote BlobStash instance
				// putBlob := func(l log15.Logger, hash string, blob []byte) error {
				// 	mexists, err := bfs.bs.StatRemote(hash)
				// 	if err != nil {
				// 		l.Error("stat failed", "err", err)
				// 		return err
				// 	}
				// 	if mexists {
				// 		stats.BlobsSkipped++
				// 	} else {
				// 		// Fetch the blob locally via the cache if needed
				// 		if blob == nil {
				// 			blob, err = bfs.bs.Get(context.TODO(), hash)
				// 			if err != nil {
				// 				return err
				// 			}

				// 		}
				// 		l.Debug("Uploading blob", "hash", hash)
				// 		if err := bfs.bs.PutRemote(hash, blob); err != nil {
				// 			l.Error("put failed", "err", err)
				// 			return err
				// 		}
				// 		stats.BlobsUploaded++
				// 	}
				// 	return nil
				// }
				if err := iterDir(rootDir, func(node Node) error {
					ref := node.Meta().Hash
					refs = append(refs, ref)
					if !node.IsDir() {
						for _, iref := range node.Meta().Refs {
							data := iref.([]interface{})
							ref := data[1].(string)
							refs = append(refs, ref)
						}
					}
					return nil
				}); err != nil {
					l.Error("iterDir failed", "err", err)
					return nil, err
				}
				// bfs.lastRootVersion = kv.Version
				// Save the vkv entry in the remote vkv API
				// newRoot := &root.Root{}
				// if err := json.Unmarshal([]byte(kv.Data), newRoot); err != nil {
				// return
				// }
				// XXX(tsileo: log the host?
				// newRoot.Comment = sr.Comment
				// newValue, err := json.Marshal(newRoot)
				// if err != nil {
				// 	return
				// }
				// if _, err := bfs.kvs.Put(kv.Key, "", newValue, kv.Version); err != nil {
				// 	l.Error("Sync failed (failed to update the remote vkv entry)", "err", err)
				// 	return
				// }
				// bfs.latest.ref = newRoot.Ref
				// bfs.latest.root = rootDir
				return refs, nil
			}

			select {
			case <-bfs.sync:
				fslog.Info("Sync triggered")
				sync()
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
	return fmt.Sprintf("%d files created, %d dirs created, %d files updated, %d dirs updated",
		s.FilesCreated, s.DirsCreated, s.FilesUpdated, s.DirsUpdated)
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
	a.Inode = 0
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
	lkv             *vkv.DB
	bs              *cache.Cache // blobstore.BlobStore
	uploader        *writer.Uploader
	socketPath      string
	immutable       bool
	name            string
	host            string
	mu              sync.Mutex // Guard for the sync goroutine
	lastRootVersion int
	sync            chan struct{}
	lastWrite       time.Time
	root            fs.Node
	mount           *Mount
	latest          *Mount
	uid             uint32
	gid             uint32
}

type Mount struct {
	immutable bool
	root      fs.Node
	ref       string
}

func (m *Mount) Empty() bool {
	return m.ref == ""
}

func (m *Mount) Copy(m2 *Mount) {
	m2.immutable = m.immutable
	m2.root = m.root
	m2.ref = m.ref
}

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
	if f.root != nil {
		return f.root, nil
	}
	if f.mount.ref == "" {
		f.log.Info("loadRoot")
		d, err := f.loadRoot()
		if err != nil {
			return nil, err
		}
		f.root = d
		f.log.Debug("loaded root", "d", d)
		return d, nil
	}
	f.log.Info("Root OP", "root", f.root)
	return f.root, nil
}

func (f *FS) setRoot(ref string, immutable bool) error {
	f.log.Info("setRoot", "ref", ref)
	blob, err := f.bs.Get(context.TODO(), ref)
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
	// First, try to fetch the local root
	var rootNode *Dir
	var newRoot *Dir
	var localRoot *Dir
	var remoteRoot *Dir
	lkv, err := f.bs.Vkv().Get(fmt.Sprintf(rootKeyFmt, f.Name()), -1)
	switch err {
	case nil:
		lroot, err := root.NewFromJSON([]byte(lkv.Data))
		if err != nil {
			return nil, err
		}
		f.log.Debug("decoding root", "root", lroot)
		blob, err := f.bs.Get(context.TODO(), lroot.Ref)
		if err != nil {
			return nil, err
		}
		m, err := meta.NewMetaFromBlob(lroot.Ref, blob)
		if err != nil {
			return nil, err
		}
		f.log.Debug("loaded meta root", "ref", m.Hash)
		d, err := NewDir(f, m, nil)
		if err != nil {
			return nil, err
		}
		localRoot = d
		// f.mount = &Mount{immutable: f.immutable, root: d, ref: m.Hash}
		// f.mount.Copy(f.staging)
		// return d, nil
	case vkv.ErrNotFound:
	default:
		return nil, err
	}

	// Then, try to fetch the remote root
	kv, err := f.kvs.Get(fmt.Sprintf(rootKeyFmt, f.Name()), -1)
	switch err {
	case nil:
		f.lastRootVersion = kv.Version
		if lkv == nil {
			if f.bs.Vkv().Put(kv.Key, kv.Hash, kv.Data, kv.Version); err != nil {
				return nil, err
			}
		}
		rroot, err := root.NewFromJSON(kv.Data)
		if err != nil {
			return nil, err
		}
		blob, err := f.bs.Get(context.TODO(), rroot.Ref)
		if err != nil {
			return nil, err
		}
		m, err := meta.NewMetaFromBlob(rroot.Ref, blob)
		if err != nil {
			return nil, err
		}
		f.log.Debug("loaded meta root", "ref", m.Hash)
		d, err := NewDir(f, m, nil)
		if err != nil {
			return nil, err
		}
		remoteRoot = d
		cmount := &Mount{immutable: f.immutable, root: d, ref: m.Hash}
		// if f.staging.ref != "" {
		// 	f.mount = cmount
		// }
		cmount.Copy(f.latest)
		// return d, nil
	case kvstore.ErrKeyNotFound:
		if lkv == nil {
			newRoot = &Dir{
				fs:       f,
				Children: map[string]Node{},
				meta:     &meta.Meta{Name: "_root"},
			}
			newRoot.log = f.log.New("ref", "undefined", "name", "_root", "type", "dir")
			if err := newRoot.Save(); err != nil {
				return nil, err
			}
			f.log.Debug("Creating a new root", "ref", newRoot.meta.Hash)
		}
		// root.log = root.log.New("ref", root.meta.Hash)
		// f.mount = &Mount{immutable: f.immutable, root: root, ref: root.meta.Hash}
		// f.mount.Copy(f.latest)
	default:
		return nil, err
	}
	switch {
	case localRoot == nil && remoteRoot != nil:
		rootNode = remoteRoot
	case localRoot != nil && remoteRoot == nil:
		rootNode = localRoot
	case localRoot != nil && remoteRoot != nil:
		t1, err := localRoot.meta.Mtime()
		if err != nil {
			return nil, err
		}
		t2, err := remoteRoot.meta.Mtime()
		if err != nil {
			return nil, err
		}
		if t1.After(t2) {
			rootNode = localRoot
		} else {
			rootNode = remoteRoot
		}
	case newRoot != nil:
		rootNode = newRoot
	}
	f.log.Info("initial load", "mount", f.mount)
	f.mount = &Mount{immutable: f.immutable, root: rootNode, ref: rootNode.meta.Hash}
	f.mount.Copy(f.latest)
	return f.mount.root, nil
}

type Node interface {
	fs.Node
	Meta() *meta.Meta
	SetMeta(*meta.Meta)
	Save() error
	IsDir() bool
	Lock()
	Unlock()
}

// Dir implements both Node and Handle for the root directory.
type Dir struct {
	fs       *FS
	meta     *meta.Meta
	parent   *Dir
	Children map[string]Node
	log      log15.Logger
	mu       sync.Mutex
}

func NewDir(rfs *FS, m *meta.Meta, parent *Dir) (*Dir, error) {
	d := &Dir{
		fs:     rfs,
		meta:   m,
		parent: parent,
		log:    rfs.log.New("ref", m.Hash, "name", m.Name, "type", "dir"),
	}
	return d, nil
}

func (d *Dir) reload() error {
	// XXX(tsileo): should we assume the Mutex is locked?
	d.log.Info("Reload")
	d.Children = map[string]Node{}
	for _, ref := range d.meta.Refs {
		d.log.Debug("Trying to fetch ref", "hash", ref.(string))
		blob, err := d.fs.bs.Get(context.TODO(), ref.(string))
		if err != nil {
			return err
		}
		m, err := meta.NewMetaFromBlob(ref.(string), blob)
		if err != nil {
			return err
		}
		if m.IsDir() {
			ndir, err := NewDir(d.fs, m, d)
			if err != nil {
				return err
			}
			d.Children[m.Name] = ndir
		} else {
			nfile, err := NewFile(d.fs, m, d)
			if err != nil {
				return err
			}
			d.Children[m.Name] = nfile
		}
	}
	return nil
}

func (d *Dir) Lock() { d.mu.Lock() }

func (d *Dir) Unlock() { d.mu.Unlock() }

func (d *Dir) IsDir() bool { return true }

func (d *Dir) Meta() *meta.Meta { return d.meta }

func (d *Dir) SetMeta(m *meta.Meta) {
	d.meta = m
}

func (d *Dir) Attr(ctx context.Context, a *fuse.Attr) error {
	d.log.Debug("OP Attr")
	if d.parent == nil {
		a.Inode = 2
	} else {
		a.Inode = 0
	}
	a.Mode = os.ModeDir | 0555
	a.Uid = d.fs.uid
	a.Gid = d.fs.gid
	return nil
}

func makePublic(node Node, value string) error {
	if value == "1" {
		node.Meta().XAttrs["public"] = value
	} else {
		delete(node.Meta().XAttrs, "public")
	}
	// TODO(tsileo): too much mutations??
	if node.IsDir() {
		for _, child := range node.(*Dir).Children {
			if err := makePublic(child, value); err != nil {
				return err
			}
		}
	}
	if err := node.Save(); err != nil {
		return err
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
	if err := d.Save(); err != nil {
		return err
	}

	// // Trigger a sync so the file will be (un)available for BlobStash right now
	if req.Name == "public" {
		bfs.sync <- struct{}{}
	}
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
		if err := d.Save(); err != nil {
			return err
		}

		// // Trigger a sync so the file won't be available via BlobStash
		if req.Name == "public" {
			bfs.sync <- struct{}{}
		}

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

	if d.Children == nil {
		if err := d.reload(); err != nil {
			return err
		}
	}

	if node, ok := d.Children[req.OldName]; ok {
		var meta *meta.Meta
		switch node.(type) {
		case *Dir:
			meta = node.(*Dir).meta
		case *File:
			meta = node.(*File).meta
		}
		if err := d.fs.uploader.RenameMeta(meta, req.NewName); err != nil {
			return err
		}
		// Delete the source
		d.mu.Lock()
		delete(d.Children, req.OldName)
		d.mu.Unlock()

		ndir := newDir.(*Dir)
		if d != ndir {
			ndir.mu.Lock()
			ndir.Children[req.NewName] = node
			ndir.mu.Unlock()
		} else {
			d.Children[req.NewName] = node
		}

		if err := d.Save(); err != nil {
			return err
		}
		if d != ndir {
			if err := ndir.Save(); err != nil {
				return err
			}
		}
		return nil
	}
	return fuse.EIO
}

func (d *Dir) Lookup(ctx context.Context, name string) (fs.Node, error) {
	d.log.Debug("OP Lookup", "name", name)
	defer d.log.Debug("OP Lookup END", "name", name)

	// Magic file for returnign the socket path, available in every directory
	if name == ".blobfs_socket" {
		return newDebugFile(d.fs.socketPath), nil
	}

	// normal lookup operation
	if d.Children == nil {
		if err := d.reload(); err != nil {
			return nil, err
		}
	}

	if c, ok := d.Children[name]; ok {
		return c, nil
	}
	return nil, fuse.ENOENT
}

func (d *Dir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	d.log.Debug("OP ReadDirAll")
	defer d.log.Debug("OP ReadDirAll END")

	if d.Children == nil {
		if err := d.reload(); err != nil {
			return nil, err
		}
	}

	dirs := []fuse.Dirent{}
	for _, c := range d.Children {
		nodeType := fuse.DT_File
		if c.IsDir() {
			nodeType = fuse.DT_Dir
		}
		dirs = append(dirs, fuse.Dirent{
			Inode: 0,
			Name:  c.Meta().Name,
			Type:  nodeType,
		})
	}
	return dirs, nil
}

func (d *Dir) Mkdir(ctx context.Context, req *fuse.MkdirRequest) (fs.Node, error) {
	d.log.Debug("OP Mkdir", "name", req.Name)
	defer d.log.Debug("OP Mkdir END", "name", req.Name)

	if d.fs.Immutable() {
		return nil, fuse.EPERM
	}

	if d.Children == nil {
		if err := d.reload(); err != nil {
			return nil, err
		}
	}

	if _, ok := d.Children[req.Name]; ok {
		return nil, fuse.EEXIST
	}

	newdir := &Dir{
		fs:       d.fs,
		parent:   d,
		Children: map[string]Node{},
		meta:     &meta.Meta{Name: req.Name},
	}
	newdir.log = d.fs.log.New("ref", "unknown", "name", req.Name, "type", "dir")
	if err := newdir.Save(); err != nil {
		return nil, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.Children[newdir.meta.Name] = newdir
	if err := d.Save(); err != nil {
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

	if d.Children == nil {
		if err := d.reload(); err != nil {
			return err
		}
	}

	delete(d.Children, req.Name)
	if err := d.Save(); err != nil {
		d.log.Error("Failed to saved", "err", err)
		return err
	}
	return nil
}

func (d *Dir) Save() error {
	d.log.Debug("saving")
	m := meta.NewMeta()
	m.Name = d.meta.Name
	m.Type = "dir"
	m.Mode = uint32(os.ModeDir | 0555)
	m.ModTime = time.Now().Format(time.RFC3339)
	if d.meta.ModTime != "" {
		m.ModTime = d.meta.ModTime
	}
	for _, c := range d.Children {
		switch node := c.(type) {
		case *Dir:
			m.AddRef(node.meta.Hash)
		case *File:
			m.AddRef(node.meta.Hash)
		}
	}
	mhash, mjs := m.Json()
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
		if _, err := d.fs.bs.Vkv().Put(fmt.Sprintf(rootKeyFmt, d.fs.Name()), "", js, -1); err != nil {
			return err
		}
		d.fs.mount.ref = mhash
		d.fs.mount.root = d
	} else {
		d.parent.mu.Lock()
		defer d.parent.mu.Unlock()
		if err := d.parent.Save(); err != nil {
			return err
		}
	}
	return nil
}

func (d *Dir) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
	d.log.Debug("OP Create", "name", req.Name)
	defer d.log.Debug("OP Create END", "name", req.Name)
	if d.fs.Immutable() {
		return nil, nil, fuse.EPERM
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	if d.Children == nil {
		if err := d.reload(); err != nil {
			return nil, nil, err
		}
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
	f, err := NewFile(d.fs, m, d)
	if err != nil {
		return nil, nil, err
	}
	d.Children[m.Name] = f
	if err := d.Save(); err != nil {
		return nil, nil, err
	}
	stats.Lock()
	stats.updated = true
	stats.FilesCreated++
	stats.Unlock()
	return f, f, nil
}

type fileState struct {
	updated bool
}

type File struct {
	fs       *FS
	data     []byte // FIXME(tsileo): if data grows too much, use a temp file
	meta     *meta.Meta
	FakeFile *filereader.File
	log      log15.Logger
	parent   *Dir

	state *fileState
	mu    sync.Mutex
}

func NewFile(fs *FS, m *meta.Meta, parent *Dir) (*File, error) {
	return &File{
		parent: parent,
		fs:     fs,
		meta:   m,
		log:    fs.log.New("ref", m.Hash, "name", m.Name, "type", "file"),
		state:  &fileState{},
	}, nil
}

func (f *File) IsDir() bool { return false }

func (f *File) Meta() *meta.Meta { return f.meta }

func (f *File) SetMeta(m *meta.Meta) {
	f.meta = m
}

func (f *File) Lock() { f.mu.Lock() }

func (f *File) Unlock() { f.mu.Unlock() }

func (f *File) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.log.Debug("OP Write", "offset", req.Offset, "size", len(req.Data))
	defer f.log.Debug("OP Write END", "offset", req.Offset, "size", len(req.Data))
	if f.fs.Immutable() {
		return fuse.EPERM
	}
	f.state.updated = true
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
	if !f.state.updated {
		return nil
	}
	f.meta.Size = len(f.data)
	if f.data != nil && len(f.data) > 0 {
		// XXX(tsileo): data will be saved once the tree will be synced
		buf := bytes.NewBuffer(f.data)
		m2, err := f.fs.uploader.PutReader(f.meta.Name, &ClosingBuffer{buf})
		f.log.Debug("new meta", "meta", fmt.Sprintf("%+v", m2))
		// f.log.Debug("WriteResult", "wr", wr)
		if err != nil {
			return err
		}
		f.parent.mu.Lock()
		defer f.parent.mu.Unlock()
		if err := f.parent.Save(); err != nil {
			return err
		}
		f.meta = m2
		// f.log = f.log.New("ref", m2.Hash[:10])
		f.log.Debug("Flushed", "data_len", len(f.data))
	}
	f.state.updated = false
	return nil
}

func (f *File) Setxattr(ctx context.Context, req *fuse.SetxattrRequest) error {
	f.log.Debug("OP Setxattr", "name", req.Name, "xattr", string(req.Xattr))
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.fs.Immutable() {
		return nil
	}

	// Prevent writing attributes name that are virtual attributes
	if _, exists := virtualXAttrs[req.Name]; exists {
		return nil
	}

	if f.meta.XAttrs == nil {
		f.meta.XAttrs = map[string]string{}
	}
	f.meta.XAttrs[req.Name] = string(req.Xattr)
	// XXX(tsileo): check thath the parent get the updated hash?
	if err := f.Save(); err != nil {
		return err
	}
	// Trigger a sync so the file will be (un)available for BlobStash right now
	if req.Name == "public" {
		bfs.sync <- struct{}{}
	}
	return nil
}

func (f *File) Save() error {
	if f.fs.Immutable() {
		f.log.Warn("Trying to save an immutable node")
		return nil
	}

	// Update the new `Meta`
	f.parent.fs.uploader.PutMeta(f.meta)
	// And save the parent
	f.parent.mu.Lock()
	defer f.parent.mu.Unlock()
	if err := f.parent.Save(); err != nil {
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
	return handleListxattr(f.meta, resp)
}

func (f *File) Forget() {
	f.log.Debug("OP Forget")
}

func handleGetxattr(fs *FS, m *meta.Meta, req *fuse.GetxattrRequest, resp *fuse.GetxattrResponse) error {
	fs.log.Debug("handleGetxattr", "name", req.Name)
	// Check if the request match a virtual extended attributes
	if xattrFunc, ok := virtualXAttrs[req.Name]; ok && xattrFunc != nil {
		resp.Xattr = xattrFunc(m)
		return nil
	}

	if req.Name == "url.semiprivate" {
		client := fs.bs.Client()
		nodeResp, err := client.DoReq("HEAD", "/api/filetree/node/"+m.Hash+"?bewit=1", nil, nil)
		if err != nil {
			return err
		}
		if nodeResp.StatusCode != 200 {
			return fmt.Errorf("bad status code: %d", nodeResp.StatusCode)
		}
		bewit := nodeResp.Header.Get("BlobStash-FileTree-Bewit")
		raw_url := fmt.Sprintf("%s/%s/%s?bewit=%s", fs.host, m.Type[0:1], m.Hash, bewit)
		resp.Xattr = []byte(raw_url)
		return nil
	}

	if req.Name == "url" && m.IsPublic() {
		// Ensure the node is public
		// FIXME(tsileo): fetch the hostname from `bfs` to reconstruct an absolute URL
		// Output the URL
		raw_url := fmt.Sprintf("%s/%s/%s", fs.host, m.Type[0:1], m.Hash)
		resp.Xattr = []byte(raw_url)
		return nil
	}

	if m.XAttrs == nil {
		return fuse.ErrNoXattr
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
	return handleGetxattr(f.parent.fs, f.meta, req, resp)
}

func (f *File) Removexattr(ctx context.Context, req *fuse.RemovexattrRequest) error {
	f.log.Debug("OP Removexattr", "name", req.Name)
	f.mu.Lock()
	defer f.mu.Unlock()

	// Can't delete virtual attributes
	if _, exists := virtualXAttrs[req.Name]; exists {
		return fuse.ErrNoXattr
	}

	if f.meta.XAttrs == nil {
		return fuse.ErrNoXattr
	}

	if _, ok := f.meta.XAttrs[req.Name]; ok {
		// Delete the attribute
		delete(f.meta.XAttrs, req.Name)

		// Save the meta
		if err := f.Save(); err != nil {
			return err
		}
		// Trigger a sync so the file won't be available via BlobStash
		if req.Name == "public" {
			bfs.sync <- struct{}{}
		}
		return nil

	}
	return fuse.ErrNoXattr
}

func (f *File) Attr(ctx context.Context, a *fuse.Attr) error {
	f.log.Debug("OP Attr")
	defer f.log.Debug("OP Attr END")
	a.Inode = 0
	a.Mode = os.FileMode(f.meta.Mode)
	a.Uid = f.fs.uid
	a.Gid = f.fs.gid
	if f.fs.Immutable() || f.data == nil {
		a.Size = uint64(f.meta.Size)
	} else {
		a.Size = uint64(len(f.data))
	}
	if f.meta.ModTime != "" {
		t, err := time.Parse(time.RFC3339, f.meta.ModTime)
		if err != nil {
			panic(fmt.Errorf("error parsing mtime for %v: %v", f, err))
		}
		a.Mtime = t
	}
	f.log.Debug("attrs", "a", a)
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
	if (f.data == nil || len(f.data) == 0) && len(f.meta.Refs) > 0 {
		// if (req.Flags.IsReadOnly() || req.Flags.IsReadWrite()) && !f.state.updated {
		if !f.fs.Immutable() {
			f.log.Debug("Open with fakefile")
			f.FakeFile = filereader.NewFile(f.fs.bs, f.meta)
			// FIXME(tsileo): only if the buffer is small, or load a temp file?
			var err error
			f.data, err = ioutil.ReadAll(f.FakeFile)
			if err != nil {
				return nil, err
			}
		}
		// }
	}
	return f, nil
}

func (f *File) Release(ctx context.Context, req *fuse.ReleaseRequest) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	// XXX(tsileo): have a counter of open and only release when it's goes to 0?
	f.log.Debug("OP Release")
	defer f.log.Debug("OP Release END")
	if f.FakeFile != nil {
		f.FakeFile.Close()
		f.FakeFile = nil
	}
	f.data = nil
	return nil
}

func (f *File) Fsync(ctx context.Context, req *fuse.FsyncRequest) error {
	f.log.Debug("OP Fsync")
	// XXX(tsileo): flush the file?
	return nil
}

func (f *File) Read(ctx context.Context, req *fuse.ReadRequest, res *fuse.ReadResponse) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.log.Debug("OP Read", "offset", req.Offset, "size", req.Size)
	defer f.log.Debug("OP Read END", "offset", req.Offset, "size", req.Size)
	if req.Offset >= int64(f.meta.Size) {
		return nil
	}
	if f.fs.Immutable() {
		f.log.Debug("Reading from FakeFile")
		buf := make([]byte, req.Size)
		n, err := f.FakeFile.ReadAt(buf, req.Offset)
		if err == io.EOF {
			err = nil
		}
		if err != nil {
			return fuse.EIO
		}
		res.Data = buf[:n]
		return nil
	}

	f.log.Debug("Reading from memory")
	fuseutil.HandleRead(req, res, f.data)
	return nil
}
