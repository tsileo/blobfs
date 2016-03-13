package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"syscall"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"bazil.org/fuse/fuseutil"
	"github.com/tsileo/blobfs/pkg/root"
	"github.com/tsileo/blobstash/client/blobstore"
	"github.com/tsileo/blobstash/client/blobstore/cache"
	"github.com/tsileo/blobstash/client/kvstore"
	"github.com/tsileo/blobstash/ext/filetree/filetreeutil/meta"
	"github.com/tsileo/blobstash/ext/filetree/reader/filereader"
	"github.com/tsileo/blobstash/ext/filetree/writer"
	"golang.org/x/net/context"
	"gopkg.in/inconshreveable/log15.v2"
)

// FIXME when saving with vim, content not available on first read?
// FIXME(tsileo): debug the rename op
// TODO(tsileo): use the client blobstore cache
// TODO(tsileo): add a mutex for directory and an open bool

const maxInt = int(^uint(0) >> 1)

var Usage = func() {
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  %s NAME MOUNTPOINT\n", os.Args[0])
	flag.PrintDefaults()
}

var Log = log15.New()
var stats *Stats

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

	stats = &Stats{}
	go func() {
		t := time.NewTicker(10 * time.Second)
		for _ = range t.C {
			if stats.updated {
				fslog.Info(stats.String())
				stats.Reset()
			}
		}
	}()

	fslog.Info("Mouting fs...", "mountpoint", mountpoint, "immutable", *immutablePtr)
	bsOpts := blobstore.DefaultOpts().SetHost(*hostPtr, os.Getenv("BLOBSTASH_API_KEY"))
	bsOpts.SnappyCompression = false
	bs := cache.New(bsOpts, "blobfs_cache")
	kvsOpts := kvstore.DefaultOpts().SetHost(*hostPtr, os.Getenv("BLOBSTASH_API_KEY"))
	kvsOpts.SnappyCompression = false
	kvs := kvstore.New(kvsOpts)

	//ls.Ls(kvs)
	//os.Exit(2)
	c, err := fuse.Mount(
		mountpoint,
		fuse.FSName("blobfs"),
		fuse.Subtype("blobfs"),
		fuse.LocalVolume(),
		fuse.VolumeName("BlobFS"),
	)
	if err != nil {
		fslog.Crit("failed to mount", "err", err)
		os.Exit(1)
	}
	defer c.Close()

	err = fs.Serve(c, &FS{
		log:       fslog,
		name:      name,
		bs:        bs,
		kvs:       kvs,
		uploader:  writer.NewUploader(bs),
		immutable: *immutablePtr,
	})
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
}

type Stats struct {
	FilesCreated int
	DirsCreated  int
	FilesUpdated int
	DirsUpdated  int
	updated      bool
	sync.Mutex
}

func (s *Stats) Reset() {
	s.FilesCreated = 0
	s.DirsCreated = 0
	s.FilesUpdated = 0
	s.DirsUpdated = 0
	s.updated = false
}
func (s *Stats) String() string {
	return fmt.Sprintf("%d files created, %d dirs created", s.FilesCreated, s.DirsCreated)
}

type FS struct {
	log       log15.Logger
	kvs       *kvstore.KvStore
	bs        *cache.Cache // blobstore.BlobStore
	uploader  *writer.Uploader
	immutable bool
	name      string
}

func (f *FS) Immutable() bool {
	return f.immutable
}

func (f *FS) Name() string {
	return f.name
}

var rootKeyFmt = "blobfs:root:%v"

func (f *FS) Root() (fs.Node, error) {
	kv, err := f.kvs.Get(fmt.Sprintf(rootKeyFmt, f.Name()), -1)
	switch err {
	case nil:
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
		return NewDir(f, m, nil)
	case kvstore.ErrKeyNotFound:
		root := &Dir{
			fs:        f,
			Name:      "_root",
			Children:  map[string]*meta.Meta{},
			Children2: map[string]fs.Node{},
		}
		root.log = f.log.New("ref", "undefined", "name", "_root", "type", "dir")
		if err := root.save(); err != nil {
			return nil, err
		}
		f.log.Debug("Creating a new root", "ref", root.meta.Hash)
		root.log = root.log.New("ref", root.meta.Hash[:10])
		return root, nil
	default:
		return nil, err
	}
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
		log:       rfs.log.New("ref", m.Hash[:10], "name", m.Name, "type", "dir"),
	}
	for _, ref := range d.meta.Refs {
		blob, err := rfs.bs.Get(ref.(string))
		if err != nil {
			return nil, err
		}
		m, err := meta.NewMetaFromBlob(ref.(string), blob)
		if err != nil {
			return d, err
		}
		d.Children[m.Name] = m
		if m.IsDir() {
			ndir, err := NewDir(rfs, m, d)
			if err != nil {
				return nil, err
			}
			d.Children2[m.Name] = ndir
		} else {
			nfile, err := NewFile(rfs, m, d)
			if err != nil {
				return nil, err
			}
			d.Children2[m.Name] = nfile
		}
	}
	return d, nil
}

func (d *Dir) Attr(ctx context.Context, a *fuse.Attr) error {
	a.Inode = 1
	a.Mode = os.ModeDir | 0555
	return nil
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
		if err := d.save(); err != nil {
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
		if err := ndir.save(); err != nil {
			return err
		}
		return nil
	}
	return fuse.EIO
}

func (d *Dir) Lookup(ctx context.Context, name string) (fs.Node, error) {
	d.log.Debug("OP Lookup", "name", name)
	defer d.log.Debug("OP Lookup END", "name", name)
	// check if debug data is requested.
	switch {
	case name == ".blobfs":
		// .blobfs is requested
		// dump the current dir debug data
		return newDebugFile(d.meta.Hash), nil
	case strings.HasSuffix(name, ".blobfs"):
		// returns a file meta data
		name = strings.Replace(name, ".blobfs", "", 1)
		if c, ok := d.Children[name]; ok {
			if c.IsFile() {
				return newDebugFile(c.Hash), nil
			}
		}
	}
	// normal lookup operation
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
	}
	newdir.log = d.fs.log.New("ref", "unknown", "name", req.Name, "type", "dir")
	if err := newdir.save(); err != nil {
		return nil, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	d.Children[newdir.Name] = newdir.meta
	if err := d.save(); err != nil {
		return nil, err
	}
	newdir.log = newdir.log.New("ref", newdir.meta.Hash[:10])

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
	if err := d.save(); err != nil {
		d.log.Error("Failed to saved", "err", err)
		return err
	}
	return nil
}

func (d *Dir) save() error {
	d.log.Debug("saving")
	m := meta.NewMeta()
	m.Type = "dir"
	m.Name = d.Name
	m.Mode = uint32(os.ModeDir | 0555)
	m.ModTime = time.Now().Format(time.RFC3339)
	for _, c := range d.Children {
		m.AddRef(c.Hash)
	}
	mhash, mjs := m.Json()
	m.Hash = mhash
	d.meta = m
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
	if d.parent != nil {
		d.parent.mu.Lock()
		defer d.parent.mu.Unlock()
		d.parent.Children[d.Name] = m
		if err := d.parent.save(); err != nil {
			return err
		}
	} else {
		// If no parent, this is the root so save the ref
		root := root.New(mhash)
		js, err := json.Marshal(root)
		if err != nil {
			return err
		}
		if _, err := d.fs.kvs.Put(fmt.Sprintf(rootKeyFmt, d.fs.Name()), string(js), -1); err != nil {
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
	if err := d.save(); err != nil {
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
	data     []byte // FIXME if data grows too much, use a temp file
	Meta     *meta.Meta
	FakeFile *filereader.File
	log      log15.Logger
	parent   *Dir
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
		parent: parent,
		fs:     fs,
		Meta:   m,
		log:    fs.log.New("ref", m.Hash[:10], "name", m.Name, "type", "file"),
	}, nil
}

func (f *File) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	f.mu.Lock()
	defer f.mu.Unlock()
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
	if f.data != nil && len(f.data) > 0 {
		buf := bytes.NewBuffer(f.data)
		m2, err := f.fs.uploader.PutReader(f.Meta.Name, &ClosingBuffer{buf})
		// f.log.Debug("WriteResult", "wr", wr)
		if err != nil {
			return err
		}
		f.parent.mu.Lock()
		defer f.parent.mu.Unlock()
		f.parent.Children[m2.Name] = m2
		if err := f.parent.save(); err != nil {
			return err
		}
		f.Meta = m2
		f.log = f.log.New("ref", m2.Hash[:10])
		f.log.Debug("Flushed")
	}
	return nil
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
	if req.Flags.IsReadOnly() || req.Flags.IsReadWrite() {
		f.log.Debug("Open with fakefile")
		f.FakeFile = filereader.NewFile(f.fs.bs, f.Meta)
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
	f.log.Debug("OP Read", "offset", req.Offset, "size", req.Size)
	if f.FakeFile != nil {
		// if (f.data == nil || len(f.data) == 0) && f.FakeFile != nil {
		if req.Offset >= int64(f.Meta.Size) {
			return nil
		}
		buf := make([]byte, req.Size)
		n, err := f.FakeFile.ReadAt(buf, req.Offset)
		if err == io.EOF {
			err = nil
		}
		if err != nil {
			return fuse.EIO
		}
		res.Data = buf[:n]
	} else {
		f.mu.Lock()
		defer f.mu.Unlock()
		fuseutil.HandleRead(req, res, f.data)
	}
	return nil
}
