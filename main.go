package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"syscall"
	"time"

	"bytes"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"bazil.org/fuse/fuseutil"
	"github.com/tsileo/blobsnap/clientutil"
	"github.com/tsileo/blobstash/client"
	"golang.org/x/net/context"
	"gopkg.in/inconshreveable/log15.v2"
)

// FIXME when saving with vim, content not available on first read?

const maxInt = int(^uint(0) >> 1)

var Usage = func() {
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  %s NAME MOUNTPOINT\n", os.Args[0])
	flag.PrintDefaults()
}

var Log = log15.New()

func main() {
	hostPtr := flag.String("host", "", "remote host, default to http://localhost:8050")
	loglevelPtr := flag.String("loglevel", "info", "logging level (debug|info|warn|crit)")
	immutablePtr := flag.Bool("immutable", false, "make the filesystem immutable")

	flag.Usage = Usage
	flag.Parse()

	if flag.NArg() != 2 {
		Usage()
		os.Exit(2)
	}
	name := flag.Arg(0)
	mountpoint := flag.Arg(1)

	lvl, err := log15.LvlFromString(*loglevelPtr)
	if err != nil {
		panic(err)
	}
	Log.SetHandler(log15.LvlFilterHandler(lvl, log15.StreamHandler(os.Stdout, log15.TerminalFormat())))

	fslog := Log.New("name", name)
	fslog.Info("Mouting fs...", "mountpoint", mountpoint, "immutable", *immutablePtr)
	bs := client.NewBlobStore(*hostPtr)
	kvs := client.NewKvStore(*hostPtr)
	c, err := fuse.Mount(
		mountpoint,
		fuse.FSName("blobfs"),
		fuse.Subtype("blobfs"),
		fuse.LocalVolume(),
		fuse.VolumeName("BlobFS"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()

	err = fs.Serve(c, &FS{
		log:       fslog,
		name:      name,
		bs:        bs,
		kvs:       kvs,
		uploader:  clientutil.NewUploader(bs, kvs),
		immutable: *immutablePtr,
	})
	if err != nil {
		log.Fatal(err)
	}

	// check if the mount process has an error to report
	<-c.Ready
	if err := c.MountError; err != nil {
		log.Fatal(err)
	}
}

// FS implements the hello world file system.
type FS struct {
	log       log15.Logger
	kvs       *client.KvStore
	bs        *client.BlobStore
	uploader  *clientutil.Uploader
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
		m, err := clientutil.NewMetaFromBlobStore(f.bs, kv.Value)
		if err != nil {
			return nil, err
		}
		f.log.Debug("loaded meta root", "ref", m.Hash)
		root := NewDir(f, m, nil)
		return root, nil
	case client.ErrKeyNotFound:
		root := &Dir{fs: f, Name: "_root", Children: map[string]*clientutil.Meta{}}
		if err := root.save(); err != nil {
			return nil, err
		}
		f.log.Debug("Creating a new root", "ref", root.meta.Hash)
		root.log = f.log.New("ref", root.meta.Hash[:10], "name", root.meta.Name, "type", "dir")
		return root, nil
	default:
		return nil, err
	}
}

// Dir implements both Node and Handle for the root directory.
type Dir struct {
	fs       *FS
	meta     *clientutil.Meta
	parent   *Dir
	Name     string
	Children map[string]*clientutil.Meta
	log      log15.Logger
}

func NewDir(fs *FS, meta *clientutil.Meta, parent *Dir) *Dir {
	d := &Dir{
		fs:       fs,
		meta:     meta,
		parent:   parent,
		Name:     meta.Name,
		Children: map[string]*clientutil.Meta{},
		log:      fs.log.New("ref", meta.Hash[:10], "name", meta.Name, "type", "dir"),
	}
	for _, ref := range d.meta.Refs {
		m, err := clientutil.NewMetaFromBlobStore(fs.bs, ref.(string))
		if err != nil {
			panic(err)
		}
		d.Children[m.Name] = m
	}
	return d
}

func (d *Dir) Attr(ctx context.Context, a *fuse.Attr) error {
	a.Inode = 1
	a.Mode = os.ModeDir | 0555
	return nil
}

func (d *Dir) Lookup(ctx context.Context, name string) (fs.Node, error) {
	d.log.Debug("OP Lookup", "name", name)
	if c, ok := d.Children[name]; ok {
		if c.IsFile() {
			return NewFile(d.fs, c, d), nil
		} else {
			return NewDir(d.fs, c, d), nil
		}
	}
	return nil, fuse.ENOENT
}

func (d *Dir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	d.log.Debug("OP ReadDirAll")
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
	if d.fs.Immutable() {
		return nil, fuse.EPERM
	}
	newdir := &Dir{
		fs:       d.fs,
		parent:   d,
		Name:     req.Name,
		Children: map[string]*clientutil.Meta{},
	}
	if err := newdir.save(); err != nil {
		return nil, err
	}
	d.Children[newdir.Name] = newdir.meta
	if err := d.save(); err != nil {
		return nil, err
	}
	newdir.log = d.fs.log.New("ref", newdir.meta.Hash[:10], "name", newdir.meta.Name, "type", "dir")
	return newdir, nil
}

func (d *Dir) Remove(ctx context.Context, req *fuse.RemoveRequest) error {
	d.log.Debug("OP Remove", "name", req.Name)
	if d.fs.Immutable() {
		return fuse.EPERM
	}
	delete(d.Children, req.Name)
	if err := d.save(); err != nil {
		return err
	}
	return nil
}

func (d *Dir) save() error {
	m := clientutil.NewMeta()
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
		log.Printf("stats failed %v", err)
		return err
	}
	if !mexists {
		if err := d.fs.bs.Put(mhash, mjs); err != nil {
			log.Printf("put failed %v", err)
			return err
		}
	}
	if d.parent != nil {
		d.parent.Children[d.Name] = m
		if err := d.parent.save(); err != nil {
			return err
		}
	} else {
		// If no parent, this is the root so save the ref
		if _, err := d.fs.kvs.Put(fmt.Sprintf(rootKeyFmt, d.fs.Name()), mhash, -1); err != nil {
			return err
		}
	}
	return nil
}

func (d *Dir) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
	d.log.Debug("OP Create", "name", req.Name)
	if d.fs.Immutable() {
		return nil, nil, fuse.EPERM
	}
	m := clientutil.NewMeta()
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
	time.Sleep(500 * time.Millisecond)
	f := NewFile(d.fs, m, d)
	d.Children[m.Name] = m
	if err := d.save(); err != nil {
		return nil, nil, err
	}
	return f, f, nil
}

type File struct {
	fs       *FS
	data     []byte
	Meta     *clientutil.Meta
	FakeFile *clientutil.FakeFile
	log      log15.Logger
	parent   *Dir
}

func NewFile(fs *FS, meta *clientutil.Meta, parent *Dir) *File {
	meta, err := clientutil.NewMetaFromBlobStore(fs.bs, meta.Hash)
	if err != nil {
		panic(err)
	}
	return &File{
		parent: parent,
		fs:     fs,
		Meta:   meta,
		log:    fs.log.New("ref", meta.Hash[:10], "name", meta.Name, "type", "file"),
	}
}
func (f *File) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	f.log.Debug("OP Write", "offset", req.Offset, "size", len(req.Data))
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
	f.log.Debug("OP Flush")
	if f.fs.Immutable() {
		return nil
	}
	if f.data != nil && len(f.data) > 0 {
		buf := bytes.NewBuffer(f.data)
		m2, _, err := f.fs.uploader.PutReader(f.Meta.Name, &ClosingBuffer{buf})
		if err != nil {
			return err
		}
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
	if req.Flags.IsReadOnly() || req.Flags.IsReadWrite() {
		f.log.Debug("Open with fakefile")
		f.FakeFile = clientutil.NewFakeFile(f.fs.bs, f.Meta)
	}
	return f, nil
}

func (f *File) Release(ctx context.Context, req *fuse.ReleaseRequest) error {
	f.log.Debug("OP Release")
	if f.FakeFile != nil {
		f.FakeFile.Close()
		f.FakeFile = nil
	}
	f.data = nil
	return nil
}

func (f *File) Fsync(ctx context.Context, req *fuse.FsyncRequest) error {
	f.log.Debug("OP Fsync")
	return nil
}

func (f *File) Read(ctx context.Context, req *fuse.ReadRequest, res *fuse.ReadResponse) error {
	f.log.Debug("OP Read", "offset", req.Offset, "size", req.Size)
	if (f.data == nil || len(f.data) == 0) && f.FakeFile != nil {
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
		fuseutil.HandleRead(req, res, f.data)
	}
	return nil
}
