// Hellofs implements a simple "hello world" file system.
package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"bytes"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	_ "bazil.org/fuse/fs/fstestutil"
	"github.com/tsileo/blobsnap/clientutil"
	"github.com/tsileo/blobstash/client"
	"golang.org/x/net/context"
)

var Usage = func() {
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  %s MOUNTPOINT\n", os.Args[0])
	flag.PrintDefaults()
}

func main() {
	flag.Usage = Usage
	flag.Parse()

	if flag.NArg() != 1 {
		Usage()
		os.Exit(2)
	}
	mountpoint := flag.Arg(0)
	bs := client.NewBlobStore("")
	kvs := client.NewKvStore("")
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

	err = fs.Serve(c, &FS{bs: bs, kvs: kvs, uploader: clientutil.NewUploader(bs, kvs)})
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
	kvs      *client.KvStore
	bs       *client.BlobStore
	uploader *clientutil.Uploader
}

func (f *FS) Root() (fs.Node, error) {
	// FIXME load/store the root from kvs
	root := &Dir{fs: f, Name: "_root", Children: map[string]*clientutil.Meta{}}
	if err := root.Save(); err != nil {
		return nil, err
	}
	return root, nil
}

// Dir implements both Node and Handle for the root directory.
type Dir struct {
	fs   *FS
	meta *clientutil.Meta

	Name     string
	Hash     string
	Children map[string]*clientutil.Meta
}

func NewDir(fs *FS, meta *clientutil.Meta) *Dir {
	d := &Dir{
		fs:       fs,
		meta:     meta,
		Name:     meta.Name,
		Hash:     meta.Hash,
		Children: map[string]*clientutil.Meta{},
	}
	for _, ref := range d.meta.Refs {
		m, err := clientutil.NewMetaFromBlobStore(fs.bs, ref.(string))
		if err != nil {
			panic(err)
		}
		d.Children[m.Hash] = m
	}
	return d
}

func (d *Dir) Attr(ctx context.Context, a *fuse.Attr) error {
	a.Inode = 1
	a.Mode = os.ModeDir | 0555
	return nil
}

func (d *Dir) Lookup(ctx context.Context, name string) (fs.Node, error) {
	if c, ok := d.Children[name]; ok {
		if c.IsFile() {
			return NewFile(d.fs, c), nil
		} else {
			return NewDir(d.fs, c), nil
		}
	}
	//if name == "hello" {
	//	return File{}, nil
	//}
	return nil, fuse.ENOENT
}

var dirDirs = []fuse.Dirent{
	{Inode: 2, Name: "hello", Type: fuse.DT_File},
}

func (d *Dir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
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
	newdir := &Dir{Name: req.Name, fs: d.fs, Children: map[string]*clientutil.Meta{}}
	if err := newdir.Save(); err != nil {
		log.Printf("newdir err: %v", err)
		return nil, err
	}
	d.Children[newdir.meta.Hash] = newdir.meta
	log.Printf("%+v %+v", d, newdir)
	d.meta.AddRef(newdir.Hash)
	if err := d.Save(); err != nil {
		return nil, err
	}
	return newdir, nil
}

func (d *Dir) Save() error {
	log.Printf("Dir.Save %+v", d)
	m := clientutil.NewMeta()
	m.Type = "dir"
	m.Name = d.Name
	m.Mode = uint32(os.ModeDir | 0555)
	m.ModTime = time.Now().Format(time.RFC3339)
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
	d.Hash = mhash
	log.Printf("Dir.Save end %+v", d)
	return nil
}
func (d *Dir) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
	m := clientutil.NewMeta()
	m.Type = "file"
	m.Name = req.Name
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
	d.meta = m
	d.Hash = mhash

	time.Sleep(100 * time.Millisecond)
	f := NewFile(d.fs, m)
	d.Children[m.Hash] = m
	d.meta.AddRef(m.Hash)
	if err := d.Save(); err != nil {
		return nil, nil, err
	}
	return f, f, nil
}

type File struct {
	fs       *FS
	buf      *bytes.Buffer
	Meta     *clientutil.Meta
	FakeFile *clientutil.FakeFile
}

func NewFile(fs *FS, meta *clientutil.Meta) *File {
	meta, err := clientutil.NewMetaFromBlobStore(fs.bs, meta.Hash)
	if err != nil {
		panic(err)
	}
	return &File{
		fs:   fs,
		Meta: meta,
		buf:  &bytes.Buffer{},
	}
}
func (f *File) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	log.Printf("Write %v %v", req.Offset, len(req.Data))
	if req.Offset != int64(f.buf.Len()) {
		return fmt.Errorf("failed")
	}
	n, err := f.buf.Write(req.Data)
	if err != nil {
		return err
	}
	resp.Size = n
	return nil
}

type ClosingBuffer struct {
	*bytes.Buffer
}

func (*ClosingBuffer) Close() error {
	return nil
}
func (f *File) Flush(ctx context.Context, req *fuse.FlushRequest) error {
	log.Printf("FLUSH")
	m2, _, err := f.fs.uploader.PutReader(f.Meta.Name, &ClosingBuffer{f.buf})
	f.buf.Reset()
	if err != nil {
		return err
	}
	f.Meta = m2
	return nil
}

func (f *File) Attr(ctx context.Context, a *fuse.Attr) error {
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

func (f *File) Open(ctx context.Context, req *fuse.OpenRequest, res *fuse.OpenResponse) (fs.Handle, error) {
	log.Printf("OPEN")
	f.FakeFile = clientutil.NewFakeFile(f.fs.bs, f.Meta)
	return f, nil
}

func (f *File) Release(ctx context.Context, req *fuse.ReleaseRequest) error {
	if f.FakeFile != nil {
		f.FakeFile.Close()
		f.FakeFile = nil
	}
	return nil
}
func (f *File) Fsync(ctx context.Context, req *fuse.FsyncRequest) error {
	return nil
}
func (f *File) Read(ctx context.Context, req *fuse.ReadRequest, res *fuse.ReadResponse) error {
	log.Printf("Read %+v", f)
	if req.Offset >= int64(f.Meta.Size) {
		return nil
	}
	buf := make([]byte, req.Size)
	n, err := f.FakeFile.ReadAt(buf, req.Offset)
	if err == io.EOF {
		err = nil
	}
	if err != nil {
		log.Printf("Error reading FakeFile %+v on %v at %d: %v", f, f.Meta.Hash, req.Offset, err)
		return fuse.EIO
	}
	res.Data = buf[:n]
	return nil
}
