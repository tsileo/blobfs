# coding: utf-8
import os
import random
import string
import time
import hashlib
import shutil
from subprocess import Popen, PIPE, check_output

PATH = os.path.dirname(os.path.realpath(__file__))
FILE_SIZE = 5 * 2 ** 20  # 5MB file by default


def random_name(l=10):
    """Returns a random string."""
    # TODO(tsileo): update the choice to all the accepted chars for a filename
    return ''.join([random.choice(string.ascii_lowercase) for c in xrange(l)])


class BlobFS(object):
    def __init__(self, rebuild=True):
        self.process = None
        self.fs_name = random_name()
        os.makedirs(self.fs_name)

        self.mnt = os.path.join(PATH, self.fs_name)
        if rebuild:
            self._build()

    def _build(self):
        """Build the `blobsfs-mount` binary in the CWD"""
        p = Popen(['go', 'build', './cmd/blobfs-mount/blobfs-mount.go'], env=os.environ)
        if p.wait():
            raise Exception('failed')

    def mount(self):
        """Execute `blobsfs-mount {fs_name} {fs_name}` and return the running process."""
        self.process = Popen(['./blobfs-mount', '-loglevel', 'debug', self.fs_name, self.fs_name])
        time.sleep(1)
        if self.process.poll():
            raise Exception('failed to mount')

    def cleanup(self):
        """Cleanup func."""
        try:
            os.remove('/tmp/blobfs_{}.sock'.format(self.fs_name))
        except:
            pass

    def unmount(self):
        # Unmount the FS if it's currently mounted
        if self.process:
            self.process.terminate()
            self.process.wait()


class File(object):
    def __init__(self, path, hexhash):
        self.path = path
        self.hexhash = hexhash
        self.basename = os.path.basename(path)

    @staticmethod
    def _write_random_data(path, size=FILE_SIZE):
        """Open the file and write random data.

        Returns the hexhash.
        """
        content = os.urandom(size)
        h = hashlib.sha1()
        h.update(content)
        hexhash = h.hexdigest()

        with open(path, 'wb+') as f:
            f.write(content)

        return hexhash

    @classmethod
    def from_random(cls, path, size=FILE_SIZE):
        """Create a file filled with random data of the given size."""
        hexhash = cls._write_random_data(path, size)
        return cls(path, hexhash)

    def edit(self, size=FILE_SIZE):
        """Update the file content with random data and update the hexhash."""
        self.hexhash = self._write_random_data(self.path, size)


    def read_and_check(self):
        """Read the file and ensure the hexhash matches."""
        with open(self.path, 'rb') as f:
            content = ''
            h = hashlib.sha1()
            while 1:
                buf = f.read()
                h.update(buf)
                content += buf
                if not buf:
                    break
            hexhash = h.hexdigest()

        assert self.hexhash == hexhash

    def remove(self):
        """Remove the file."""
        os.remove(self.path)

    def move(self, path):
        """Move the file to the given path (abs path expected)."""
        if not os.path.isabs(path):
            raise Exception('path for move op must be abosulte, got %{}'.format(path))

        os.rename(self.path, path)
        self.path = path
        self.basename = os.path.basename(self.path)

class Dir(object):
    def __init__(self, path):
        self.path = path
        self.basename = os.path.basename(path)

    def list(self):
        """Returns the directory content."""
        return os.listdir(self.path)

    def remove(self):
        """Remove the directory tree."""
        shutil.rmtree(self.path)

    def move(self, path):
        """Move the file to the given path (abs path expected)."""
        if not os.path.isabs(path):
            raise Exception('path for move op must be abosulte, got %{}'.format(path))

        os.rename(self.path, path)
        self.path = path
        self.basename = os.path.basename(self.path)

    def create_dir(self, name=None):
        if name is None:
            name = random_name()
        path = os.path.join(self.path, name)
        os.mkdir(path)
        return Dir(path)

    def create_file(self, name=None, size=FILE_SIZE):
        if name is None:
            name = random_name()
        path = os.path.join(self.path, name)
        return File.from_random(path)

blobfs = BlobFS()
blobfs.cleanup()

mnt = blobfs.mnt
mnt = blobfs.mnt
blobfs.mount()

try:
    root_dir = Dir(mnt)

    # Ensure the directory is empty
    assert len(root_dir.list()) == 0

    # Create a file
    f1 = root_dir.create_file()
    # Ensure we can see the newly created file in the root dir
    assert root_dir.list() == [f1.basename]

    f1.read_and_check()

    d1 = root_dir.create_dir()

    assert len(root_dir.list()) == 2

    # Check that we get EEXIST (17) error when creating a file that already exists
    try:
        root_dir.create_dir(name=d1.basename)
    except OSError as exc:
        assert exc.errno == 17

    f2 = d1.create_file()

    f2.read_and_check()

    f2.move(os.path.join(mnt, random_name()))

    f2.read_and_check()

    f2.move(os.path.join(mnt, d1.basename, random_name()))

    f2.read_and_check()


    # from IPython import embed; embed()

finally:
    blobfs.unmount()
