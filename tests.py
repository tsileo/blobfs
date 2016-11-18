# coding: utf-8
"""Test suite for BlobFS.

A custom test suite allows the tests for be written at the same as new features.
A proper POSIX test suite may be added later once more features/calls are implemented.
"""
import os
import random
import string
import time
import hashlib
import shutil
from subprocess import Popen, PIPE, check_output

PATH = os.path.dirname(os.path.realpath(__file__))
FILE_SIZE = 3 * 2 ** 20  # 3MB file by default

# TODO(tsileo): test `blobfs` checkout, log
# TODO(tsileo): use pytest with custom error?
# TODO(tsileo): test that we can read the `.blobfs_socket` file


def random_name(l=10):
    """Returns a random string."""
    # TODO(tsileo): update the choice to all the accepted chars for a filename
    return ''.join([random.choice(string.ascii_lowercase) for c in xrange(l)])


class BlobStash(object):
    def __init__(self, rebuild=True):
        self.process = None

    def run(self):
        """Execute `blobsfs-mount {fs_name} {fs_name}` and return the running process."""
        self.process = Popen(['blobstash', './tests/blobstash.yaml'], env=os.environ)
        time.sleep(1)
        if self.process.poll():
            raise Exception('failed to mount')

    def cleanup(self):
        """Cleanup func."""
        try:
            shutil.rmtree('blobstash_data')
        except:
            pass

    def shutdown(self):
        if self.process:
            self.process.terminate()
            self.process.wait()


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

        p = Popen(['go', 'install', 'github.com/tsileo/blobfs/cmd/blobfs'], env=os.environ)
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
            shutil.rmtree()
        except:
            pass

    def unmount(self):
        # Unmount the FS if it's currently mounted
        if self.process:
            self.process.terminate()
            self.process.wait()

    def remove_data(self):
        shutil.rmtree(self._var_dir())

    def _var_dir(self):
        blobfs_var_dir = os.environ.get('BLOBFS_VAR_DIR')
        if blobfs_var_dir:
            return blobfs_var_dir
        return os.path.join(os.environ['HOME'], 'var', 'blobfs')

    def restart(self, remove_data=False):
        try:
            self.unmount()
        except OSError as exc:
            print 'Failed',  exc
        self.cleanup()
        if remove_data:
            self.remove_data()
        self.mount()


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

blobstash = BlobStash()
blobstash.cleanup()

blobstash.run()

blobfs = BlobFS()
blobfs.cleanup()

mnt = blobfs.mnt
blobfs.mount()

try:
    root_dir = Dir(mnt)

    # Ensure the directory is empty
    assert len(root_dir.list()) == 0

    # Create a file
    f1 = root_dir.create_file()
    # time.sleep(0.5)
    # Ensure we can see the newly created file in the root dir
    assert root_dir.list() == [f1.basename]

    f1.read_and_check()

    # Edit the file
    f1.edit()
    # time.sleep(0.5)

    f1.read_and_check()

    f3_name = random_name()
    f3_content = 'testing'
    with open(os.path.join(mnt, f3_name), 'wb+') as f3:
        f3.write(f3_content)
        f3.flush()
        # time.sleep(1)

        # Ensure we can see the content after the flush if we open the file again
        with open(os.path.join(mnt, f3_name)) as f3ro:
            f3ro_content = f3ro.read()
            print f3ro_content, f3_content
            assert f3ro_content == f3_content

        f3.write(f3_content)

    with open(os.path.join(mnt, f3_name)) as f3:
        assert f3.read() == f3_content * 2

    d1 = root_dir.create_dir()
    # time.sleep(0.5)

    # Ensure the file is created
    assert len(root_dir.list()) == 3

    # Check that we get EEXIST (17) error when creating a file that already exists
    eraised = False
    try:
        root_dir.create_dir(name=d1.basename)
    except OSError as exc:
        eraised = True
        assert exc.errno == 17

    assert eraised

    # Check that a non-existing file return ENOENT (2)
    eraised = False
    try:
        open('itdoesnotexist')
    except IOError as exc:
        eraised = True
        assert exc.errno == 2

    assert eraised

    f2 = d1.create_file()
    # time.sleep(0.5)

    f2.read_and_check()

    f2.move(os.path.join(mnt, random_name()))
    # time.sleep(0.5)

    f2.read_and_check()

    f2.move(os.path.join(mnt, d1.basename, random_name()))
    # time.sleep(0.5)

    f2.read_and_check()

    print 'sync'
    check_output(['blobfs', 'sync'], cwd=mnt)
    time.sleep(2)

    print 'restart'
    blobfs.restart(remove_data=True)

    f1.read_and_check()
    f2.read_and_check()

    # TODO(tsileo): enable this on console flag to "test" new test more easily
    # from IPython import embed; embed()

finally:
    blobfs.unmount()
    blobfs.remove_data()
    os.rmdir(mnt)
    blobstash.shutdown()
