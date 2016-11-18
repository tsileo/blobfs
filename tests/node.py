# coding: utf-8
import os
import hashlib
import shutil
import string
import random

FILE_SIZE = 3 * 2 ** 20  # 3MB file by default


def random_name(l=10):
    """Returns a random string."""
    # TODO(tsileo): update the choice to all the accepted chars for a filename
    return ''.join([random.choice(string.ascii_lowercase) for c in xrange(l)])


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
