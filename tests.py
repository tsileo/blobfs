import os
import random
import string
import time
import hashlib
from subprocess import Popen, PIPE


def random_name():
    return ''.join([random.choice(string.ascii_lowercase) for c in xrange(10)])

path = os.path.dirname(os.path.realpath(__file__))

fs_name = random_name()

try:
    os.remove('/tmp/blobfs_{}.sock'.format(fs_name))
except:
    pass

os.makedirs(fs_name)

mnt = os.path.join(path, fs_name)


def run_blob_cmd(cmd):
    process = Popen(['blobfs', cmd], stdout=PIPE, cwd=mnt)
    output = process.communicate()[0]
    retcode = process.poll()
    if retcode:
        raise Exception('failed')
    return output


def build_blobfs_mount():
    p = Popen(['go', 'build', './cmd/blobfs-mount/blobfs-mount.go'], env=os.environ)
    if p.wait():
        raise Exception('failed')


# TODO(tsileo): build blobfs-mount
def run_blobfs_mount():
    p = Popen(['./blobfs-mount', '-loglevel', 'debug', fs_name, fs_name])
    time.sleep(1)
    if p.poll():
        raise Exception('failed to mount')
    return p

build_blobfs_mount()

p = run_blobfs_mount()
try:

    contents = os.listdir(mnt)

    print 'contents='
    print contents
    print

    rdir = random_name()

    d1 = os.path.join(mnt, rdir)

    os.mkdir(d1)
    assert os.path.isdir(d1)

    f1 = os.path.join(d1, random_name())
    print 'f1 =', f1

    f2 = os.path.join(mnt, random_name())
    print 'f2 =', f2

    f3 = os.path.join(d1, random_name())
    print 'f3 =', f3


    content = os.urandom(50 * 1024)

    h = hashlib.sha1()
    h.update(content)
    hexhash = h.hexdigest()

    with open(f1, 'wb+') as f:
        f.write(content)

    with open(f3, 'wb+') as f:
        f.write(content)

    with open(f1, 'rb') as f:
        content2 = ''
        h2 = hashlib.sha1()
        while 1:
            buf = f.read()
            h2.update(buf)
            content2 += buf
            if not buf:
                break
        hexhash2 = h2.hexdigest()
        assert hexhash == hexhash2
        print 'Write test succeed'

    os.rename(f1, f2)

    with open(f2, 'rb') as f:
        content3 = ''
        h3 = hashlib.sha1()
        while 1:
            buf = f.read()
            h3.update(buf)
            content3 += buf
            if not buf:
                break
        hexhash3 = h3.hexdigest()
        assert hexhash == hexhash3
        print 'Move test succeed'

    os.remove(f2)
    assert not os.path.isfile(f2)
    print 'Delete test succeed'

    # Unmount the FS, remount it and check the files are still there
    p.terminate()
    p.wait()
    os.remove('/tmp/blobfs_{}.sock'.format(fs_name))
    p = run_blobfs_mount()

    print 'Running blobfs log'
    print run_blob_cmd('log')
    print 'ok'

    # TODO(tsileo): remove cache and re-load?
    with open(f3, 'rb') as f:
        content3 = ''
        h3 = hashlib.sha1()
        while 1:
            buf = f.read()
            h3.update(buf)
            content3 += buf
            if not buf:
                break
        hexhash3 = h3.hexdigest()
        assert hexhash == hexhash3
        print 'Unmount/mount test succeed'

finally:
    p.terminate()
    p.wait()
