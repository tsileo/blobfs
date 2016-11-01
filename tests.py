import os
import random
import string
import hashlib

def random_name():
    return ''.join([random.choice(string.ascii_lowercase) for c in xrange(10)])

path = os.path.dirname(os.path.realpath(__file__))

mnt = os.path.join(path, 'cmd/blobfs-mount/ts5')

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


content = os.urandom(50 * 1024)

h = hashlib.sha1()
h.update(content)
hexhash = h.hexdigest()

with open(f1, 'wb+') as f:
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
