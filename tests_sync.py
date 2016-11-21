# coding: utf-8
import os
import sys

from tests.node import File
from tests.node import Dir
from tests.node import random_name
from tests.process import BlobFS
from tests.process import BlobStash
# TODO(tsileo): test `blobfs` checkout, log
# TODO(tsileo): use pytest with custom error?
# TODO(tsileo): test that we can read the `.blobfs_socket` file

DEBUG = False
if len(sys.argv) > 1:
    DEBUG = True

blobstash = BlobStash()
blobstash.cleanup()

blobstash.run()

blobfs1 = BlobFS()
blobfs1.mount(random_name(), debug=DEBUG)
mnt1 = blobfs1.mnt

blobfs2 = BlobFS(blobfs1.fs_name)
blobfs2.mount(random_name(), debug=DEBUG)
mnt2 = blobfs2.mnt


try:
    root_dir1 = Dir(mnt1)
    root_dir2 = Dir(mnt2)

    f1_1 = root_dir1.create_file(prefix='f1')
    f1_1.read_and_check()

    assert len(root_dir1.list()) == 1

    print 'sync'
    print blobfs1.cmd('sync')
    print blobfs2.cmd('fetch')

    assert len(root_dir2.list()) == 1

    f2_1 = File(os.path.join(mnt2, f1_1.basename), f1_1.hexhash)
    f2_1.read_and_check()

    f2_2 = root_dir2.create_file(prefix='f2')
    f2_2.read_and_check()

    assert len(root_dir2.list()) == 2

    print blobfs2.cmd('sync')
    print 'FETCH'
    # Back to mount 1
    print blobfs1.cmd('fetch')

    assert len(root_dir1.list()) == 2

    f1_1.read_and_check()

    f1_2 = File(os.path.join(mnt1, f2_2.basename), f2_2.hexhash)
    f1_2.read_and_check()

    f1_3 = root_dir1.create_file(prefix='f3')
    f1_3.read_and_check()

    assert len(root_dir1.list()) == 3

    print blobfs1.cmd('sync')

    # Back to mount 2

    f2_4 = root_dir2.create_file(prefix='f4')
    f2_4.read_and_check()

    print 'MARK1', root_dir1.list()
    print 'MARK2', root_dir2.list()

    print blobfs2.cmd('fetch')

    f2_1.read_and_check()
    f2_2.read_and_check()

    print 'MARK21', root_dir2.list()

    f2_3 = File(os.path.join(mnt2, f1_3.basename), f1_3.hexhash)
    f2_3.read_and_check()

    assert len(root_dir2.list()) == 4
#     os.remove(f2_1.path)

    print 'SYNC'
    print blobfs2.cmd('sync')
    print 'FETCH'
    print blobfs1.cmd('fetch')

    f1_1.read_and_check()
    f1_2.read_and_check()
    f1_3.read_and_check()
    f1_1.read_and_check()

    print 'LEN', root_dir1.list(), f2_4.basename

    f1_4 = File(os.path.join(mnt1, f2_4.basename), f2_4.hexhash)
    f1_4.read_and_check()

#     # FIXME(tsileo): ensure f1_1

finally:
    blobfs1.unmount()
    blobfs1.cleanup()
    blobfs1.remove_data()
    os.rmdir(mnt1)

    blobfs2.unmount()
    blobfs2.cleanup()
    os.rmdir(mnt2)

    blobstash.shutdown()
