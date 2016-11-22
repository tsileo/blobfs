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

    # Create a new file
    f1_3 = root_dir1.create_file(prefix='f3')
    f1_3.read_and_check()

    assert len(root_dir1.list()) == 3

    print blobfs1.cmd('sync')

    # Back to mount 2
    print 'MARK1', root_dir1.list()
    print 'MARK2', root_dir2.list()

    print blobfs2.cmd('fetch')

    f2_1.read_and_check()
    f2_2.read_and_check()

    print 'MARK21', root_dir2.list()

    f2_3 = File(os.path.join(mnt2, f1_3.basename), f1_3.hexhash)
    f2_3.read_and_check()

    assert len(root_dir2.list()) == 3
#     os.remove(f2_1.path)

    # Create another file
    f2_4 = root_dir2.create_file(prefix='f4')
    f2_4.read_and_check()

    print 'SYNC'
    print blobfs2.cmd('sync')
    print 'FETCH'

    # Create another file to trigger a sync conflict
    f1_5 = root_dir1.create_file(prefix='f5')
    f1_5.read_and_check()

    print blobfs1.cmd('fetch')

    f1_1.read_and_check()
    f1_2.read_and_check()
    f1_3.read_and_check()

    print 'LEN', root_dir1.list(), f2_4.basename

    # Ensure f4 has been pulled
    f1_4 = File(os.path.join(mnt1, f2_4.basename), f2_4.hexhash)
    f1_4.read_and_check()

    # resync
    print blobfs1.cmd('sync')
    print blobfs2.cmd('fetch')

    # Ensure f5 has been synced
    f2_5 = File(os.path.join(mnt2, f1_5.basename), f1_5.hexhash)
    f2_5.read_and_check()

    # # Now both FS1 and FS2 are in sync, edit f1
    # f1_1.edit()
    # print 'EDIT'
    # f1_1.print_debug()
    # print
    # print

    # f1_1.read_and_check()

    # print blobfs1.cmd('sync')

    # # Now back to FS2, also edit f1 to trigger a conflicted
    # print 'BEFORE FETCH'
    # f2_1.edit()
    # f2_1.print_debug()

    # f2_1.read_and_check()

    # print blobfs2.cmd('fetch')

    # print 'f2_1'
    # f2_1.print_debug()

    # f2_1_conflicted = File(os.path.join(mnt2, f2_1.basename + '.conflicted'), f1_1.hexhash)
    # f2_1_conflicted.print_debug()
    # f2_1_conflicted.read_and_check()

    # # FIXME(tsileo): add File.debug that dumps the meta (from HTTP socket API)

finally:
    blobfs1.unmount()
    blobfs1.cleanup()
    blobfs1.remove_data()
    os.rmdir(mnt1)

    blobfs2.unmount()
    blobfs2.cleanup()
    os.rmdir(mnt2)

    blobstash.shutdown()
