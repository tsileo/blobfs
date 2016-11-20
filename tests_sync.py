# coding: utf-8
import os
import time

from tests.node import File
from tests.node import Dir
from tests.node import random_name
from tests.process import BlobFS
from tests.process import BlobStash
# TODO(tsileo): test `blobfs` checkout, log
# TODO(tsileo): use pytest with custom error?
# TODO(tsileo): test that we can read the `.blobfs_socket` file


blobstash = BlobStash()
blobstash.cleanup()

blobstash.run()

blobfs1 = BlobFS()
blobfs1.mount(random_name())
mnt1 = blobfs1.mnt

blobfs2 = BlobFS(blobfs1.fs_name)
blobfs2.mount(random_name())
mnt2 = blobfs2.mnt


try:
    root_dir1 = Dir(mnt1)
    root_dir2 = Dir(mnt2)

    f1_1 = root_dir1.create_file()
    f1_1.read_and_check()

    print 'sync'
    print blobfs1.cmd('sync')
    print blobfs2.cmd('fetch')

    f2_1 = File(os.path.join(mnt2, f1_1.basename), f1_1.hexhash)
    f2_1.read_and_check()

finally:
    blobfs1.unmount()
    blobfs1.cleanup()
    blobfs1.remove_data()
    os.rmdir(mnt1)

    blobfs2.unmount()
    blobfs2.cleanup()
    os.rmdir(mnt2)

    blobstash.shutdown()
