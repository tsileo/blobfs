# coding: utf-8
import os
import sys

import requests

from tests.node import File
from tests.node import Dir
from tests.node import random_name
from tests.process import BlobFS
from tests.process import BlobStash
# TODO(tsileo): test `blobfs` checkout, log
# TODO(tsileo): use pytest with custom error?
# TODO(tsileo): test that we can read the `.blobfs_socket` file


def _blobstash_query(path):
    r = requests.get('http://localhost:8050'+path, auth=('', os.environ.get('BLOBSTASH_API_KEY')))
    r.raise_for_status()
    return r.json()


def blobstash_fs(name, path):
    return _blobstash_query('/api/filetree/fs/fs/'+name+path)

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

    # Create a first file
    f1_1 = root_dir1.create_file(prefix='f1')
    f1_1.read_and_check()

    assert len(root_dir1.list()) == 1

    print "BLOBSTASH FS"

    print 'sync'
    print blobfs1.cmd('-comment', 'sync #1', 'push')

    # data = blobstash_fs(blobfs1.fs_name, '/')
    # assert len(data['children']) == 1

    print blobfs2.cmd('fetch')

    assert len(root_dir2.list()) == 1

    f2_1 = File(os.path.join(mnt2, f1_1.basename), f1_1.hexhash)
    # FIXME(tsileo): fix caching issue
    f2_1.read_and_check()

    # edit f1_1
    f1_1.edit()
    f1_1.read_and_check()
    print 'OOOO'
    print blobfs1.cmd('push')
    print blobfs2.cmd('fetch')

    assert f2_1.hexhash != f1_1.hexhash
    f2_1.hexhash = f1_1.hexhash
    f2_1.read_and_check()

    f2_2 = root_dir2.create_file(prefix='f2')
    f2_2.read_and_check()

    assert len(root_dir2.list()) == 2

    print blobfs2.cmd('-comment', 'sync #2', 'sync')

    # data = blobstash_fs(blobfs1.fs_name, '/')
    # assert len(data['children']) == 2

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

    print blobfs1.cmd('-comment', 'sync #3', 'sync')

    # data = blobstash_fs(blobfs1.fs_name, '/')
    # assert len(data['children']) == 3

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
    print blobfs2.cmd('-comment', 'sync #4', 'sync')
    print 'FETCH'

    # data = blobstash_fs(blobfs1.fs_name, '/')
    # assert len(data['children']) == 4


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

    print blobfs1.cmd('-comment', 'sync #41', 'sync')

    # data = blobstash_fs(blobfs1.fs_name, '/')
    # assert len(data['children']) == 5


    print blobfs2.cmd('fetch')

    # resync
    # print blobfs2.cmd('-comment', 'sync #5', 'sync')


    # print blobfs
    # XXX(tsileo): move this below / Ensure f5 has been synced
    f2_5 = File(os.path.join(mnt2, f1_5.basename), f1_5.hexhash)
    f2_5.read_and_check()
    # old_hash = f2_1.meta()['hash']
    # print 'OLD HASH', old_hash

    # Now both FS1 and FS2 are in sync, edit f1
    print 'EDITd21'
    f2_1.edit()
    # f2_1.print_debug()
    # print 'NEW META', f2_1.meta()['hash']
    # print
    # print

    f2_6 = root_dir2.create_file(prefix='f6')
    f2_6.read_and_check()
    f2_1.read_and_check()



    # FIXME(tsileo): use the filetree FS API to check the meta of the conflicted file

    # old_data = blobstash_fs(blobfs2.fs_name, '/'+f2_1.basename)
    print 'blobfs2 sync'
    print blobfs2.cmd('-comment', 'sync #6', 'sync')
    f2_1.read_and_check()

    # print 'HEREHEREHERE'
    # data = blobstash_fs(blobfs2.fs_name, '/'+f2_1.basename)
    # print data
    # assert data['ref'] != old_data['ref']
    # print blobstash_fs(blobfs2.fs_name, '/')

    print 'blobfs2 sync done'

    print "blobfs1"
    print blobfs1.cmd('debug')
    print "blobfs2"
    print blobfs2.cmd('debug')


    # # Now back to FS2, also edit f1 to trigger a conflicted
    # print 'BEFORE FETCH'
    # f1_1.edit()
    # print 'f1_1'
    # f1_1.print_debug()

    # f1_1.read_and_check()

    f1_1.edit()
    print 'f1_1'
    f1_1.print_debug()

    f1_1.read_and_check()


    assert f2_6.basename not in root_dir1.list()
    print blobfs1.cmd('fetch')

    assert f2_6.basename in root_dir1.list()

    f1_6 = File(os.path.join(mnt1, f2_6.basename), f2_6.hexhash)
    f1_6.read_and_check()


    print "blobfs1"
    print blobfs1.cmd('debug')
    print "blobfs2"
    print blobfs2.cmd('debug')


    print 'f1_1 after fetch'
    f1_1.print_debug()
    f1_1.read_and_check()


#     print root_dir1.list()
    f1_1_conflicted = File(os.path.join(mnt1, f1_1.basename + '.conflicted'), f2_1.hexhash)
    f1_1_conflicted.read_and_check()
    # f1_1_conflicted.print_debug()
    print 'conflicted OK'

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
