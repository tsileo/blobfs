# coding: utf-8
"""Test suite for BlobFS.

A custom test suite allows the tests for be written at the same as new features.
A proper POSIX test suite may be added later once more features/calls are implemented.
"""
import logging
import os
import sys

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

blobfs = BlobFS()

blobfs.mount(debug=DEBUG)
mnt = blobfs.mnt

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
    print blobfs.cmd('sync')

    print 'restart'
    blobfs.restart(remove_data=True)

    f1.read_and_check()
    f2.read_and_check()

    # TODO(tsileo): enable this on console flag to "test" new test more easily
    # from IPython import embed; embed()

except Exception, exc:
    logging.exception('failed')
    print exc
finally:
    blobfs.unmount()
    blobfs.cleanup()
    blobfs.remove_data()
    os.rmdir(mnt)
    blobstash.shutdown()
