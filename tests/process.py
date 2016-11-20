# coding: utf-8
from subprocess import Popen
from subprocess import check_output
import tempfile
import time
import shutil
import os
from node import random_name

PATH = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))


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
    def __init__(self, fs_name=None, rebuild=True):
        self.process = None
        if not fs_name:
            fs_name = random_name()
        self.fs_name = fs_name

        if rebuild:
            self._build()

    def _build(self):
        """Build the `blobsfs-mount` binary in the CWD"""
        p = Popen(['go', 'build', './cmd/blobfs-mount/blobfs-mount.go'], env=os.environ)
        if p.wait():
            raise Exception('failed')

        p = Popen(['go', 'install', './cmd/blobfs'], env=os.environ)
        if p.wait():
            raise Exception('failed')

    def mount(self, mountpoint=None, debug=False):
        """Execute `blobsfs-mount {fs_name} {fs_name}` and return the running process."""
        if not mountpoint:
            mountpoint = self.fs_name
        self.mountpoint = mountpoint
        self.mnt = os.path.join(PATH, mountpoint)
        if not os.path.exists(self.mnt):
            os.makedirs(self.mnt)
        self.var_dir = os.path.join(tempfile.gettempdir(), 'blobfs_test_{}_{}'.format(self.fs_name, self.mountpoint))
        if not os.path.exists(self.var_dir):
            os.makedirs(self.var_dir)
        env = dict(os.environ)
        env['BLOBFS_VAR_DIR'] = self.var_dir
        process_args = ['./blobfs-mount']
        if debug:
            process_args.extend(['-loglevel', 'debug'])
        process_args.extend([self.fs_name, mountpoint])
        self.process = Popen(process_args, env=env)
        time.sleep(1)
        if self.process.poll():
            raise Exception('failed to mount')
        with open(os.path.join(self.mnt, '.blobfs_socket')) as f:
            self.socket_path = f.read()

    def cmd(self, *args):
        cmd = ['blobfs']
        cmd.extend(args)
        print cmd
        return check_output(cmd, cwd=self.mnt)

    def cleanup(self):
        """Cleanup func."""
        try:
            print self.socket_path
            os.remove(self.socket_path)
            shutil.rmtree()
        except:
            pass

    def unmount(self):
        # Unmount the FS if it's currently mounted
        if self.process:
            self.process.terminate()
            self.process.wait()

    def remove_data(self):
        shutil.rmtree(self.var_dir)

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
