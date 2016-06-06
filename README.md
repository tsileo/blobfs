# BlobFS

A FUSE file system built on top of [BlobStash](https://github.com/tsileo/blobstash) with a taste of content versioning.

## Features

 - Content addressed (with BLAKE2b as hashing algorithm), files are split into blobs, and retrieved by hash, blobs are deduplicated (incremental backups by default).
 - You choose when to **commit** changes, with basic CVS features.
 - **checkout** old versions as immutable snapshot

## Usage

```
Usage of blobfs-mount:
  blobfs NAME MOUNTPOINT
  -host="": remote host, default to http://localhost:8050
  -immutable=false: make the filesystem immutable
  -loglevel="info": logging level (debug|info|warn|crit)
```

```console
$ mkdir ~/docs
$ blobfs-mount documents ~/docs
$ cd ~/docs
$ touch newdoc
$ blobfs -comment "added new doc" commit
$ blobfs log
$ ls
newdoc
$ touch newnewdoc
$ blobfs -comment "added new new doc" commit
$ blobfs log
$ ls
newdoc newnewdoc
$ blobfs checkout aef...
$ ls
newdoc
```

## TODOs

- [ ] Basic automatic conflict resolution
- [ ] Support BlobStash namespace
- [ ] Watch the root key for update
- [ ] A `put` subcommand for upload directory
- [ ] File locking?
