# BlobFS

A FUSE file system built on top of [BlobStash](https://github.com/tsileo/blobstash) with a taste of content versioning.

## Features

 - Content addressed (with BLAKE2b as hashing algorithm), files are split into blobs, and retrieved by hash, blobs are deduplicated (incremental backups by default).
 - You choose when to **commit** changes, with basic CVS features.
 - **checkout** old versions as immutable snapshot
 - Easily share entire directories or single files through BlobStash

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

- [ ] Better user support
- [ ] Better attr support
- [ ] A web UI (DropBox like, open source too) available on `my.blobfs.com` that connect to the user's BlobStash instance
- [ ] `.ignore` file support
- [ ] Basic automatic conflict resolution
- [ ] Watch the root key for update
- [ ] bash/zsh subcommand autocompletion doc
- [ ] A `put` subcommand for upload directory?
- [ ] File locking?
