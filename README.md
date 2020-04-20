# BlobFS

A new version of this project is available here: https://git.sr.ht/~tsileo/blobfs

A FUSE file system built on top of [BlobStash](https://github.com/tsileo/blobstash) with built-in sync and deduplication.

## Features

 - Content addressed (with BLAKE2b as hashing algorithm), files are split into blobs, and retrieved by hash, blobs are deduplicated (incremental backups by default).
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
```

## TODOs

- [ ] undo cmd like the hammer filesystem
- [ ] Better user support
- [ ] Better attr support
- [ ] A web UI (DropBox like, open source too) available on `my.blobfs.com` that connect to the user's BlobStash instance
- [ ] `.ignore` file support
- [ ] Basic automatic conflict resolution
- [ ] Watch the root key for update
- [ ] bash/zsh subcommand autocompletion doc
- [ ] A `put` subcommand for upload directory?
- [ ] File locking?
