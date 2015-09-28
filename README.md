# BlobFS

A FUSE file system built on top of [BlobStash](https://github.com/tsileo/blobstash).

## Usage

```
Usage of blobfs:
  blobfs NAME MOUNTPOINT
  -host="": remote host, default to http://localhost:8050
  -immutable=false: make the filesystem immutable
  -loglevel="info": logging level (debug|info|warn|crit)
```

## Debug

You can dump the meta hash by opening the special `.blobfs` file in dir (like `/mnt/my/dir/.blobfs`), or `*.blobfs` (like `/mnt/my/file.blobfs`).

```
$ cat mnt/.blobfs | awk '{print "http://localhost:8050/api/v1/blobstore/blob/"$1}' | xargs curl | jq .
$ cat mnt/file.blobfs | awk '{print "http://localhost:8050/api/v1/blobstore/blob/"$1}' | xargs curl | jq .
```
