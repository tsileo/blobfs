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

### blobfsdebug

```
$ ./debug/blobfsdebug mnt/hello.txt 
{
  "name": "hello.txt",
  "type": "file",
  "size": 1189,
  "mode": 438,
  "mtime": "2015-09-22T10:53:22+02:00",
  "refs": [
    [
      1189,
      "8ab2ebc58055ca8439070c9d7b5527dcf5931ee46d0f91fec9cf99ab7cff9758"
    ]
  ],
  "version": "1"
}
```
