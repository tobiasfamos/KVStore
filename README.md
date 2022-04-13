# Key Value store 

Implementation of a B+ tree done as part of the Data Management and Data
Structures lecture at UniFr. 

This is a project for educational purposes.

## Team

- Julius Oeftiger
- Michael Senn
- Tobias Famos

## Features

This implementation of a KV store supports the basic functionality required of
one:
- Create, read and delete KV store on disk
- Get and put key-value pairs

Deletions are not supported. Upserts are not supported.

## Tests & Benchmarks

To run all tests, run - from the root directory of the project:
```bash
go test ./...
```

There are also some benchmarks, which - in addition to providing performance
details - help in testing correctness. To run those, execute:

```bash
go test ./... -bench=.
```

## Rough performance characteristics

The following estimates are on a notebook with:
- An i7-7500U CPU
- An SSD as storage

The KV store performs at roughly:
- 300k inserts per second while not having to page out
- 600k reads per second while not having to page out
- 8k writes per second while having to page out

The performance while having to page out could certainly be improved, there's
going to be a lot of low-hanging fruits throughout.

## Building the project

The project ships with a toy interactive CLI, which allows to access a KV store
on disk, and get and set values.

To build it, run:
```bash
go build
```

Then the executable can be ran:
```bash
./KVStore
```

An example session is shown below:
```
› ./KVStore /tmp/test
Loading KV store from /tmp/test
KV Store @ /tmp/test> set 1 0x68656c6c6f
Successfully stored 1 = 68656c6c6f0000000000
KV Store @ /tmp/test> set 2 0x20776f726c64
Successfully stored 2 = 20776f726c6400000000
KV Store @ /tmp/test> set 1993 0x061A
Successfully stored 1993 = 061a0000000000000000
KV Store @ /tmp/test> get 1
1 = 68656c6c6f0000000000
KV Store @ /tmp/test> get 2
2 = 20776f726c6400000000
KV Store @ /tmp/test> get 1993
1993 = 061a0000000000000000
KV Store @ /tmp/test> exit
KV store successfully closed
```
