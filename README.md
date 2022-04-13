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

To build the project, run:
```bash
go build
```

Then the executable can be ran:
```bash
./KVStore
```

