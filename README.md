# Key Value store 

Assignment for the lecture Data Management and Data Structures at UniFr. 

## Status

Basic get and put functionality of the KV store are present, albeit only
non-persistent so far. This includes splitting of leaf nodes when they exceed
the maximum size, but does *not* include the splitting of internal
(respectively) the root node, due to said feature not being implemented yet.

In addition to implementing splitting of internal nodes, persistence by means
of a disk component will be added at a later time.

## Team

- Julius Oeftiger
- Michael Senn
- Tobias Famos

## Tests

To run all tests, run - from the root directory of the project:
```bash
go test ./...
```

## Building the project

To build the project, run:
```bash
go build
```

Then the executable can be ran:
```bash
./KVStore
```
