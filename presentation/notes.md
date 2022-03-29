# Presentation notes

This document aims to be a braindump of some things we might want to mention in
the presentation.

## Introduction

- The structure we chose to implement is a B+ tree
- Two types of nodes in the tree
  - Intermediary nodes:
    - Contain array of sorted separator keys
    - And (in between every two separators, like a fence) references to next
      node in the tree (as page ID, more later)
  - Leaf nodes:
    - Contain array of sorted uint64 keys (i.e. keys stored in the tree structure)
    - And array of corresponding [10]byte values

## Persistence

- Memory / disk layout
  - Working with 4KiB regions of continuous memory / disk
  - Each page adressed by an uint32 page identifier, so up to 2^32 * 4KiB ~= 18
    TiB of adressable memory
  - Page contains:
    - Page ID (4 byte)
    - Byte flag field (1 byte)
    - Node data
  - Nodes are sized to fill one page
    - Intermediary nodes: X separator / page pairs (FIXME @Julius)
    - Leaf nodes: X key / value pairs (FIXME @ Julius)
  - Encoding / decoding of nodes done using a custom, fixed-length encoding
    method (as if you'd take a C struct and outright write it into a `malloc`'d
    block)
    - I.e tightly coupled to structure of structs. Tradeoff simplicity vs flexibility

## 
