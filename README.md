# blox [![Build Status](https://travis-ci.org/hexablock/blox.svg?branch=master)](https://travis-ci.org/hexablock/blox)
A Content-Addressable storage system

### Command line hash verification

Example:

```
    # Verify data block
    echo -n -e '\x01' | cat - assembler.go | shasum -a 256 -b
```
