# `shardmap`

[![GoDoc](https://img.shields.io/badge/api-reference-blue.svg?style=flat-square)](https://godoc.org/github.com/tidwall/shardmap)

A simple and efficient thread-safe sharded hashmap for Go.
This is an alternative to the standard Go map and `sync.Map`, and is optimized
for when your map needs to perform lots of concurrent reads and writes.

Under the hood `shardmap` uses 
[robinhood hashmap](https://github.com/tidwall/rhh) and 
[xxhash](https://github.com/cespare/xxhash).

# Getting Started

## Installing

To start using `shardmap`, install Go and run `go get`:

```sh
$ go get -u github.com/tidwall/shardmap
```

This will retrieve the library.

## Usage

The `Map` type works similar to a standard Go map, and includes four methods:
`Set`, `Get`, `Delete`, `Len`.

```go
var m shardmap.Map
m.Set("Hello", "Dolly!")
val, _ := m.Get("Hello")
fmt.Printf("%v\n", val)
val, _ = m.Delete("Hello")
fmt.Printf("%v\n", val)
val, _ = m.Get("Hello")
fmt.Printf("%v\n", val)

// Output:
// Dolly!
// Dolly!
// <nil>
```

## Performance

Benchmarking conncurrent SET, GET, RANGE, and DELETE operations for 
    `sync.Map`, `map[string]interface{}`, `github.com/tidwall/shardmap`. 

```
go version go1.13 darwin/amd64

     number of cpus: 12
     number of keys: 1000000
            keysize: 10
        random seed: 1569421428153357000

-- sync.Map --
set: 1,000,000 ops over 12 threads in 955ms, 1,046,873/sec, 955 ns/op
get: 1,000,000 ops over 12 threads in 269ms, 3,718,882/sec, 268 ns/op
rng:       100 ops over 12 threads in 2434ms,       41/sec, 24342711 ns/op
del: 1,000,000 ops over 12 threads in 241ms, 4,156,554/sec, 240 ns/op

-- stdlib map --
set: 1,000,000 ops over 12 threads in 481ms, 2,078,213/sec, 481 ns/op
get: 1,000,000 ops over 12 threads in 45ms, 22,439,321/sec, 44 ns/op
rng:       100 ops over 12 threads in 260ms,       384/sec, 2598202 ns/op
del: 1,000,000 ops over 12 threads in 187ms, 5,339,459/sec, 187 ns/op

-- github.com/tidwall/shardmap --
set: 1,000,000 ops over 12 threads in 78ms, 12,828,089/sec, 77 ns/op
get: 1,000,000 ops over 12 threads in 22ms, 45,686,575/sec, 21 ns/op
rng:       100 ops over 12 threads in 231ms,       432/sec, 2310163 ns/op
del: 1,000,000 ops over 12 threads in 49ms, 20,259,435/sec, 49 ns/op
```

*run from my Macbook 2018*

## Contact

Josh Baker [@tidwall](http://twitter.com/tidwall)

## License

`shardmap` source code is available under the MIT [License](/LICENSE).