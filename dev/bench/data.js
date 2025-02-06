window.BENCHMARK_DATA = {
  "lastUpdate": 1738866896985,
  "repoUrl": "https://github.com/loopholelabs/silo",
  "entries": {
    "Silo Go Benchmark": [
      {
        "commit": {
          "author": {
            "email": "jamesmoore@loopholelabs.io",
            "name": "Jimmy Moore",
            "username": "jimmyaxod"
          },
          "committer": {
            "email": "jamesmoore@loopholelabs.io",
            "name": "Jimmy Moore",
            "username": "jimmyaxod"
          },
          "distinct": true,
          "id": "4464d5c32b105361c80890d5f26219bf630be550",
          "message": "Updated bench run\n\nSigned-off-by: Jimmy Moore <jamesmoore@loopholelabs.io>",
          "timestamp": "2025-02-06T18:33:25Z",
          "tree_id": "d790c0ed864925c1ad525e07c60b6e51cab64fae",
          "url": "https://github.com/loopholelabs/silo/commit/4464d5c32b105361c80890d5f26219bf630be550"
        },
        "date": 1738866896487,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkMigrationPipe/32-concurrency",
            "value": 43971635,
            "unit": "ns/op\t 238.47 MB/s",
            "extra": "92 times\n4 procs"
          },
          {
            "name": "BenchmarkMigrationPipe/32-concurrency - ns/op",
            "value": 43971635,
            "unit": "ns/op",
            "extra": "92 times\n4 procs"
          },
          {
            "name": "BenchmarkMigrationPipe/32-concurrency - MB/s",
            "value": 238.47,
            "unit": "MB/s",
            "extra": "92 times\n4 procs"
          },
          {
            "name": "BenchmarkMigrationPipe/128-concurrency",
            "value": 43698477,
            "unit": "ns/op\t 239.96 MB/s",
            "extra": "100 times\n4 procs"
          },
          {
            "name": "BenchmarkMigrationPipe/128-concurrency - ns/op",
            "value": 43698477,
            "unit": "ns/op",
            "extra": "100 times\n4 procs"
          },
          {
            "name": "BenchmarkMigrationPipe/128-concurrency - MB/s",
            "value": 239.96,
            "unit": "MB/s",
            "extra": "100 times\n4 procs"
          },
          {
            "name": "BenchmarkMigrationPipe/max-concurrency",
            "value": 43238775,
            "unit": "ns/op\t 242.51 MB/s",
            "extra": "82 times\n4 procs"
          },
          {
            "name": "BenchmarkMigrationPipe/max-concurrency - ns/op",
            "value": 43238775,
            "unit": "ns/op",
            "extra": "82 times\n4 procs"
          },
          {
            "name": "BenchmarkMigrationPipe/max-concurrency - MB/s",
            "value": 242.51,
            "unit": "MB/s",
            "extra": "82 times\n4 procs"
          },
          {
            "name": "BenchmarkMigrationPipe/1-pipe",
            "value": 43147531,
            "unit": "ns/op\t 243.02 MB/s",
            "extra": "81 times\n4 procs"
          },
          {
            "name": "BenchmarkMigrationPipe/1-pipe - ns/op",
            "value": 43147531,
            "unit": "ns/op",
            "extra": "81 times\n4 procs"
          },
          {
            "name": "BenchmarkMigrationPipe/1-pipe - MB/s",
            "value": 243.02,
            "unit": "MB/s",
            "extra": "81 times\n4 procs"
          },
          {
            "name": "BenchmarkMigrationPipe/4-pipes",
            "value": 44321497,
            "unit": "ns/op\t 236.58 MB/s",
            "extra": "100 times\n4 procs"
          },
          {
            "name": "BenchmarkMigrationPipe/4-pipes - ns/op",
            "value": 44321497,
            "unit": "ns/op",
            "extra": "100 times\n4 procs"
          },
          {
            "name": "BenchmarkMigrationPipe/4-pipes - MB/s",
            "value": 236.58,
            "unit": "MB/s",
            "extra": "100 times\n4 procs"
          },
          {
            "name": "BenchmarkMigrationPipe/32-pipes",
            "value": 43895451,
            "unit": "ns/op\t 238.88 MB/s",
            "extra": "88 times\n4 procs"
          },
          {
            "name": "BenchmarkMigrationPipe/32-pipes - ns/op",
            "value": 43895451,
            "unit": "ns/op",
            "extra": "88 times\n4 procs"
          },
          {
            "name": "BenchmarkMigrationPipe/32-pipes - MB/s",
            "value": 238.88,
            "unit": "MB/s",
            "extra": "88 times\n4 procs"
          },
          {
            "name": "BenchmarkMigrationPipe/4k-blocks",
            "value": 610034907,
            "unit": "ns/op\t  17.19 MB/s",
            "extra": "3 times\n4 procs"
          },
          {
            "name": "BenchmarkMigrationPipe/4k-blocks - ns/op",
            "value": 610034907,
            "unit": "ns/op",
            "extra": "3 times\n4 procs"
          },
          {
            "name": "BenchmarkMigrationPipe/4k-blocks - MB/s",
            "value": 17.19,
            "unit": "MB/s",
            "extra": "3 times\n4 procs"
          },
          {
            "name": "BenchmarkMigrationPipe/64k-blocks",
            "value": 44019691,
            "unit": "ns/op\t 238.21 MB/s",
            "extra": "87 times\n4 procs"
          },
          {
            "name": "BenchmarkMigrationPipe/64k-blocks - ns/op",
            "value": 44019691,
            "unit": "ns/op",
            "extra": "87 times\n4 procs"
          },
          {
            "name": "BenchmarkMigrationPipe/64k-blocks - MB/s",
            "value": 238.21,
            "unit": "MB/s",
            "extra": "87 times\n4 procs"
          },
          {
            "name": "BenchmarkMigrationPipe/256k-blocks",
            "value": 11555011,
            "unit": "ns/op\t 907.46 MB/s",
            "extra": "109 times\n4 procs"
          },
          {
            "name": "BenchmarkMigrationPipe/256k-blocks - ns/op",
            "value": 11555011,
            "unit": "ns/op",
            "extra": "109 times\n4 procs"
          },
          {
            "name": "BenchmarkMigrationPipe/256k-blocks - MB/s",
            "value": 907.46,
            "unit": "MB/s",
            "extra": "109 times\n4 procs"
          },
          {
            "name": "BenchmarkMigrationPipe/no-sharding",
            "value": 11196206,
            "unit": "ns/op\t 936.55 MB/s",
            "extra": "99 times\n4 procs"
          },
          {
            "name": "BenchmarkMigrationPipe/no-sharding - ns/op",
            "value": 11196206,
            "unit": "ns/op",
            "extra": "99 times\n4 procs"
          },
          {
            "name": "BenchmarkMigrationPipe/no-sharding - MB/s",
            "value": 936.55,
            "unit": "MB/s",
            "extra": "99 times\n4 procs"
          },
          {
            "name": "BenchmarkMigrationPipe/4k-shards",
            "value": 11442693,
            "unit": "ns/op\t 916.37 MB/s",
            "extra": "112 times\n4 procs"
          },
          {
            "name": "BenchmarkMigrationPipe/4k-shards - ns/op",
            "value": 11442693,
            "unit": "ns/op",
            "extra": "112 times\n4 procs"
          },
          {
            "name": "BenchmarkMigrationPipe/4k-shards - MB/s",
            "value": 916.37,
            "unit": "MB/s",
            "extra": "112 times\n4 procs"
          },
          {
            "name": "BenchmarkMigrationPipe/64k-shards",
            "value": 11410584,
            "unit": "ns/op\t 918.95 MB/s",
            "extra": "110 times\n4 procs"
          },
          {
            "name": "BenchmarkMigrationPipe/64k-shards - ns/op",
            "value": 11410584,
            "unit": "ns/op",
            "extra": "110 times\n4 procs"
          },
          {
            "name": "BenchmarkMigrationPipe/64k-shards - MB/s",
            "value": 918.95,
            "unit": "MB/s",
            "extra": "110 times\n4 procs"
          },
          {
            "name": "BenchmarkMigrationPipe/256k-shards",
            "value": 11126261,
            "unit": "ns/op\t 942.43 MB/s",
            "extra": "99 times\n4 procs"
          },
          {
            "name": "BenchmarkMigrationPipe/256k-shards - ns/op",
            "value": 11126261,
            "unit": "ns/op",
            "extra": "99 times\n4 procs"
          },
          {
            "name": "BenchmarkMigrationPipe/256k-shards - MB/s",
            "value": 942.43,
            "unit": "MB/s",
            "extra": "99 times\n4 procs"
          },
          {
            "name": "BenchmarkMigrationPipe/no-compress",
            "value": 11161648,
            "unit": "ns/op\t 939.45 MB/s",
            "extra": "100 times\n4 procs"
          },
          {
            "name": "BenchmarkMigrationPipe/no-compress - ns/op",
            "value": 11161648,
            "unit": "ns/op",
            "extra": "100 times\n4 procs"
          },
          {
            "name": "BenchmarkMigrationPipe/no-compress - MB/s",
            "value": 939.45,
            "unit": "MB/s",
            "extra": "100 times\n4 procs"
          },
          {
            "name": "BenchmarkMigrationPipe/compress",
            "value": 39990218,
            "unit": "ns/op\t 262.21 MB/s",
            "extra": "31 times\n4 procs"
          },
          {
            "name": "BenchmarkMigrationPipe/compress - ns/op",
            "value": 39990218,
            "unit": "ns/op",
            "extra": "31 times\n4 procs"
          },
          {
            "name": "BenchmarkMigrationPipe/compress - MB/s",
            "value": 262.21,
            "unit": "MB/s",
            "extra": "31 times\n4 procs"
          }
        ]
      }
    ]
  }
}