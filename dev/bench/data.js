window.BENCHMARK_DATA = {
  "lastUpdate": 1738867968361,
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
          "id": "b694ae61d1540f172993bcc697f1be2d9506f54f",
          "message": "Updated benchmark\n\nSigned-off-by: Jimmy Moore <jamesmoore@loopholelabs.io>",
          "timestamp": "2025-02-06T18:51:20Z",
          "tree_id": "27dadd8c3f9b1235f3df135ed99c69570a985c4d",
          "url": "https://github.com/loopholelabs/silo/commit/b694ae61d1540f172993bcc697f1be2d9506f54f"
        },
        "date": 1738867967750,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkMigrationPipe/32-concurrency",
            "value": 3565862,
            "unit": "ns/op",
            "extra": "330 times\n4 procs"
          },
          {
            "name": "BenchmarkMigrationPipe/128-concurrency",
            "value": 4441672,
            "unit": "ns/op",
            "extra": "279 times\n4 procs"
          },
          {
            "name": "BenchmarkMigrationPipe/max-concurrency",
            "value": 4571698,
            "unit": "ns/op",
            "extra": "259 times\n4 procs"
          },
          {
            "name": "BenchmarkMigrationPipe/1-pipe",
            "value": 4552243,
            "unit": "ns/op",
            "extra": "259 times\n4 procs"
          },
          {
            "name": "BenchmarkMigrationPipe/4-pipes",
            "value": 3004069,
            "unit": "ns/op",
            "extra": "375 times\n4 procs"
          },
          {
            "name": "BenchmarkMigrationPipe/32-pipes",
            "value": 2704714,
            "unit": "ns/op",
            "extra": "418 times\n4 procs"
          },
          {
            "name": "BenchmarkMigrationPipe/4k-blocks",
            "value": 28566976,
            "unit": "ns/op",
            "extra": "74 times\n4 procs"
          },
          {
            "name": "BenchmarkMigrationPipe/64k-blocks",
            "value": 2727907,
            "unit": "ns/op",
            "extra": "418 times\n4 procs"
          },
          {
            "name": "BenchmarkMigrationPipe/256k-blocks",
            "value": 2580069,
            "unit": "ns/op",
            "extra": "459 times\n4 procs"
          },
          {
            "name": "BenchmarkMigrationPipe/no-sharding",
            "value": 2554266,
            "unit": "ns/op",
            "extra": "456 times\n4 procs"
          },
          {
            "name": "BenchmarkMigrationPipe/4k-shards",
            "value": 2901409,
            "unit": "ns/op",
            "extra": "400 times\n4 procs"
          },
          {
            "name": "BenchmarkMigrationPipe/64k-shards",
            "value": 2552722,
            "unit": "ns/op",
            "extra": "454 times\n4 procs"
          },
          {
            "name": "BenchmarkMigrationPipe/256k-shards",
            "value": 2499448,
            "unit": "ns/op",
            "extra": "466 times\n4 procs"
          },
          {
            "name": "BenchmarkMigrationPipe/no-compress",
            "value": 2503435,
            "unit": "ns/op",
            "extra": "454 times\n4 procs"
          },
          {
            "name": "BenchmarkMigrationPipe/compress",
            "value": 37269859,
            "unit": "ns/op",
            "extra": "31 times\n4 procs"
          }
        ]
      }
    ]
  }
}