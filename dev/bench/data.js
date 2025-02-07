window.BENCHMARK_DATA = {
  "lastUpdate": 1738933894344,
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
      },
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
          "id": "349705d445e0003cf2e82f53935b5b6272e3a3e2",
          "message": "Lint fix\n\nSigned-off-by: Jimmy Moore <jamesmoore@loopholelabs.io>",
          "timestamp": "2025-02-06T19:31:28Z",
          "tree_id": "5b2dc70b8df3c3ea76846a39d69a84168455e79d",
          "url": "https://github.com/loopholelabs/silo/commit/349705d445e0003cf2e82f53935b5b6272e3a3e2"
        },
        "date": 1738870335347,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkMigrationPipe/32-concurrency",
            "value": 3867205,
            "unit": "ns/op",
            "extra": "286 times\n4 procs"
          },
          {
            "name": "BenchmarkMigrationPipe/128-concurrency",
            "value": 4556622,
            "unit": "ns/op",
            "extra": "264 times\n4 procs"
          },
          {
            "name": "BenchmarkMigrationPipe/max-concurrency",
            "value": 4753827,
            "unit": "ns/op",
            "extra": "249 times\n4 procs"
          },
          {
            "name": "BenchmarkMigrationPipe/1-pipe",
            "value": 4667554,
            "unit": "ns/op",
            "extra": "250 times\n4 procs"
          },
          {
            "name": "BenchmarkMigrationPipe/4-pipes",
            "value": 2904951,
            "unit": "ns/op",
            "extra": "388 times\n4 procs"
          },
          {
            "name": "BenchmarkMigrationPipe/32-pipes",
            "value": 2952607,
            "unit": "ns/op",
            "extra": "352 times\n4 procs"
          },
          {
            "name": "BenchmarkMigrationPipe/4k-blocks",
            "value": 28749062,
            "unit": "ns/op",
            "extra": "64 times\n4 procs"
          },
          {
            "name": "BenchmarkMigrationPipe/64k-blocks",
            "value": 2889057,
            "unit": "ns/op",
            "extra": "410 times\n4 procs"
          },
          {
            "name": "BenchmarkMigrationPipe/256k-blocks",
            "value": 2621996,
            "unit": "ns/op",
            "extra": "447 times\n4 procs"
          },
          {
            "name": "BenchmarkMigrationPipe/no-sharding",
            "value": 2716517,
            "unit": "ns/op",
            "extra": "448 times\n4 procs"
          },
          {
            "name": "BenchmarkMigrationPipe/4k-shards",
            "value": 3014672,
            "unit": "ns/op",
            "extra": "379 times\n4 procs"
          },
          {
            "name": "BenchmarkMigrationPipe/64k-shards",
            "value": 2640764,
            "unit": "ns/op",
            "extra": "434 times\n4 procs"
          },
          {
            "name": "BenchmarkMigrationPipe/256k-shards",
            "value": 2589898,
            "unit": "ns/op",
            "extra": "438 times\n4 procs"
          },
          {
            "name": "BenchmarkMigrationPipe/no-compress",
            "value": 2639414,
            "unit": "ns/op",
            "extra": "451 times\n4 procs"
          },
          {
            "name": "BenchmarkMigrationPipe/compress",
            "value": 37306166,
            "unit": "ns/op",
            "extra": "30 times\n4 procs"
          }
        ]
      },
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
          "id": "7ae9b7ec5182ecc6d5f5eb00347e62d9252c0a7d",
          "message": "Lint fix\n\nSigned-off-by: Jimmy Moore <jamesmoore@loopholelabs.io>",
          "timestamp": "2025-02-07T13:10:40Z",
          "tree_id": "7b1bb8c8f65bf3e336f50c68c2c30fb6a8088930",
          "url": "https://github.com/loopholelabs/silo/commit/7ae9b7ec5182ecc6d5f5eb00347e62d9252c0a7d"
        },
        "date": 1738933893260,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkMigrationPipe/32-concurrency",
            "value": 3845367,
            "unit": "ns/op",
            "extra": "298 times\n4 procs"
          },
          {
            "name": "BenchmarkMigrationPipe/128-concurrency",
            "value": 4874492,
            "unit": "ns/op",
            "extra": "255 times\n4 procs"
          },
          {
            "name": "BenchmarkMigrationPipe/max-concurrency",
            "value": 4970337,
            "unit": "ns/op",
            "extra": "234 times\n4 procs"
          },
          {
            "name": "BenchmarkMigrationPipe/1-pipe",
            "value": 5050136,
            "unit": "ns/op",
            "extra": "237 times\n4 procs"
          },
          {
            "name": "BenchmarkMigrationPipe/4-pipes",
            "value": 3263031,
            "unit": "ns/op",
            "extra": "362 times\n4 procs"
          },
          {
            "name": "BenchmarkMigrationPipe/32-pipes",
            "value": 3223768,
            "unit": "ns/op",
            "extra": "328 times\n4 procs"
          },
          {
            "name": "BenchmarkMigrationPipe/4k-blocks",
            "value": 31234930,
            "unit": "ns/op",
            "extra": "43 times\n4 procs"
          },
          {
            "name": "BenchmarkMigrationPipe/64k-blocks",
            "value": 3068951,
            "unit": "ns/op",
            "extra": "390 times\n4 procs"
          },
          {
            "name": "BenchmarkMigrationPipe/256k-blocks",
            "value": 2651820,
            "unit": "ns/op",
            "extra": "428 times\n4 procs"
          },
          {
            "name": "BenchmarkMigrationPipe/no-sharding",
            "value": 2729365,
            "unit": "ns/op",
            "extra": "423 times\n4 procs"
          },
          {
            "name": "BenchmarkMigrationPipe/4k-shards",
            "value": 3116151,
            "unit": "ns/op",
            "extra": "366 times\n4 procs"
          },
          {
            "name": "BenchmarkMigrationPipe/64k-shards",
            "value": 2760516,
            "unit": "ns/op",
            "extra": "418 times\n4 procs"
          },
          {
            "name": "BenchmarkMigrationPipe/256k-shards",
            "value": 2681140,
            "unit": "ns/op",
            "extra": "429 times\n4 procs"
          },
          {
            "name": "BenchmarkMigrationPipe/no-compress",
            "value": 2687202,
            "unit": "ns/op",
            "extra": "426 times\n4 procs"
          },
          {
            "name": "BenchmarkMigrationPipe/compress",
            "value": 37193764,
            "unit": "ns/op",
            "extra": "30 times\n4 procs"
          }
        ]
      }
    ]
  }
}