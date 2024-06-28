<div align="center">

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="./docs/logo-dark.svg">
  <img alt="Logo" src="./docs/logo-light.svg">
</picture>

[![License: AGPL 3.0](https://img.shields.io/github/license/loopholelabs/silo)](<[https://www.apache.org/licenses/LICENSE-2.0](https://www.gnu.org/licenses/agpl-3.0.en.html)>)
[![Discord](https://dcbadge.vercel.app/api/server/JYmFhtdPeu?style=flat)](https://loopholelabs.io/discord)
![Go Version](https://img.shields.io/badge/go%20version-%3E=1.21-61CFDD.svg)
[![Go Reference](https://pkg.go.dev/badge/github.com/loopholelabs/silo.svg)](https://pkg.go.dev/github.com/loopholelabs/silo)

</div>

## Overview

Silo is a storage primitive designed to support live migration.
One of the core functionalities within Silo is the ability to
migrate/sync storage to various `backends` while it is still in use (without affecting performance).

## Sources

All storage sources within Silo implement `storage.StorageProvider`. You can find some example sources at [pkg/storage/sources](pkg/storage/sources/README.md).

## Expose

If you wish to expose a Silo storage device to an external consumer, one way would be to use the NBD kernel driver. See [pkg/expose/sources](pkg/storage/expose/README.md).

## Block orders

When you wish to move storage from one place to another, you'll need to specify an order. This can be dynamically changing. For example, there is a volatility monitor which can be used to migrate storage from least volatile to most volatile. Also you may wish to prioritize certain blocks for example if the destination is trying to read them. [pkg/storage/blocks](pkg/storage/blocks/README.md).

## Migration

Migration of storage is handled by a `Migrator`. For more information on this see [pkg/storage/migrator](pkg/storage/migrator/README.md).

Example of a basic migration. Here we have block number on the Y axis, and time on the X axis.
We start out by performing random writes to regions of the storage. We start migration at 500ms and you can see that less volatile blocks are moved first (in blue). Once that is completed, dirty blocks are synced up (in red).

![](images/graph.png?raw=true)

This example adds a device reading from the destination. The block order is by least volatile, but with a priority for the blocks needed for reading. You can also see on the graph that the average read latency drops as more of the storage is locally available at dest.

![](images/example1.png?raw=true)

Same as above, but with concurrency set to 32. As long as the destination can do concurrent writes, everything will flow.

![](images/example2.png?raw=true)

## Contributing

Bug reports and pull requests are welcome on GitHub at [https://github.com/loopholelabs/silo][gitrepo]. For more
contribution information check
out [the contribution guide](https://github.com/loopholelabs/silo/blob/master/CONTRIBUTING.md).

## License

The Polyglot project is available as open source under the terms of
the [AGPL, 3.0 License](https://www.gnu.org/licenses/agpl-3.0.en.html).

## Code of Conduct

Everyone interacting in the Silo project’s codebases, issue trackers, chat rooms and mailing lists is expected to follow the [CNCF Code of Conduct](https://github.com/cncf/foundation/blob/master/code-of-conduct.md).

## Project Managed By:

[![https://loopholelabs.io][loopholelabs]](https://loopholelabs.io)

[gitrepo]: https://github.com/loopholelabs/silo
[loopholelabs]: https://cdn.loopholelabs.io/loopholelabs/LoopholeLabsLogo.svg
[loophomepage]: https://loopholelabs.io
