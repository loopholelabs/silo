# Blocks

During a Silo migration, blocks can be sent in various orderings. This is decided on by a BlockOrderer.
BlockOrderers can also be chained together.

| Orderer | Description |
| ------- | ----------- |
| AnyBlockOrder | This pulls blocks out blocks in no particular order.
| PriorityBlockOrder | This can prioritize certain blocks. If it doesn't know about a block, it'll defer to next in chain.
| VolatilityMonitor | This pulls blocks in order from least volatile to most volatile.

A typical Silo migration would be set up to use VolatilityMonitor to send blocks in a good order (Least volatile to most), but also have a PriorityBlockOrder infront to prioritise any blocks the destination is trying to read.

You can create your own BlockOrderer and use it in a migration, or combine it with others in a chain.

You can also tell a BlockOrderer that blocks are no longer needed. For example if the destination has performed some writes and no longer needs certain blocks. To do this you call Remove() which will propogate down the chain. The block will then not be returned.
