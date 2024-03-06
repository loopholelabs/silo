# Volatility monitor

You can use a volatility monitor to monitor writes, and to return blocks in order from least volatile to most.

## Usage

`VolatilityMonitor` implements both `storage.StorageProvider` and also `storage.BlockOrder`. So you can put it in a chain of storage providers, and then use it as an orderer for a migrator.