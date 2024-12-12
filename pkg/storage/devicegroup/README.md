# Device Group

The `DeviceGroup` combines some number of Silo devices into a single unit, which can then be migrated to another Silo instance.
All internat concerns such as volatilityMonitor, waitingCache, as well as the new S3 assist, are now hidden from the consumer.

## Creation

There are two methods to create a `DeviceGroup`.

### NewFromSchema

This takes in an array of Silo device configs, and creates the devices. If `expose==true` then a corresponding NBD device will be created and attached.

### NewFromProtocol

This takes in a `protocol` and creates the devices as they are received from a sender.

## Usage (Sending devices)

Devices in a `DeviceGroup` are sent together, which allows Silo to optimize all aspects of the transfer.

    // Create a device group from schema
	dg, err := devicegroup.NewFromSchema(devices, log, siloMetrics)

    // Start a migration
	err := dg.StartMigrationTo(protocol)

    // Migrate the data with max total concurrency 100
	err = dg.MigrateAll(100, pHandler)

    // Migrate any dirty blocks
    // hooks gives some control over the dirty loop
	err = dg.MigrateDirty(hooks)

    // Close everything
    dg.CloseAll()

There is also support for sending custom data. This would typically be done either in `pHandler` (The progress handler), or in one of the `MigrateDirty` hooks.

    pHandler := func(ps []*migrator.MigrationProgress) {
        // Do some test here to see if enough data migrated

        // If so, send a custom Authority Transfer event.
        dg.SendCustomData(authorityTransferPacket)
    }

## Usage (Receiving devices)

    // Create a DeviceGroup from protocol
    // tweak allows us to modify the schema
	dg, err = NewFromProtocol(ctx, protocol, tweak, nil, nil)

    // Handle any custom data
	go dg.HandleCustomData(func(data []byte) {
        // We got sent some custom data!
    })

    // Wait for migration completion
	dg.WaitForCompletion()

Once a `DeviceGroup` is has been created and migration is completed, you can then send the devices somewhere else with `StartMigration(protocol)`.