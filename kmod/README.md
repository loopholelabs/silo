<div align="center">
<picture>
  <source media="(prefers-color-scheme: dark)" srcset="../docs/kmod-logo-dark.svg">
  <img alt="Logo" src="../docs/kmod-logo-light.svg">
</picture>

[![License: AGPL 3.0](https://img.shields.io/github/license/loopholelabs/silo)](https://www.gnu.org/licenses/agpl-3.0.en.html)
[![Discord](https://dcbadge.vercel.app/api/server/JYmFhtdPeu?style=flat)](https://loopholelabs.io/discord)
</div>


## Build and Load

The kernel module requires Linux kernel v6.5+. Use the following command to
build the kernel module.

```bash
make module
```

This command generates a file called `memory-overlay.ko` that you can load into
the kernel using the command `sudo make load` and unload using `sudo make
unload`.

Set the `LOG_LEVEL` variable when building the kernel module to change log
verbosity.

```bash
make module LOG_LEVEL=2
```

The following log levels are available.

* `LOG_LEVEL=1`: `INFO` (default).
* `LOG_LEVEL=2`: `DEBUG`.
* `LOG_LEVEL=3`: `TRACE`.

## Testing and Examples

The folder [`tests/page_fault`](tests/page_fault) contains several userspace C
programs that can be used to test the kernel module and as examples on how to
use it.

The test programs map randomized test files into memory. You can generate them
using the [`tests/generate.go`](tests/generate.go) helper. This program
requires the Go toolchain to be installed. You can execute the helper with the
following command.

```bash
make tests-generate
```

After loading the module and generating the test data, you can build and
execute the test programs using the following command.

```bash
make tests
```

You can retrieve the kernel module output using the `sudo dmesg` command, or
run `sudo dmesg -w` in another window to actively follow the latest log output.

## Device Driver API

The kernel module creates a character device driver that is available in the
path `/dev/memory_overlay`. You can interact with the kernel module by sending
[`ioctl`][man_ioctl] commands to this device.

```c
int syscall_dev = open("/dev/memory_overlay", O_WRONLY);
if (syscall_dev < 0) {
	printf("ERROR: failed to open /dev/memory_overlay: %s\n", strerror(errno));
	return EXIT_FAILURE;
}

struct mem_overlay_req req;
// ...generate request...

int ret = ioctl(syscall_dev, IOCTL_MEM_OVERLAY_REQ_CMD, &req);
if (ret) {
	printf("ERROR: failed not call 'IOCTL_MMAP_CMD': %s\n", strerror(errno));
  close(syscall_dev);
  return EXIT_FAILURE;
}

// ...before exit...

struct mem_overlay_cleanup_req cleanup_req = {
	.id = req.id,
};
int ret = ioctl(syscall_dev, IOCTL_MEM_OVERLAY_CLEANUP_CMD, &cleanup_req);
if (ret) {
	printf("ERROR: could not call 'IOCTL_MMAP_CMD': %s\n", strerror(errno));
  close(syscall_dev);
	return EXIT_FAILURE;
}
```

The device driver uses the following commands, which are defined in the
[`common.h`](common.h) file.

### `IOCTL_MEM_OVERLAY_REQ_CMD` Command

The `IOCTL_MEM_OVERLAY_REQ_CMD` takes a `mem_overlay_req` as input and is used
to register a new set of memory overlays for a base memory area.

Each base memory can only be registered once.

#### `mem_overlay_req` Fields

```c
struct mem_overlay_req {
	unsigned long id;

	unsigned long base_addr;
	unsigned long overlay_addr;

	unsigned int segments_size;
	struct mem_overlay_segment_req *segments;
};
```

* `id`: Request identifier. This value is set by the kernel module if the
  command succeeds and should not be set when making the request.
* `base_addr`: Virtual address where the base file is mapped in memory.
* `overlay_addr`: Virtual address where the overlay file is mapped in memory.
* `segments_size`: The number of memory segments to overlay.
* `segments`: Array of memory segments to overlay.

#### `mem_overlay_segment_req` Fields

```c
struct mem_overlay_segment_req {
	unsigned long start_pgoff;
	unsigned long end_pgoff;
};
```

* `start_pgoff`: Page offset of where the segment start (inclusive).
* `end_pgoff`: Page offset of where the segment ends (inclusive).

#### Return Value

On success, a `0` is returned. On error, `-1` is returned, and
[`errno`][man_errno] is set to indicate the error.

#### Errors

* `EFAULT`: Internal module error. Refer to the kernel module logs for more
  information.
* `EINVAL`: Invalid base or overlay virtual memory address.
* `EEXIST`: Base file is already registered.
* `ENOMEM`: Failed to allocate memory.

### `IOCTL_MEM_OVERLAY_CLEANUP_CMD` Command

The `IOCTL_MEM_OVERLAY_CLEANUP_CMD` takes a `mem_overlay_cleanup_req` as input
and is used to remove a previous memory overlay request from the kernel module.

Every call to `IOCTL_MEM_OVERLAY_REQ_CMD` MUST be have a corresponding
`IOCTL_MEM_OVERLAY_CLEANUP_CMD` before the program exits. Fail to do so may
result kernel panics due to invalid memory pages left in the system.

#### `mem_overlay_cleanup_req` fields

```c
struct mem_overlay_cleanup_req {
	unsigned long id;
};
```

* `id`: Request identifier returned from a call to `IOCTL_MEM_OVERLAY_REQ_CMD`.

#### Return value

On success, a `0` is returned. On error, `-1` is returned, and
[`errno`][man_errno] is set to indicate the error.

#### Errors

* `EFAULT`: Internal module error. Refer to the kernel module logs for more
  information.
* `ENOENT`: Request ID not found.

## Known Issues

### Unsupported CPU architectures

The only CPU architecture currently supported is `x86-64`.

### Loading data from a FUSE file system

If the test files are stored in a FUSE file system, the kernel module may panic
with the following error.

```
[  937.417750] kernel BUG at mm/truncate.c:667!
[  937.418572] invalid opcode: 0000 [#1] PREEMPT SMP PTI
[  937.418927] CPU: 1 PID: 859139 Comm: page_fault_mult Tainted: G           OE      6.5.13-debug-v1 #6
[  937.419296] Hardware name: VMware, Inc. VMware Virtual Platform/440BX Desktop Reference Platform, BIOS 6.00 05/28/2020
[  937.419712] RIP: 0010:invalidate_inode_pages2_range+0x265/0x4d0
[  937.420160] Code: 85 c0 0f 8e 12 01 00 00 4c 89 ff e8 a5 9c 04 00 49 8b 07 a9 00 00 01 00 0f 84 c6 fe ff ff 41 8b 47 58 85 c0 0f 8e 01 01 00 00 <0f> 0b 4d 3b 67 18 0f 85 d2 fe ff ff be c0 0c 00 00 4c 89 ff e8 c2
[  937.421253] RSP: 0018:ffff99f3c1bf7ab8 EFLAGS: 00010246
[  937.421857] RAX: 0000000000000000 RBX: 0000000000000000 RCX: 0000000000000000
[  937.422444] RDX: 0000000000000000 RSI: 0000000000000000 RDI: 0000000000000000
[  937.423032] RBP: ffff99f3c1bf7c08 R08: 0000000000000000 R09: 0000000000000000
[  937.423669] R10: 0000000000000000 R11: 0000000000000000 R12: ffff8951cd68aef8
[  937.424284] R13: 0000000000000001 R14: 0000000000000000 R15: ffffed5884f9b040
[  937.424907] FS:  00007cfa2a7ec640(0000) GS:ffff8954efc80000(0000) knlGS:0000000000000000
[  937.425572] CS:  0010 DS: 0000 ES: 0000 CR0: 0000000080050033
[  937.426239] CR2: 000072bd02ffd658 CR3: 0000000116ff0002 CR4: 00000000003706e0
[  937.427030] Call Trace:
[  937.427747]  <TASK>
[  937.428474]  ? show_regs+0x72/0x90
[  937.429195]  ? die+0x38/0xb0
[  937.429918]  ? do_trap+0xe3/0x100
[  937.430687]  ? do_error_trap+0x75/0xb0
[  937.431426]  ? invalidate_inode_pages2_range+0x265/0x4d0
[  937.432190]  ? exc_invalid_op+0x53/0x80
[  937.432962]  ? invalidate_inode_pages2_range+0x265/0x4d0
[  937.433791]  ? asm_exc_invalid_op+0x1b/0x20
[  937.434633]  ? invalidate_inode_pages2_range+0x265/0x4d0
[  937.435458]  ? invalidate_inode_pages2_range+0x24b/0x4d0
[  937.436277]  ? fuse_put_request+0x9d/0x110
[  937.437106]  invalidate_inode_pages2+0x17/0x30
[  937.437722]  fuse_open_common+0x1cc/0x220
[  937.438313]  ? __pfx_fuse_open+0x10/0x10
[  937.438909]  fuse_open+0x10/0x20
[  937.439508]  do_dentry_open+0x187/0x590
[  937.440140]  vfs_open+0x33/0x50
[  937.440755]  path_openat+0xaed/0x10a0
[  937.441379]  ? asm_sysvec_call_function_single+0x1b/0x20
[  937.442034]  do_filp_open+0xb2/0x160
[  937.442525]  ? __pfx_autoremove_wake_function+0x10/0x10
[  937.443040]  ? alloc_fd+0xad/0x1a0
[  937.443547]  do_sys_openat2+0xa1/0xd0
[  937.444068]  __x64_sys_openat+0x55/0xa0
[  937.444585]  x64_sys_call+0xee8/0x2570
[  937.445104]  do_syscall_64+0x56/0x90
[  937.445631]  entry_SYSCALL_64_after_hwframe+0x73/0xdd
[  937.446167] RIP: 0033:0x7cfac49145b4
[  937.446715] Code: 24 20 eb 8f 66 90 44 89 54 24 0c e8 56 c4 f7 ff 44 8b 54 24 0c 44 89 e2 48 89 ee 41 89 c0 bf 9c ff ff ff b8 01 01 00 00 0f 05 <48> 3d 00 f0 ff ff 77 34 44 89 c7 89 44 24 0c e8 98 c4 f7 ff 8b 44
[  937.447783] RSP: 002b:00007cfa2a7ebd90 EFLAGS: 00000293 ORIG_RAX: 0000000000000101
[  937.448308] RAX: ffffffffffffffda RBX: 00007cfa2a7ec640 RCX: 00007cfac49145b4
[  937.448812] RDX: 0000000000000000 RSI: 000063bc9c8be020 RDI: 00000000ffffff9c
[  937.449316] RBP: 000063bc9c8be020 R08: 0000000000000000 R09: 000000007fffffff
[  937.449817] R10: 0000000000000000 R11: 0000000000000293 R12: 0000000000000000
[  937.450307] R13: 0000000000000000 R14: 00007cfac48947d0 R15: 00007ffd47198540
[  937.450799]  </TASK>
[  937.451256] Modules linked in: batch_syscalls(OE) vsock_loopback(E) vmw_vsock_virtio_transport_common(E) vmw_vsock_vmci_transport(E) vsock(E) binfmt_misc(E) intel_rapl_msr(E) intel_rapl_common(E) vmw_balloon(E) rapl(E) joydev(E) input_leds(E) serio_raw(E) vmw_vmci(E) mac_hid(E) sch_fq_codel(E) dm_multipath(E) scsi_dh_rdac(E) scsi_dh_emc(E) scsi_dh_alua(E) msr(E) efi_pstore(E) ip_tables(E) x_tables(E) autofs4(E) btrfs(E) blake2b_generic(E) raid10(E) raid456(E) async_raid6_recov(E) async_memcpy(E) async_pq(E) async_xor(E) async_tx(E) xor(E) raid6_pq(E) libcrc32c(E) raid1(E) raid0(E) multipath(E) linear(E) hid_generic(E) crct10dif_pclmul(E) crc32_pclmul(E) polyval_clmulni(E) polyval_generic(E) ghash_clmulni_intel(E) sha256_ssse3(E) sha1_ssse3(E) aesni_intel(E) crypto_simd(E) cryptd(E) usbhid(E) hid(E) vmwgfx(E) drm_ttm_helper(E) psmouse(E) ttm(E) drm_kms_helper(E) vmxnet3(E) mptspi(E) drm(E) ahci(E) mptscsih(E) libahci(E) mptbase(E) scsi_transport_spi(E) i2c_piix4(E) pata_acpi(E)
[  937.454514] ---[ end trace 0000000000000000 ]---
[  937.454974] RIP: 0010:invalidate_inode_pages2_range+0x265/0x4d0
[  937.455442] Code: 85 c0 0f 8e 12 01 00 00 4c 89 ff e8 a5 9c 04 00 49 8b 07 a9 00 00 01 00 0f 84 c6 fe ff ff 41 8b 47 58 85 c0 0f 8e 01 01 00 00 <0f> 0b 4d 3b 67 18 0f 85 d2 fe ff ff be c0 0c 00 00 4c 89 ff e8 c2
[  937.456403] RSP: 0018:ffff99f3c1bf7ab8 EFLAGS: 00010246
[  937.456897] RAX: 0000000000000000 RBX: 0000000000000000 RCX: 0000000000000000
[  937.457354] RDX: 0000000000000000 RSI: 0000000000000000 RDI: 0000000000000000
[  937.457893] RBP: ffff99f3c1bf7c08 R08: 0000000000000000 R09: 0000000000000000
[  937.458363] R10: 0000000000000000 R11: 0000000000000000 R12: ffff8951cd68aef8
[  937.458813] R13: 0000000000000001 R14: 0000000000000000 R15: ffffed5884f9b040
[  937.459255] FS:  00007cfa2a7ec640(0000) GS:ffff8954efc80000(0000) knlGS:0000000000000000
[  937.459694] CS:  0010 DS: 0000 ES: 0000 CR0: 0000000080050033
[  937.460133] CR2: 000072bd02ffd658 CR3: 0000000116ff0002 CR4: 00000000003706e0
```

To avoid this issue, run the tests from a non-FUSE file system.

## Contributing

Bug reports and pull requests are welcome on GitHub at
[https://github.com/loopholelabs/kmod-batch-syscalls][gitrepo]. For more
contribution information check out [the contribution
guide](https://github.com/loopholelabs/kmod-batch-syscalls/blob/master/CONTRIBUTING.md).

## License

This Kernel Module is available as open source under the terms of
the [GPL v3 License](https://www.gnu.org/licenses/gpl-3.0.en.html).

## Code of Conduct

Everyone interacting in this project’s codebases, issue trackers, chat rooms
and mailing lists is expected to follow the [CNCF Code of
Conduct](https://github.com/cncf/foundation/blob/master/code-of-conduct.md).

## Project Managed By:

[![https://loopholelabs.io][loopholelabs]](https://loopholelabs.io)

[gitrepo]: https://github.com/loopholelabs/kmod-batch-syscalls
[loopholelabs]: https://cdn.loopholelabs.io/loopholelabs/LoopholeLabsLogo.svg
[loophomepage]: https://loopholelabs.io
[man_errno]: https://man7.org/linux/man-pages/man3/errno.3.html
[man_ioctl]: https://www.man7.org/linux/man-pages/man2/ioctl.2.html
