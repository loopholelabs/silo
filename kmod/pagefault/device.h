// SPDX-License-Identifier: GPL-2.0

#ifndef SILO_DEVICE_H
#define SILO_DEVICE_H

#define MAGIC 'p'
#define IOCTL_OVERLAY_REQ_CMD _IOWR(MAGIC, 1, struct overlay_req *)
#define IOCTL_OVERLAY_CLEANUP_CMD _IOWR(MAGIC, 2, struct overlay_cleanup_req *)

static const char device_path[] = "/dev/silo_pagefault";

struct overlay_segment_req {
	unsigned long start_pgoff;
	unsigned long end_pgoff;
};

struct overlay_req {
	unsigned long id;

	unsigned long base_addr;
	unsigned long overlay_addr;

	unsigned int segments_size;
	struct overlay_segment_req *segments;
};

struct overlay_cleanup_req {
	unsigned long id;
};

#endif //SILO_DEVICE_H
