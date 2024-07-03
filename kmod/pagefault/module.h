// SPDX-License-Identifier: GPL-3.0

#include <linux/xarray.h>

#ifndef SILO_MODULE_H
#define SILO_MODULE_H

#define MAJOR_DEV 64
#define DEVICE_ID "silo_pagefault"

struct overlay_segment {
	unsigned long overlay_addr;
	struct vm_area_struct *overlay_vma;

	unsigned long start_pgoff;
	unsigned long end_pgoff;
};

struct overlay {
	unsigned long base_addr;
	struct vm_area_struct *base_vma;
	struct xarray segments;

	const struct vm_operations_struct *original_vm_ops;
	struct vm_operations_struct *hijacked_vm_ops;
};

#endif //SILO_MODULE_H
