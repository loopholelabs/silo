/*
    Copyright (C) 2024 Loophole Labs

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program. If not, see <https://www.gnu.org/licenses/>.
*/

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
