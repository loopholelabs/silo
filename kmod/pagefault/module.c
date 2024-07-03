// SPDX-License-Identifier: GPL-3.0

#include <linux/module.h>
#include <linux/init.h>
#include <linux/fs.h>
#include <linux/file.h>
#include <linux/ioctl.h>
#include <linux/uaccess.h>
#include <linux/slab.h>
#include <linux/mm.h>
#include <linux/mm_types.h>
#include <linux/device.h>
#include <linux/time.h>
#include <linux/xarray.h>
#include <linux/percpu_counter.h>

#include <asm/io.h>

#include "module.h"
#include "device.h"
#include "hashtable.h"
#include "log.h"

MODULE_AUTHOR("Loophole Labs (Shivansh Vij, Luiz Aoqui)");
MODULE_DESCRIPTION("Silo Page Fault Handler");
MODULE_LICENSE("GPL");

static struct hashtable *overlays;

static vm_fault_t hijacked_map_pages(struct vm_fault *vmf, pgoff_t start_pgoff, pgoff_t end_pgoff)
{
	unsigned long id = (unsigned long)vmf->vma;
	log_debug("page fault page=%lu start=%lu end=%lu id=%lu", vmf->pgoff, start_pgoff, end_pgoff, id);
	struct overlay *overlay = hashtable_lookup(overlays, id);
	if (!overlay) {
		log_error("unable to find overlay id=%lu", id);
		return VM_FAULT_SIGBUS;
	}

	XA_STATE(xas, &overlay->segments, start_pgoff);
	struct overlay_segment *seg;
	vm_fault_t ret;
	pgoff_t end;

	rcu_read_lock();
	for (pgoff_t start = start_pgoff; start <= end_pgoff; start = end + 1) {
		do {
			seg = xas_find(&xas, end_pgoff);
		} while (xas_retry(&xas, seg));

		// The range doesn't overlap with any segment, so handle it like a
		// normal page fault.
		if (seg == NULL) {
			end = end_pgoff;
			log_debug("handling base page fault start=%lu end=%lu id=%lu", start, end, id);
			ret = filemap_map_pages(vmf, start, end);
			break;
		}

		// Handle any non-overlay range before the next segment.
		if (start < seg->start_pgoff) {
			end = seg->start_pgoff - 1;
			log_debug("handling base page fault start=%lu end=%lu id=%lu", start, end, id);
			ret = filemap_map_pages(vmf, start, end);
			if (ret & VM_FAULT_ERROR) {
				break;
			}
			start = end + 1;
		}

		// Handle fault over overlay range.
		end = min(seg->end_pgoff, end_pgoff);
		log_debug("handling overlay page fault start=%lu end=%lu id=%lu", start, end, id);

		// Make a copy of the target VMA to change its source file without
		// affecting any potential concurrent reader.
		struct vm_area_struct *vma_orig = vmf->vma;
		struct vm_area_struct vma_copy;
		memcpy(&vma_copy, vmf->vma, sizeof(struct vm_area_struct));
		vma_copy.vm_file = seg->overlay_vma->vm_file;

		// Use a pointer to the vmf->vma pointer to alter its reference since
		// this field is marked as a const.
		struct vm_area_struct **vma_p = (struct vm_area_struct **)&vmf->vma;
		*vma_p = &vma_copy;
		ret = filemap_map_pages(vmf, start, end);
		*vma_p = vma_orig;
		if (ret & VM_FAULT_ERROR) {
			break;
		}
	}
	rcu_read_unlock();
	return ret;
}

static void cleanup_overlay_segments(struct xarray segments)
{
	unsigned long i;
	struct overlay_segment *seg;
	xa_for_each(&segments, i, seg) {
		kvfree(seg);
		i = seg->end_pgoff + 1;
	}
	xa_destroy(&segments);
}

/*
 * Free memory used by an overlay entry. If the process that owns the
 * memory can be assumed to still be running, a mm mmap write lock should be
 * held before calling this function.
 */
static void cleanup_overlay(void *data)
{
	struct overlay *overlay = (struct overlay *)data;

	// Revert base VMA vm_ops to its original value in case the VMA is used
	// after cleanup (usually to call ->close() on unmap).
	const struct vm_operations_struct *vm_ops = overlay->base_vma->vm_ops;
	if (vm_ops != NULL && vm_ops->map_pages == hijacked_map_pages) {
		overlay->base_vma->vm_ops = overlay->original_vm_ops;
	}

	kvfree(overlay->hijacked_vm_ops);
	cleanup_overlay_segments(overlay->segments);
	kvfree(overlay);
}

static int device_open(struct inode *device_file, struct file *instance)
{
	return 0;
}

static int device_close(struct inode *device_file, struct file *instance)
{
	return 0;
}

static long int unlocked_ioctl_handle_overlay_req(unsigned long arg)
{
	long int res = 0;

	// Read request data from userspace.
	struct overlay_req req;
	unsigned long ret = copy_from_user(&req, (struct overlay_req *)arg, sizeof(struct overlay_req));
	if (ret) {
		log_error("failed to copy overlay request from user: %lu", ret);
		return -EFAULT;
	}

	// Acquire mm write lock since we expect to mutate the base VMA.
	// TODO: support per-VMA locking (https://lwn.net/Articles/937943/).
	mmap_write_lock(current->mm);

	// Find base and overlay VMAs.
	struct vm_area_struct *overlay_vma = find_vma(current->mm, req.overlay_addr);
	if (overlay_vma == NULL || overlay_vma->vm_start > req.overlay_addr) {
		log_error("failed to find overlay VMA");
		res = -EINVAL;
		goto out;
	}

	struct vm_area_struct *base_vma = find_vma(current->mm, req.base_addr);
	if (base_vma == NULL || base_vma->vm_start > req.base_addr) {
		log_error("failed to find base VMA");
		res = -EINVAL;
		goto out;
	}
	unsigned long id = (unsigned long)base_vma;

	// Check if VMA is already stored.
	struct overlay *overlay = hashtable_lookup(overlays, id);
	if (overlay) {
		if (base_vma->vm_ops->map_pages == hijacked_map_pages) {
			log_error("overlay already exists");
			res = -EEXIST;
			goto out;
		}

		// Leftover overlay, delete from state and proceed.
		cleanup_overlay(overlay);
		hashtable_delete(overlays, id);
		overlay = NULL;
	}

	// Read overlay segments from request.
	struct overlay_segment_req *segs = kvzalloc(sizeof(struct overlay_segment) * req.segments_size, GFP_KERNEL);
	if (!segs) {
		log_error("failed to allocate segments");
		res = -ENOMEM;
		goto out;
	}
	ret = copy_from_user(segs, req.segments, sizeof(struct overlay_segment_req) * req.segments_size);
	if (ret) {
		log_error("failed to copy overlay segments request from user: %lu", ret);
		res = -EFAULT;
		goto free_segs;
	}

	log_debug("received overlay request base_addr=%lu overlay_addr=%lu", req.base_addr, req.overlay_addr);

	// Create new overlay instance.
	overlay = kvzalloc(sizeof(struct overlay), GFP_KERNEL);
	if (!overlay) {
		log_error("failed to allocate memory for overlay");
		res = -ENOMEM;
		goto free_segs;
	}

	overlay->base_addr = req.base_addr;
	xa_init(&(overlay->segments));

	struct overlay_segment *seg;
	for (int i = 0; i < req.segments_size; i++) {
		unsigned long start = segs[i].start_pgoff;
		unsigned long end = segs[i].end_pgoff;

		seg = kvzalloc(sizeof(struct overlay_segment), GFP_KERNEL);
		if (!seg) {
			log_error("failed to allocate memory for overlay segment start=%lu end=%lu", start, end);
			res = -ENOMEM;
			goto cleanup_segments;
		}

		seg->start_pgoff = start;
		seg->end_pgoff = end;
		seg->overlay_addr = req.overlay_addr;
		seg->overlay_vma = overlay_vma;

		log_debug("inserting segment to overlay start=%lu end=%lu", start, end);
		xa_store_range(&overlay->segments, start, end, seg, GFP_KERNEL);
	}

	// Hijack page fault handler for base VMA.
	log_info("hijacking vm_ops for base VMA addr=0x%lu", req.base_addr);
	overlay->hijacked_vm_ops = kvzalloc(sizeof(struct vm_operations_struct), GFP_KERNEL);
	if (!overlay->hijacked_vm_ops) {
		log_error("failed to allocate memory for hijacked vm_ops");
		res = -ENOMEM;
		goto cleanup_segments;
	}

	// Store base VMA and original vm_ops (so we can restore it on cleanup).
	overlay->base_vma = base_vma;
	overlay->original_vm_ops = base_vma->vm_ops;

	memcpy(overlay->hijacked_vm_ops, base_vma->vm_ops, sizeof(struct vm_operations_struct));
	overlay->hijacked_vm_ops->map_pages = hijacked_map_pages;
	base_vma->vm_ops = overlay->hijacked_vm_ops;
	log_info("done hijacking vm_ops addr=0x%lu", req.base_addr);

	// Save overlay into hashtable.
	int iret = hashtable_insert(overlays, id, overlay);
	if (iret) {
		log_error("failed to insert overlay into hashtable: %d", iret);
		res = -EFAULT;
		goto revert_vm_ops;
	}

	// Return ID to userspace request.
	req.id = id;
	ret = copy_to_user((struct overlay_req *)arg, &req, sizeof(struct overlay_req));
	if (ret) {
		log_error("failed to copy overlay ID to user: %lu", ret);
		res = -EFAULT;
		goto revert_vm_ops;
	}

	log_info("overlay created successfully id=%lu", id);
	goto free_segs;

revert_vm_ops:
	base_vma->vm_ops = overlay->original_vm_ops;
	kvfree(overlay->hijacked_vm_ops);
cleanup_segments:
	cleanup_overlay_segments(overlay->segments);
	kvfree(overlay);
free_segs:
	kvfree(segs);
out:
	mmap_write_unlock(current->mm);
	return res;
}

static long int unlocked_ioctl_handle_overlay_cleanup_req(unsigned long arg)
{
	struct overlay_cleanup_req req;
	unsigned long ret =
		copy_from_user(&req, (struct overlay_cleanup_req *)arg, sizeof(struct overlay_cleanup_req));
	if (ret) {
		log_error("failed to copy overlay cleanup request from user: %lu", ret);
		return -EFAULT;
	}

	struct overlay *overlay = hashtable_delete(overlays, req.id);
	if (!overlay) {
		log_error("failed to cleanup overlay id=%lu", req.id);
		return -ENOENT;
	}

	struct mm_struct *mm = overlay->base_vma->vm_mm;
	mmap_write_lock(mm);
	cleanup_overlay(overlay);
	mmap_write_unlock(mm);

	log_info("overlay removed successfully id=%lu", req.id);
	return 0;
}

static long int unlocked_ioctl(struct file *file, unsigned cmd, unsigned long arg)
{
	switch (cmd) {
	case IOCTL_OVERLAY_REQ_CMD:
		log_debug("called IOCTL_OVERLAY_REQ_CMD");
		return unlocked_ioctl_handle_overlay_req(arg);
	case IOCTL_OVERLAY_CLEANUP_CMD:
		log_debug("called IOCTL_OVERLAY_CLEANUP_CMD");
		return unlocked_ioctl_handle_overlay_cleanup_req(arg);
	default:
		log_error("unknown ioctl cmd %x", cmd);
	}
	return -EINVAL;
}

static struct file_operations file_ops = { .owner = THIS_MODULE,
					   .open = device_open,
					   .release = device_close,
					   .unlocked_ioctl = unlocked_ioctl };

static unsigned int major;
static dev_t device_number;
static struct class *device_class;

static int __init init_mod(void)
{
	START_FOLLOW;
	overlays = hashtable_setup(&cleanup_overlay);
	log_info("registering device with major %u and ID '%s'", (unsigned int)MAJOR_DEV, DEVICE_ID);
	int ret = register_chrdev(MAJOR_DEV, DEVICE_ID, &file_ops);
	if (!ret) {
		major = MAJOR_DEV;
		log_info("registered device (major %d, minor %d)", major, 0);
		device_number = MKDEV(major, 0);
	} else if (ret > 0) {
		major = ret >> 20;
		log_info("registered device (major %d, minor %d)", major, ret & 0xfffff);
		device_number = MKDEV(major, ret & 0xfffff);
	} else {
		log_error("unable to register device: %d", ret);
		return ret;
	}

	log_debug("creating device class with ID '%s'", DEVICE_ID);
	device_class = class_create(DEVICE_ID);
	if (IS_ERR(device_class)) {
		log_error("unable to create device class");
		unregister_chrdev(major, DEVICE_ID);
		return -EINVAL;
	}

	log_debug("creating device with id '%s'", DEVICE_ID);
	struct device *device = device_create(device_class, NULL, device_number, NULL, DEVICE_ID);
	if (IS_ERR(device)) {
		log_error("unable to create device");
		class_destroy(device_class);
		unregister_chrdev(major, DEVICE_ID);
		return -EINVAL;
	}
	END_FOLLOW;
	return 0;
}

static void __exit exit_mod(void)
{
	START_FOLLOW;
	if (overlays) {
		log_info("cleaning up overlays hashtable");
		hashtable_cleanup(overlays);
		overlays = NULL;
	}
	log_info("unregistering device with major %u and ID '%s'", (unsigned int)major, DEVICE_ID);
	device_destroy(device_class, device_number);
	class_destroy(device_class);
	unregister_chrdev(major, DEVICE_ID);
	END_FOLLOW;
}

module_init(init_mod);
module_exit(exit_mod);
