// SPDX-License-Identifier: GPL-3.0

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <stdbool.h>

#include <bits/time.h>

#include <sys/ioctl.h>
#include <sys/mman.h>
#include <sys/stat.h>

#include "../../device.h"

struct test_case {
	unsigned long pgoff;
	int fd;
	char *data;
};

size_t page_size, total_size;

static const char base_file[] = "base.bin";
static const char overlay_file[] = "overlay.bin";
static const int page_size_factor = 1024;

bool verify_test_cases(struct test_case *tcs, int tcs_nr, int base_fd,
		       char *base_map)
{
	char *buffer = malloc(page_size);
	memset(buffer, 0, page_size);
	bool valid = true;

	for (unsigned long pgoff = 0; pgoff < total_size / page_size; pgoff++) {
		size_t offset = pgoff * page_size;

		struct test_case *tc = NULL;
		for (int i = 0; i < tcs_nr; i++) {
			if (tcs[i].pgoff == pgoff) {
				tc = &tcs[i];
				break;
			}
		}

		int fd = base_fd;
		if (tc != NULL) {
			if (tc->fd > 0) {
				fd = tc->fd;
				printf("checking if page %lu is from overlay\n",
				       pgoff);
			} else if (tc->data) {
				printf("checking if page %lu has expected data\n",
				       pgoff);
				memcpy(buffer, tc->data, page_size);
				fd = -1;
			}
		}

		if (fd > 0) {
			lseek(fd, offset, SEEK_SET);
			read(fd, buffer, page_size);
		}

		if (memcmp(base_map + offset, buffer, page_size)) {
			printf("== ERROR: base memory does not match the file contents at page %lu\n",
			       pgoff);
			valid = false;
			break;
		}
		memset(buffer, 0, page_size);
	}

	free(buffer);
	return valid;
}

int main()
{
	int res = EXIT_SUCCESS;

	page_size = sysconf(_SC_PAGESIZE);
	total_size = page_size * page_size_factor;
	printf("Using pagesize %lu with total size %lu\n", page_size,
	       total_size);

	// Read base.bin test file and mmap it into memory.
	int base_fd = open(base_file, O_RDONLY);
	if (base_fd < 0) {
		printf("ERROR: could not open base file %s: %s\n", base_file,
		       strerror(errno));
		return EXIT_FAILURE;
	}
	printf("opened base file %s\n", base_file);

	char *base_mmap = mmap(NULL, total_size, PROT_READ | PROT_WRITE,
			       MAP_PRIVATE, base_fd, 0);
	if (base_mmap == MAP_FAILED) {
		printf("ERROR: could not mmap base file %s: %s\n", base_file,
		       strerror(errno));
		res = EXIT_FAILURE;
		goto close_base;
	}
	printf("mapped base file %s\n", base_file);

	// Read overlay test file and create memory overlay request.
	int overlay_fd = open(overlay_file, O_RDONLY);
	if (overlay_fd < 0) {
		printf("ERROR: could not open overlay file %s: %s\n",
		       overlay_file, strerror(errno));
		res = EXIT_FAILURE;
		goto unmap_base;
	}
	printf("opened overlay file %s\n", overlay_file);

	char *overlay_map =
		mmap(NULL, total_size, PROT_READ, MAP_PRIVATE, overlay_fd, 0);
	if (overlay_map == MAP_FAILED) {
		printf("ERROR: could not mmap overlay file %s: %s\n",
		       overlay_file, strerror(errno));
		res = EXIT_FAILURE;
		goto close_overlay;
	}
	printf("mapped overlay file %s\n", overlay_file);

	struct overlay_req req;
	req.base_addr = *(unsigned long *)(&base_mmap);
	req.overlay_addr = *(unsigned long *)(&overlay_map);
	req.segments_size = 1;
	req.segments = malloc(sizeof(struct overlay_segment_req) *
			      req.segments_size);
	memset(req.segments, 0,
	       sizeof(struct overlay_segment_req) * req.segments_size);

	// Overlay single page.
	req.segments[0].start_pgoff = 0;
	req.segments[0].end_pgoff = 0;

	// Call kernel module with ioctl call to the character device.
	int syscall_dev = open(device_path, O_WRONLY);
	if (syscall_dev < 0) {
		printf("ERROR: could not open %s: %d\n", device_path,
		       syscall_dev);
		res = EXIT_FAILURE;
		goto free_segments;
	}

	printf("= TEST: verify IOCTL_OVERLAY_REQ_CMD succeeds\n");
	int ret;
	ret = ioctl(syscall_dev, IOCTL_OVERLAY_REQ_CMD, &req);
	if (ret) {
		printf("== ERROR: could not call 'IOCTL_MMAP_CMD': %s\n",
		       strerror(errno));
		res = EXIT_FAILURE;
		goto close_syscall_dev;
	}
	printf("== OK: called IOCTL_OVERLAY_REQ_CMD successfully!\n");

	// Call the kernel module a second time and verify it fails.
	printf("= TEST: verify second call to IOCTL_OVERLAY_REQ_CMD fails\n");
	ret = ioctl(syscall_dev, IOCTL_OVERLAY_REQ_CMD, &req);
	if (errno != EEXIST) {
		printf("== ERROR: expected call to 'IOCTL_MMAP_CMD' to return %d, got %d.\n",
		       EEXIST, errno);
		res = EXIT_FAILURE;
		goto close_syscall_dev;
	}
	printf("== OK: second call to IOCTL_OVERLAY_REQ_CMD failed successfully!\n");

	// Clean up memory overlay.
	printf("= TEST: verify IOCTL_OVERLAY_CLEANUP_CMD succeeds\n");
	struct overlay_cleanup_req cleanup_req = {
		.id = req.id,
	};
	ret = ioctl(syscall_dev, IOCTL_OVERLAY_CLEANUP_CMD, &cleanup_req);
	if (ret) {
		printf("== ERROR: could not call 'IOCTL_MMAP_CMD': %s\n",
		       strerror(errno));
		res = EXIT_FAILURE;
	}
	printf("== OK: called IOCTL_OVERLAY_CLEANUP_CMD successfully!\n");

	printf("= TEST: verify second call to IOCTL_OVERLAY_CLEANUP_CMD fails\n");
	ret = ioctl(syscall_dev, IOCTL_OVERLAY_CLEANUP_CMD, &cleanup_req);
	if (errno != ENOENT) {
		printf("== ERROR: expected call to 'IOCTL_MMAP_CMD' to return %d, got %d\n",
		       ENOENT, errno);
		res = EXIT_FAILURE;
		goto close_syscall_dev;
	}
	printf("== OK: called IOCTL_OVERLAY_CLEANUP_CMD successfully!\n");
	res = EXIT_SUCCESS;
close_syscall_dev:
	close(syscall_dev);
	printf("closed device driver\n");
free_segments:
	free(req.segments);
	munmap(overlay_map, total_size);
	printf("unmapped overlay file\n");
close_overlay:
	close(overlay_fd);
	printf("closed overlay file\n");
unmap_base:
	munmap(base_mmap, total_size);
	printf("unmapped base file\n");
close_base:
	close(base_fd);
	printf("closed base file\n");
	printf("done\n");
	return res;
}
