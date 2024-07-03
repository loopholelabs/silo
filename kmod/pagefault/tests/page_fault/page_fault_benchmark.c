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

#include "../../common.h"

// N represents a memory overlay fragmentation factor. The benchmark setup code
// splits the base memory area into groups of N pages and overlays every other
// group.
//
// The higher the value of N, the larger each memory overlay segment is, and
// fewer segments are created.
//
// The examples below illustrate how the base memory is overlaid for different
// values of N (X marks overlaid pages).
//
//   N=2
//     1   2   3   4   5   6   7   8
//   +---+---+---+---+---+---+---+---+
//   | x | x |   |   | x | x |   |   |
//   +---+---+---+---+---+---+---+---+
//   | x | x |   |   | x | x |   |   |
//   +---+---+---+---+---+---+---+---+
//   | x | x |   |   | x | x |   |   |
//   +---+---+---+---+---+---+---+---+
//   :   :   :   :   :   :   :   :   :
//
//   N=5
//     1   2   3   4   5   6   7   8
//   +---+---+---+---+---+---+---+---+
//   | x | x | x | x | x |   |   |   |
//   +---+---+---+---+---+---+---+---+
//   |   |   | x | x | x | x | x |   |
//   +---+---+---+---+---+---+---+---+
//   |   |   |   |   | x | x | x | x |
//   +---+---+---+---+---+---+---+---+
//   :   :   :   :   :   :   :   :   :
//
static const int N = 10;

size_t PAGE_SIZE, TOTAL_SIZE, TOTAL_PAGES;

static const char BASE_FILE[] = "baseXL.bin";
static const char CLEAN_BASE_FILE[] = "baseXL2.bin";
static const char OVERLAY_FILE[] = "overlayXL.bin";
static const int PAGE_SIZE_FACTOR = 1024 * 1024;

bool verify_test_cases(int overlay_fd, int base_fd, char *base_map)
{
	char *buffer = calloc(PAGE_SIZE, 1);
	bool valid = true;

	struct timespec before, after;
	if (clock_gettime(CLOCK_MONOTONIC, &before) < 0) {
		printf("ERROR: could not measure 'before' time for base mmap: %s\n",
		       strerror(errno));
		valid = false;
		goto out;
	}

	printf("%ld.%.9ld: starting memory check\n", before.tv_sec,
	       before.tv_nsec);

	for (unsigned long pgoff = 0; pgoff < TOTAL_PAGES; pgoff++) {
		size_t offset = pgoff * PAGE_SIZE;

		int fd = base_fd;
		bool is_overlay = pgoff % (2 * N) >= 0 && pgoff % (2 * N) < N;
		if (overlay_fd > 0 && is_overlay) {
			fd = overlay_fd;
		}
		lseek(fd, offset, SEEK_SET);
		read(fd, buffer, PAGE_SIZE);

		if (memcmp(base_map + offset, buffer, PAGE_SIZE)) {
			printf("== ERROR: base memory does not match the file contents at page %lu\n",
			       pgoff);
			valid = false;
			break;
		}
		memset(buffer, 0, PAGE_SIZE);
	}

	if (clock_gettime(CLOCK_MONOTONIC, &after) < 0) {
		printf("ERROR: could not measure 'after' time for base mmap: %s\n",
		       strerror(errno));
		valid = false;
		goto out;
	}

	printf("%ld.%.9ld: finished memory check\n", after.tv_sec,
	       after.tv_nsec);

	long secs_diff = after.tv_sec - before.tv_sec;
	long nsecs_diff = after.tv_nsec;
	if (secs_diff == 0) {
		nsecs_diff = after.tv_nsec - before.tv_nsec;
	}
	printf("test verification took %ld.%.9lds\n", secs_diff, nsecs_diff);

out:
	free(buffer);
	return valid;
}

void clear_cache()
{
	sync();
	int cache_fd = open("/proc/sys/vm/drop_caches", O_WRONLY);
	write(cache_fd, "3", 1);
	close(cache_fd);
}

int main()
{
	int res = EXIT_SUCCESS;

	PAGE_SIZE = sysconf(_SC_PAGESIZE);
	TOTAL_SIZE = PAGE_SIZE * PAGE_SIZE_FACTOR;
	TOTAL_PAGES = TOTAL_SIZE / PAGE_SIZE;
	printf("Page size:     %lu bytes\n", PAGE_SIZE);
	printf("Overlay size:  %d pages\n", N);
	printf("Total size:    %lu bytes\n", TOTAL_SIZE);
	printf("Total pages:   %lu pages\n", TOTAL_PAGES);

	// Read base.bin test file and mmap it into memory.
	int base_fd = open(BASE_FILE, O_RDONLY);
	if (base_fd < 0) {
		printf("ERROR: could not open base file %s: %s\n", BASE_FILE,
		       strerror(errno));
		return EXIT_FAILURE;
	}

	int clean_base_fd = open(CLEAN_BASE_FILE, O_RDONLY);
	if (clean_base_fd < 0) {
		printf("ERROR: could not open clean base file %s: %s\n",
		       CLEAN_BASE_FILE, strerror(errno));
		close(base_fd);
		return EXIT_FAILURE;
	}

	char *base_mmap = mmap(NULL, TOTAL_SIZE, PROT_READ | PROT_WRITE,
			       MAP_PRIVATE, base_fd, 0);
	if (base_mmap == MAP_FAILED) {
		printf("ERROR: could not mmap base file %s: %s\n", BASE_FILE,
		       strerror(errno));
		res = EXIT_FAILURE;
		goto close_base;
	}

	char *clean_base_mmap = mmap(NULL, TOTAL_SIZE, PROT_READ, MAP_PRIVATE,
				     clean_base_fd, 0);
	if (clean_base_mmap == MAP_FAILED) {
		printf("ERROR: could not mmap second base file %s: %s\n",
		       CLEAN_BASE_FILE, strerror(errno));
		res = EXIT_FAILURE;
		goto close_base;
	}

	// Read overlay test file and create memory overlay request.
	int overlay_fd = open(OVERLAY_FILE, O_RDONLY);
	if (overlay_fd < 0) {
		printf("ERROR: could not open overlay file %s: %s\n",
		       OVERLAY_FILE, strerror(errno));
		res = EXIT_FAILURE;
		goto unmap_base;
	}

	char *overlay_map =
		mmap(NULL, TOTAL_SIZE, PROT_READ, MAP_PRIVATE, overlay_fd, 0);
	if (overlay_map == MAP_FAILED) {
		printf("ERROR: could not mmap overlay file %s: %s\n",
		       OVERLAY_FILE, strerror(errno));
		res = EXIT_FAILURE;
		goto close_overlay;
	}

	struct mem_overlay_req req;
	req.base_addr = *(unsigned long *)(&base_mmap);
	req.overlay_addr = *(unsigned long *)(&overlay_map);
	req.segments_size = (TOTAL_PAGES + (2 * N - 1)) / (2 * N);
	req.segments = calloc(sizeof(struct mem_overlay_segment_req),
			      req.segments_size);

	printf("Requesting %u operations and sending %lu bytes worth of mmap segments\n",
	       req.segments_size,
	       sizeof(struct mem_overlay_segment_req) * req.segments_size);

	// Overlay every other N pages.
	for (int i = 0; i < req.segments_size; i++) {
		req.segments[i].start_pgoff = 2 * N * i;

		// Cap segment end to the number of pages in the base memory area.
		unsigned long end = 2 * N * i + (N - 1);
		end = end > TOTAL_PAGES ? TOTAL_PAGES : end;
		req.segments[i].end_pgoff = end;
	}

	// Call kernel module with ioctl call to the character device.
	int syscall_dev = open(kmod_device_path, O_WRONLY);
	if (syscall_dev < 0) {
		printf("ERROR: could not open %s: %s\n", kmod_device_path,
		       strerror(errno));
		res = EXIT_FAILURE;
		goto free_segments;
	}

	int ret = ioctl(syscall_dev, IOCTL_MEM_OVERLAY_REQ_CMD, &req);
	if (ret) {
		printf("ERROR: could not call 'IOCTL_MMAP_CMD': %s\n",
		       strerror(errno));
		res = EXIT_FAILURE;
		goto close_syscall_dev;
	}

	printf("= TEST: checking memory contents with overlay\n");
	clear_cache();
	if (!verify_test_cases(overlay_fd, base_fd, base_mmap)) {
		res = EXIT_FAILURE;
		goto cleanup;
	}
	printf("== OK: overlay memory verification completed successfully!\n");

	printf("= TEST: checking memory contents without overlay\n");
	clear_cache();
	if (!verify_test_cases(-1, clean_base_fd, clean_base_mmap)) {
		res = EXIT_FAILURE;
		goto cleanup;
	}
	printf("== OK: non-overlay memory verification completed successfully!\n");

cleanup:
	// Clean up memory overlay.
	printf("calling IOCTL_MEM_OVERLAY_CLEANUP_CMD\n");
	struct mem_overlay_cleanup_req cleanup_req = {
		.id = req.id,
	};
	ret = ioctl(syscall_dev, IOCTL_MEM_OVERLAY_CLEANUP_CMD, &cleanup_req);
	if (ret) {
		printf("ERROR: could not call 'IOCTL_MMAP_CMD': %s\n",
		       strerror(errno));
		res = EXIT_FAILURE;
	}
close_syscall_dev:
	close(syscall_dev);
free_segments:
	free(req.segments);
	munmap(overlay_map, TOTAL_SIZE);
close_overlay:
	close(overlay_fd);
unmap_base:
	munmap(clean_base_mmap, TOTAL_SIZE);
	munmap(base_mmap, TOTAL_SIZE);
close_base:
	close(clean_base_fd);
	close(base_fd);

	printf("done\n");
	return res;
}
