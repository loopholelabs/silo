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

struct test_case {
	unsigned long pgoff;
	int fd;
	char *data;
};

size_t PAGE_SIZE, TOTAL_SIZE;

bool verify_test_cases(struct test_case *tcs, int tcs_nr, int base_fd,
		       char *base_map)
{
	char *buffer = malloc(PAGE_SIZE);
	memset(buffer, 0, PAGE_SIZE);
	bool valid = true;

	for (unsigned long pgoff = 0; pgoff < TOTAL_SIZE / PAGE_SIZE; pgoff++) {
		size_t offset = pgoff * PAGE_SIZE;

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
				memcpy(buffer, tc->data, PAGE_SIZE);
				fd = -1;
			}
		}

		if (fd > 0) {
			lseek(fd, offset, SEEK_SET);
			read(fd, buffer, PAGE_SIZE);
		}

		if (memcmp(base_map + offset, buffer, PAGE_SIZE)) {
			printf("== ERROR: base memory does not match the file contents at page %lu\n",
			       pgoff);
			valid = false;
			break;
		}
		memset(buffer, 0, PAGE_SIZE);
	}

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

int mmap_file(char *filename, size_t size, int *fd, char **mmap_addr)
{
	*fd = open(filename, O_RDONLY);
	if (*fd < 0) {
		printf("ERROR: could not open file %s: %s\n", filename,
		       strerror(errno));
		return EXIT_FAILURE;
	}

	*mmap_addr =
		mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_PRIVATE, *fd, 0);
	if (*mmap_addr == MAP_FAILED) {
		printf("ERROR: could not mmap file %s: %s\n", filename,
		       strerror(errno));
		close(*fd);
		return EXIT_FAILURE;
	}
	return EXIT_SUCCESS;
}

int call_kmod(unsigned long cmd, void *req)
{
	int syscall_dev = open(kmod_device_path, O_WRONLY);
	if (syscall_dev < 0) {
		printf("ERROR: could not open %s: %s\n", kmod_device_path,
		       strerror(errno));
		return EXIT_FAILURE;
	}

	int result = ioctl(syscall_dev, cmd, req);
	if (result) {
		printf("ERROR: could not call command %lu: %s\n", cmd,
		       strerror(errno));
		close(syscall_dev);
		return EXIT_FAILURE;
	}

	close(syscall_dev);
	return EXIT_SUCCESS;
}

int test_memory_read()
{
	clear_cache();
	int res = EXIT_SUCCESS;

	// Read base.bin test file and map it into memory.
	int base_fd;
	char *base_mmap;
	if (mmap_file("base.bin", TOTAL_SIZE, &base_fd, &base_mmap)) {
		return EXIT_FAILURE;
	}

	// mmap base.bin test file again in an area that will not be modified.
	char *clean_base_mmap =
		mmap(NULL, TOTAL_SIZE, PROT_READ, MAP_PRIVATE, base_fd, 0);
	if (clean_base_mmap == MAP_FAILED) {
		printf("ERROR: could not map clean base file %s: %s\n",
		       "base.bin", strerror(errno));
		res = EXIT_FAILURE;
		goto unmap_base;
	}
	printf("mapped clean base file %s\n", "base.bin");

	// Read overlay.bin test file and map it into memory.
	int overlay_fd;
	char *overlay_mmap;
	if (mmap_file("overlay.bin", TOTAL_SIZE, &overlay_fd, &overlay_mmap)) {
		res = EXIT_FAILURE;
		goto unmap_clean_base;
	}

	// Create test memory overlay request with several overlay scenarios.
	struct overlay_req req;
	req.base_addr = *(unsigned long *)(&base_mmap);
	req.overlay_addr = *(unsigned long *)(&overlay_mmap);
	req.segments_size = 6;
	req.segments = calloc(sizeof(struct mem_overlay_segment_req),
			      req.segments_size);

	// Overlay single page.
	req.segments[0].start_pgoff = 0;
	req.segments[0].end_pgoff = 0;

	// Overlay multiple pages.
	req.segments[1].start_pgoff = 4;
	req.segments[1].end_pgoff = 6;

	// Two overlays back-to-back.
	req.segments[2].start_pgoff = 20;
	req.segments[2].end_pgoff = 20;
	req.segments[3].start_pgoff = 21;
	req.segments[3].end_pgoff = 21;

	// Large overlay that crosses fault-around and xa_store_range limits.
	req.segments[4].start_pgoff = 30;
	req.segments[4].end_pgoff = 70;

	// Another large overlay to make sure both ranges are cleaned up properly.
	req.segments[5].start_pgoff = 80;
	req.segments[5].end_pgoff = 140;

	if (call_kmod(IOCTL_MEM_OVERLAY_REQ_CMD, &req)) {
		res = EXIT_FAILURE;
		goto free_segments;
	};

	// Create expected read results.
	// Every page in a segment range should have data from the overlay file.
	int tcs_nr = 0;
	for (int i = 0; i < req.segments_size; i++) {
		struct overlay_segment_req seg = req.segments[i];
		tcs_nr += seg.end_pgoff - seg.start_pgoff + 1;
	}

	struct test_case *tcs = calloc(sizeof(struct test_case), tcs_nr);
	int tcs_n = 0;
	for (int i = 0; i < req.segments_size; i++) {
		struct mem_overlay_segment_req seg = req.segments[i];

		for (int pgoff = seg.start_pgoff; pgoff <= seg.end_pgoff;
		     pgoff++) {
			tcs[tcs_n].pgoff = pgoff;
			tcs[tcs_n].fd = overlay_fd;
			tcs_n++;
		}
	}

	// Verify memory overlays properly.
	printf("= TEST: checking memory contents with overlay\n");
	if (!verify_test_cases(tcs, tcs_nr, base_fd, base_mmap)) {
		res = EXIT_FAILURE;
		goto free_tcs;
	}
	printf("== OK: overlay memory verification completed successfully!\n");

	// Verify clean base map was not modified.
	printf("= TEST: checking memory contents without overlay\n");
	if (!verify_test_cases(NULL, 0, base_fd, clean_base_mmap)) {
		res = EXIT_FAILURE;
		goto free_tcs;
	}
	printf("== OK: non-overlay memory verification completed successfully!\n");

free_tcs:
	free(tcs);

	struct mem_overlay_cleanup_req cleanup_req = {
		.id = req.id,
	};
	if (call_kmod(IOCTL_MEM_OVERLAY_CLEANUP_CMD, &cleanup_req))
		res = EXIT_FAILURE;
free_segments:
	free(req.segments);
	munmap(overlay_mmap, TOTAL_SIZE);
unmap_clean_base:
	munmap(clean_base_mmap, TOTAL_SIZE);
unmap_base:
	munmap(base_mmap, TOTAL_SIZE);
	close(base_fd);

	return res;
}

int test_memory_write()
{
	clear_cache();
	int res = EXIT_SUCCESS;

	// Read base.bin test file and map it into memory.
	int base_fd;
	char *base_mmap;
	if (mmap_file("base.bin", TOTAL_SIZE, &base_fd, &base_mmap)) {
		return EXIT_FAILURE;
	}

	// Read overlay.bin test file and map it into memory.
	int overlay_fd;
	char *overlay_mmap;
	if (mmap_file("overlay.bin", TOTAL_SIZE, &overlay_fd, &overlay_mmap)) {
		res = EXIT_FAILURE;
		goto unmap_base;
	}

	// Create test memory overlay request with an overlay that covers multiple
	// pages so we can write in the middle of the area.
	struct mem_overlay_req req;
	req.base_addr = *(unsigned long *)(&base_mmap);
	req.overlay_addr = *(unsigned long *)(&overlay_mmap);
	req.segments_size = 1;
	req.segments = calloc(sizeof(struct mem_overlay_segment_req),
			      req.segments_size);

	req.segments[0].start_pgoff = 4;
	req.segments[0].end_pgoff = 6;

	if (call_kmod(IOCTL_MEM_OVERLAY_REQ_CMD, &req)) {
		res = EXIT_FAILURE;
		goto free_segments;
	};

	// Write random data to different areas of the base memory map.
	int rand_fd = open("/dev/random", O_RDONLY);
	if (rand_fd < 0) {
		printf("ERROR: could not open /dev/random: %d\n", rand_fd);
		res = EXIT_FAILURE;
		goto cleanup_kmod;
	}

	char *rand = calloc(PAGE_SIZE, 1);
	if (read(rand_fd, rand, PAGE_SIZE) < 0) {
		printf("ERROR: failed to read /dev/random: %s\n",
		       strerror(errno));
		res = EXIT_FAILURE;
		goto close_rand;
	}

	// Write to overlay page.
	memcpy(base_mmap + PAGE_SIZE * 5, rand, PAGE_SIZE);
	// Write to non-overlay page.
	memcpy(base_mmap + PAGE_SIZE * 10, rand, PAGE_SIZE);

	// Create expected results.
	// Pages 5 and 10 should have the random data.
	// Pages 4 and 6 should have overlay data.
	int tcs_nr = 4;
	struct test_case *tcs = calloc(sizeof(struct test_case), tcs_nr);
	tcs[0].pgoff = 4;
	tcs[0].fd = overlay_fd;
	tcs[1].pgoff = 5;
	tcs[1].data = rand;
	tcs[2].pgoff = 6;
	tcs[2].fd = overlay_fd;
	tcs[3].pgoff = 10;
	tcs[3].data = rand;

	printf("= TEST: checking memory write\n");
	if (!verify_test_cases(tcs, tcs_nr, base_fd, base_mmap)) {
		res = EXIT_FAILURE;
		goto free_tcs;
	}
	printf("== OK: memory write verification completed successfully!\n");

free_tcs:
	free(tcs);
	free(rand);
close_rand:
	close(rand_fd);
cleanup_kmod:;
	struct mem_overlay_cleanup_req cleanup_req = {
		.id = req.id,
	};
	if (call_kmod(IOCTL_MEM_OVERLAY_CLEANUP_CMD, &cleanup_req))
		res = EXIT_FAILURE;
free_segments:
	free(req.segments);
	munmap(overlay_mmap, TOTAL_SIZE);
unmap_base:
	munmap(base_mmap, TOTAL_SIZE);
	close(base_fd);

	return res;
}

int main()
{
	PAGE_SIZE = sysconf(_SC_PAGESIZE);
	TOTAL_SIZE = PAGE_SIZE * 1024;
	printf("Using pagesize %lu with total size %lu\n", PAGE_SIZE,
	       TOTAL_SIZE);

	if (test_memory_read())
		return EXIT_FAILURE;
	if (test_memory_write())
		return EXIT_FAILURE;

	// TODO: parse /proc/<pid>/smaps to verify memory sharing.

	return EXIT_SUCCESS;
}
