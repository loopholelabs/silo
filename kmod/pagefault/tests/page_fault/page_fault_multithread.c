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
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <fcntl.h>

#include <sys/ioctl.h>
#include <sys/mman.h>

#include "../../common.h"

const static int nr_threads = 200;
size_t page_size, total_size;
char *base_mmap;

char base_file[] = "base.bin";
char overlay_file[] = "overlay.bin";

void *page_fault(void *idx)
{
	int ret = EXIT_SUCCESS;
	printf("[%03ld] verifying base memory\n", (long)idx);

	char *buffer = malloc(page_size);
	memset(buffer, 0, page_size);

	int base_fd = open(base_file, O_RDONLY);
	if (base_fd < 0) {
		printf("[%03ld] ERROR: could not open base file: %s\n",
		       (long)idx, strerror(errno));
		ret = EXIT_FAILURE;
		goto out;
	}
	printf("[%03ld] opened base file %s\n", (long)idx, base_file);

	int overlay_fd = open(overlay_file, O_RDONLY);
	if (overlay_fd < 0) {
		printf("[%03ld] ERROR: could not open overlay file: %s\n",
		       (long)idx, strerror(errno));
		ret = EXIT_FAILURE;
		goto out;
	}
	printf("[%03ld] opened overlay file %s\n", (long)idx, overlay_file);

	int fd;
	for (unsigned long pgoff = 0; pgoff < total_size / page_size; pgoff++) {
		size_t offset = pgoff * page_size;

		fd = base_fd;
		if (pgoff % 2 == 0) {
			fd = overlay_fd;
		}
		lseek(fd, offset, SEEK_SET);
		read(fd, buffer, page_size);

		if (memcmp(base_mmap + offset, buffer, page_size)) {
			printf("== [%03ld] ERROR: base memory does not match the file contents at page %lu\n",
			       (long)idx, pgoff);
			ret = EXIT_FAILURE;
			goto out;
		}
		memset(buffer, 0, page_size);
	}
	printf("== [%03ld] OK: base memory verification complete\n", (long)idx);

out:
	free(buffer);
	return (void *)(long)ret;
}

int main()
{
	int res = EXIT_SUCCESS;
	page_size = sysconf(_SC_PAGESIZE);
	total_size = page_size * 1024;
	pthread_t tid[nr_threads];

	int base_fd = open(base_file, O_RDONLY);
	if (base_fd < 0) {
		printf("ERROR: could not open base file: %d\n", base_fd);
		return EXIT_FAILURE;
	}
	printf("base file %s opened\n", base_file);

	base_mmap = mmap(NULL, total_size, PROT_READ, MAP_PRIVATE, base_fd, 0);
	if (base_mmap == MAP_FAILED) {
		printf("ERROR: could not mmap base file\n");
		res = EXIT_FAILURE;
		goto close_base;
	}
	printf("base file %s mapped\n", base_file);

	int overlay_fd = open(overlay_file, O_RDONLY);
	if (overlay_fd < 0) {
		printf("ERROR: could not open overlay file: %d\n", overlay_fd);
		res = EXIT_FAILURE;
		goto unmap_base;
	}
	printf("overlay file %s opened\n", overlay_file);

	char *overlay_map =
		mmap(NULL, total_size, PROT_READ, MAP_PRIVATE, overlay_fd, 0);
	if (overlay_map == MAP_FAILED) {
		printf("ERROR: could not mmap overlay file\n");
		res = EXIT_FAILURE;
		goto close_overlay;
	}
	printf("overlay file %s mapped\n", overlay_file);

	struct mem_overlay_req req;
	req.base_addr = *(unsigned long *)(&base_mmap);
	req.overlay_addr = *(unsigned long *)(&overlay_map);
	req.segments_size = total_size / (page_size * 2);
	req.segments = malloc(sizeof(struct mem_overlay_segment_req) *
			      req.segments_size);
	memset(req.segments, 0,
	       sizeof(struct mem_overlay_segment_req) * req.segments_size);

	// Overlay half of the pages.
	for (int i = 0; i < req.segments_size; i++) {
		req.segments[i].start_pgoff = 2 * i;
		req.segments[i].end_pgoff = 2 * i;
	}
	printf("generated memory overlay request\n");

	int syscall_dev = open(kmod_device_path, O_WRONLY);
	if (syscall_dev < 0) {
		printf("ERROR: could not open %s: %d\n", kmod_device_path,
		       syscall_dev);
		res = EXIT_FAILURE;
		goto free_segments;
	}
	printf("opened %s device\n", kmod_device_path);

	int ret = ioctl(syscall_dev, IOCTL_MEM_OVERLAY_REQ_CMD, &req);
	if (ret) {
		printf("ERROR: could not call 'IOCTL_MMAP_CMD': %s\n",
		       strerror(errno));
		res = EXIT_FAILURE;
		goto close_syscall_dev;
	}
	printf("called IOCTL_MEM_OVERLAY_REQ_CMD\n");

	printf("= TEST: generate concurrent page faults\n");
	for (long i = 0; i < nr_threads; i++) {
		pthread_create(&tid[i], NULL, page_fault, (void *)i);
	}

	int success = 0, fail = 0;
	for (int i = 0; i < nr_threads; i++) {
		void *res;
		pthread_join(tid[i], &res);
		switch ((long)res) {
		case EXIT_SUCCESS:
			success += 1;
			break;
		default:
			fail += 1;
			break;
		}
	}
	if (fail != 0) {
		printf("== ERROR: expected all threads to succeed, got %d failures\n",
		       fail);
		res = EXIT_FAILURE;
		goto cleanup;
	}
	printf("== OK: all threads completed successfully! success=%d fail=%d\n",
	       success, fail);

cleanup:;
	struct mem_overlay_cleanup_req cleanup_req = {
		.id = req.id,
	};
	ret = ioctl(syscall_dev, IOCTL_MEM_OVERLAY_CLEANUP_CMD, &cleanup_req);
	if (ret) {
		printf("ERROR: could not call 'IOCTL_MMAP_CMD': %s\n",
		       strerror(errno));
		res = EXIT_FAILURE;
	}
	printf("called IOCTL_MEM_OVERLAY_CLEANUP_CMD\n");

close_syscall_dev:
	close(syscall_dev);
	printf("closed device driver\n");
free_segments:
	free(req.segments);
	printf("freed segments\n");
	munmap(overlay_map, total_size);
	printf("unmapped overlay\n");
close_overlay:
	close(overlay_fd);
	printf("closed overlay\n");
unmap_base:
	munmap(base_mmap, total_size);
	printf("unmapped base\n");
close_base:
	close(base_fd);
	printf("closed base\n");

	printf("done\n");
	return res;
}
