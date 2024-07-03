// SPDX-License-Identifier: GPL-3.0

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <fcntl.h>
#include <syscall.h>

#include <sys/ioctl.h>
#include <sys/mman.h>

#include "../../device.h"

const static int nr_threads = 2000;
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

void *ioctl_overlay(void *args)
{
	pid_t tid = syscall(SYS_gettid);
	int res = EXIT_SUCCESS;
	struct overlay_req *req = args;

	int syscall_dev = open(device_path, O_WRONLY);
	if (syscall_dev < 0) {
		printf("[%d] ERROR: could not open %s: %d\n", tid,
		       device_path, syscall_dev);
		res = EXIT_FAILURE;
		goto out;
	}

	int ret = ioctl(syscall_dev, IOCTL_OVERLAY_REQ_CMD, req);
	if (ret) {
		printf("[%d] ERROR: could not call 'IOCTL_MMAP_CMD': %s\n", tid,
		       strerror(errno));
		res = EXIT_FAILURE;
	}
	close(syscall_dev);
out:
	return (void *)(long)res;
}

void *ioctl_cleanup(void *args)
{
	pid_t tid = syscall(SYS_gettid);
	int res = EXIT_SUCCESS;
	struct overlay_cleanup_req *req = args;

	int syscall_dev = open(device_path, O_WRONLY);
	if (syscall_dev < 0) {
		printf("[%d] ERROR: could not open %s: %d\n", tid,
		       device_path, syscall_dev);
		res = EXIT_FAILURE;
		goto out;
	}

	int ret = ioctl(syscall_dev, IOCTL_OVERLAY_CLEANUP_CMD, req);
	if (ret) {
		printf("[%d] ERROR: could not call 'IOCTL_MMAP_CMD': %s\n", tid,
		       strerror(errno));
		res = EXIT_FAILURE;
	}
	close(syscall_dev);
out:
	return (void *)(long)res;
}

int main()
{
	int res = EXIT_SUCCESS;
	page_size = sysconf(_SC_PAGESIZE);
	total_size = page_size * 1024;
	pthread_t tid[nr_threads];

	int base_fd = open(base_file, O_RDONLY);
	if (base_fd < 0) {
		printf("ERROR: could not open base file %s: %s\n", base_file,
		       strerror(errno));
		return EXIT_FAILURE;
	}
	printf("base file %s opened\n", base_file);

	base_mmap = mmap(NULL, total_size, PROT_READ, MAP_PRIVATE, base_fd, 0);
	if (base_mmap == MAP_FAILED) {
		printf("ERROR: could not mmap base file %s: %s\n", base_file,
		       strerror(errno));
		res = EXIT_FAILURE;
		goto close_base;
	}
	printf("base file %s mapped\n", base_file);

	int overlay_fd = open(overlay_file, O_RDONLY);
	if (overlay_fd < 0) {
		printf("ERROR: could not open overlay file %s: %s\n",
		       overlay_file, strerror(errno));
		res = EXIT_FAILURE;
		goto unmap_base;
	}
	printf("overlay file %s opened\n", overlay_file);

	char *overlay_map =
		mmap(NULL, total_size, PROT_READ, MAP_PRIVATE, overlay_fd, 0);
	if (overlay_map == MAP_FAILED) {
		printf("ERROR: could not mmap overlay file %s: %s\n",
		       overlay_file, strerror(errno));
		res = EXIT_FAILURE;
		goto close_overlay;
	}
	printf("overlay file %s mapped\n", overlay_file);

	struct overlay_req req;
	req.base_addr = *(unsigned long *)(&base_mmap);
	req.overlay_addr = *(unsigned long *)(&overlay_map);
	req.segments_size = total_size / (page_size * 2);
	req.segments = malloc(sizeof(struct overlay_segment_req) *
			      req.segments_size);
	memset(req.segments, 0,
	       sizeof(struct overlay_segment_req) * req.segments_size);

	// Overlay half of the pages.
	for (int i = 0; i < req.segments_size; i++) {
		req.segments[i].start_pgoff = 2 * i;
		req.segments[i].end_pgoff = 2 * i;
	}
	printf("generated memory overlay request\n");

	// Call memory overlay ioctl multiple times.
	printf("= TEST: call IOCTL_OVERLAY_REQ_CMD multiple times\n");
	for (long i = 0; i < nr_threads; i++) {
		pthread_create(&tid[i], NULL, ioctl_overlay, (void *)&req);
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
	if (success != 1) {
		printf("== ERROR: expected one thread to succeed in calling IOCTL_OVERLAY_REQ_CMD, got %d\n",
		       success);
		res = EXIT_FAILURE;
		goto free_segments;
	}
	printf("== OK: calls to IOCTL_OVERLAY_REQ_CMD completed successfully! success=%d fail=%d\n",
	       success, fail);

	// Call cleanup ioctl multiple times.
	printf("= TEST: call IOCTL_OVERLAY_CLEANUP_CMD multiple times\n");
	struct overlay_cleanup_req cleanup_req = {
		.id = req.id,
	};
	for (long i = 0; i < nr_threads; i++) {
		pthread_create(&tid[i], NULL, ioctl_cleanup,
			       (void *)&cleanup_req);
	}

	success = 0, fail = 0;
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
	if (success != 1) {
		printf("ERROR: expected one thread to succeed in calling IOCTL_OVERLAY_CLEANUP_CMD success=%d fail=%d\n",
		       success, fail);
		res = EXIT_FAILURE;
		goto free_segments;
	}
	printf("== OK: calls to IOCTL_OVERLAY_CLEANUP_CMD completed successfully! success=%d fail=%d\n",
	       success, fail);
	res = EXIT_SUCCESS;
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
