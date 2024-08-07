tests = page_fault \
				page_fault_multithread \
				page_fault_multithread_ioctl \
				page_fault_benchmark \
				page_fault_fork \
				page_fault_ioctl_error

all: $(tests)

ifdef VERIFY
    CFLAGS += -DVERIFY
endif

%.out: page_fault/%.c ../device.h
	gcc ${CFLAGS} $< -I../ -o $@

.PHONY: binaries
binaries: $(addsuffix .out,$(tests))

.PHONY: page_fault
page_fault: page_fault.out
	sudo sysctl -w vm.max_map_count=8388608
	sudo ./page_fault.out

.PHONY: page_fault_multithread
page_fault_multithread: page_fault_multithread.out
	sudo sysctl -w vm.max_map_count=8388608
	sudo ./page_fault_multithread.out

.PHONY: page_fault_multithread_ioctl
page_fault_multithread_ioctl: page_fault_multithread_ioctl.out
	sudo sysctl -w vm.max_map_count=8388608
	sudo ./page_fault_multithread_ioctl.out

.PHONY: page_fault_fork
page_fault_fork: page_fault_fork.out
	sudo sysctl -w vm.max_map_count=8388608
	sudo ./page_fault_fork.out

.PHONY: page_fault_benchmark
page_fault_benchmark: page_fault_benchmark.out
	sudo sysctl -w vm.max_map_count=8388608
	sudo ./page_fault_benchmark.out

.PHONY: page_fault_ioctl_error
page_fault_ioctl_error: page_fault_ioctl_error.out
	sudo sysctl -w vm.max_map_count=8388608
	sudo ./page_fault_ioctl_error.out

.PHONY: generate
generate:
	go run generate.go

.PHONY: clean
clean:
	rm -rf *.out *.bin
