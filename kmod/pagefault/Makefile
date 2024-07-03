obj-m := silo-pagefault.o
silo-pagefault-objs := module.o log.o hashtable.o

LOG_LEVEL ?= 1
ccflags-y += -DLOG_LEVEL=${LOG_LEVEL}

clean-files := *.o *.mod.c *.mod.o *.ko *.symvers *.o.d

all: module

.PHONY: module
module:
	${MAKE} -C /lib/modules/$(shell uname -r)/build M=$(PWD) modules

.PHONY: clean
clean: tests-clean
	${MAKE} -C /lib/modules/$(shell uname -r)/build M=$(PWD) clean
	$(RM) $(clean-files)

.PHONY: load
load:
	insmod silo-pagefault.ko

.PHONY: unload
unload:
	rmmod silo_pagefault

.PHONY: tests
tests:
	cd tests && ((${MAKE} all && exit 0) || exit -1)

.PHONY: tests-benchmark
tests-benchmark:
	cd tests && ((${MAKE} page_fault_benchmark && exit 0) || exit -1)

.PHONY: tests-userspace
tests-userspace:
	cd tests && ((${MAKE} userspace && exit 0) || exit -1)

.PHONY: tests-generate
tests-generate:
	cd tests && ((${MAKE} generate && exit 0) || exit -1)

.PHONY: tests-clean
tests-clean:
	cd tests && ((${MAKE} clean && exit 0) || exit -1)

.PHONY: tests-binaries
tests-binaries:
	cd tests && ((${MAKE} binaries && exit 0) || exit -1)
