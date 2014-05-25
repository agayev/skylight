module := sadc
obj-m := dm-$(module).o
KDIR := /lib/modules/$(shell uname -r)/build
PWD := $(shell pwd)

all:
	$(MAKE) -C $(KDIR) M=$(PWD) modules

test:
	go run $(module)-test.go

clean:
	$(MAKE) -C $(KDIR) M=$(PWD) clean
