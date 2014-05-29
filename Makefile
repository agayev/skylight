module := sadc
obj-m := dm-$(module).o
KDIR := /lib/modules/$(shell uname -r)/build
PWD := $(shell pwd)

all:
	$(MAKE) -C $(KDIR) M=$(PWD) modules

test:
	go run test.go -f $(module).test

clean:
	$(MAKE) -C $(KDIR) M=$(PWD) clean
