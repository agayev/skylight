package main

import (
	"fmt"
	"os/exec"
	"strings"
	"strconv"
)

const (
	moduleName = "sadc"
	rootDir = "/home/aghayev/shingle/"
	loopDevice = "/dev/loop0"
	fileName = rootDir + moduleName + ".img"
	fileSize = 10 * 1024 * 1024
	modulePath = rootDir + "dm-" + moduleName + ".ko"
	moduleRmName = "dm_" + moduleName
	targetName = moduleName
)

func doCmd(args string) {
	s := strings.Split(args, " ")
	out, err := exec.Command(s[0], s[1:]...).Output()
	if err != nil {
		panic(fmt.Sprintf("\n\n%s: %s", args, out))
	}
}

func setup() {
	fmt.Printf("%-50s", "Setting up test environment:")

	doCmd("make clean")
	doCmd("make")
	doCmd("sudo insmod " + modulePath)
	doCmd("fallocate -l " + strconv.Itoa(fileSize) + " " + fileName)
	doCmd("sudo losetup " + loopDevice + " " + fileName)

	fmt.Println("OK")
}

func tearDown() {
	fmt.Printf("%-50s", "Tearing down test environment:")

	//doCmd("sudo dmsetup remove " + targetName)
	doCmd("sudo rmmod " +  moduleRmName)
	doCmd("sudo losetup -d " + loopDevice)
	doCmd("rm " + fileName)

	fmt.Println("OK")
}

func main() {
	setup()
	tearDown()
}
