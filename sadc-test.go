package main

import (
	"fmt"
	"log"
	"os/exec"
	"strconv"
	"strings"
)

const (
	diskSize       = 76 << 10
	trackSize      = 4096
	bandSizeTracks = 3
	cachePercent   = 40
	pbaSize        = 4096
	lbaSize        = 512

	moduleName   = "sadc"
	rootDir      = "/home/aghayev/shingle/"
	loopDevice   = "/dev/loop0"
	fileName     = rootDir + moduleName + ".img"
	fileSize     = diskSize
	modulePath   = rootDir + "dm-" + moduleName + ".ko"
	moduleRmName = "dm_" + moduleName
	targetName   = moduleName
)

func numUsableSectors() int {
	bandSize := bandSizeTracks * trackSize
	numBands := diskSize / bandSize
	numCacheBands := numBands * cachePercent / 100
	numDataBands := (numBands/numCacheBands - 1) * numCacheBands
	return numDataBands * bandSize / lbaSize
}

type Cmd struct {
	c *exec.Cmd
	s string
}

func doCmd(cmdLine string) {
	var cmds []Cmd

	for _, s := range strings.Split(cmdLine, "|") {
		args := strings.Fields(s)
		cmd := exec.Command(args[0], args[1:]...)
		cmds = append(cmds, Cmd{c: cmd, s: s})
	}

	for i, cmd := range cmds[:len(cmds)-1] {
		stdout, err := cmd.c.StdoutPipe()
		if err != nil {
			log.Fatalf("%s: %v", cmd.s, err)
		}
		err = cmd.c.Start()
		if err != nil {
			log.Fatalf("%s: %v", cmd.s, err)
		}
		cmds[i+1].c.Stdin = stdout
	}

	_, err := cmds[len(cmds)-1].c.Output()
	if err != nil {
		log.Fatalf("%s: %v", cmds[len(cmds)-1].s, err)
	}
}

func setup() {
	fmt.Println("Setting up test environment...")

	doCmd("make clean")
	doCmd("make")
	doCmd("sudo insmod " + modulePath)
	doCmd("fallocate -l " + strconv.Itoa(fileSize) + " " + fileName)
	doCmd("sudo losetup " + loopDevice + " " + fileName)

	c := fmt.Sprintf("echo 0 %d %s %s %d %d %d %d | sudo dmsetup create %s",
		numUsableSectors(), targetName, loopDevice, trackSize,
		bandSizeTracks, cachePercent, diskSize, targetName)
	doCmd(c)
}

func tearDown() {
	fmt.Println("Tearing down test environment...")

	doCmd("sudo dmsetup remove " + targetName)
	doCmd("sudo rmmod " + moduleRmName)
	doCmd("sudo losetup -d " + loopDevice)
	doCmd("rm " + fileName)
}

func main() {
	setup()
	tearDown()
}
