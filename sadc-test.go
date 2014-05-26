package main

import (
	"regexp"
	"unsafe"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
)

const (
	resetDisk      = 0xDEADBEEF

	diskSize       = 76 << 10
	trackSize      = 4096
	pageSize       = 4096
	bandSizeTracks = 3
	cachePercent   = 40
	pbaSize        = 4096
	lbaSize        = 512

	moduleName   = "sadc"
	loopDevice   = "/dev/loop0"
	fileName     = moduleName + ".img"
	fileSize     = diskSize
	modulePath   = "dm-" + moduleName + ".ko"
	moduleRmName = "dm_" + moduleName
	targetName   = moduleName
	targetDevice = "/dev/mapper/" + targetName
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
			log.Panicf("%s: %v", cmd.s, err)
		}
		err = cmd.c.Start()
		if err != nil {
			log.Panicf("%s: %v", cmd.s, err)
		}
		cmds[i+1].c.Stdin = stdout
	}

	_, err := cmds[len(cmds)-1].c.Output()
	if err != nil {
		log.Panicf("%s: %v", cmds[len(cmds)-1].s, err)
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

func alignedBlock(c byte) []byte {
	b := make([]byte, pageSize * 2)
	a := int(uintptr(unsafe.Pointer(&b[0])) & (pageSize - 1))

	o := 0
	if a != 0 {
		o = pageSize - a
	}
	b = b[o:o + pageSize]

	for i := range(b) {
		b[i] = c
	}
	return b
}

func writeBlocks(f *os.File, offset, count int, c byte) {
	b := alignedBlock(c)

	for i := 0; i < count; i++ {
		_, err := f.WriteAt(b, int64(offset * pageSize))
		if err != nil {
			log.Panicf("Write failed: %v", err)
		}
	}
}

func readBlocks(f *os.File, offset, count int, c byte) {
	b := alignedBlock(0)

	for i := 0; i < count; i++ {
		_, err := f.ReadAt(b, int64(offset * pageSize))
		if err != nil {
			log.Panicf("Read failed: %v", err)
		}
		for i := range(b) {
			if b[i] != c {
				log.Panicf("Expected %c, got %c", c, b[i])
			}
		}
	}
}

func doResetDisk() {
	f, err := os.Open(targetDevice)
	if err != nil {
		log.Panicf("os.OpenFile(%s) failed: %v", targetDevice, err)
	}
	defer f.Close()

	_, _, errr := syscall.Syscall(syscall.SYS_IOCTL, f.Fd(), resetDisk, 0)
	if errr != 0 {
		log.Panicf("Resetting %s failed: %v", targetDevice, errr)
	}
}

type Test struct {
	uspace string
	blktrace string
}

func validateUspaceCmd(cmd string) {
	var r = regexp.MustCompile(`^[wr]\s[a-z]\s\d+\s\d+$`)
	if !r.MatchString(cmd) {
		log.Panicf("Invalid uspace command: %s", cmd)
	}
}

func validateBlktraceCmd(cmd string) {
	var r = regexp.MustCompile(`^[wr]\s\d+\s\d+$`)
	if !r.MatchString(cmd) {
		log.Panicf("Invalid blktrace command: %s", cmd)
	}
}

func doUspaceCmd(f *os.File, cmd string) {
	validateUspaceCmd(cmd)

	fs := strings.Fields(cmd)
	op, c := fs[0], fs[1][0]
	offset, _ := strconv.Atoi(fs[2])
	count, _ := strconv.Atoi(fs[3])

	if op == "w" {
		writeBlocks(f, offset, count, c)
	} else {
		readBlocks(f, offset, count, c)
	}
}

func doBlktraceCmd(f *os.File, cmd string) {
	validateBlktraceCmd(cmd)

	// fs := strings.Fields(cmd)
	// op := fs[0]
	// offset, _ := strconv.Atoi(fs[1])
	// count, _ := strconv.Atoi(fs[2])


}

func doTest(i int, t Test) {
	f, err := os.OpenFile(targetDevice, os.O_RDWR|syscall.O_DIRECT, 0666)
	if err != nil {
		log.Panicf("os.OpenFile(%s) failed: %v", targetDevice, err)
	}
	defer f.Close()

	fmt.Printf("Running test %d: ", i)
	for _, cmd := range(strings.Split(t.uspace, ",")) {
		doUspaceCmd(f, cmd)
	}
	for _, cmd := range(strings.Split(t.blktrace, ",")) {
		doBlktraceCmd(f, cmd)
	}
	fmt.Println("ok")
}

func main() {
	var tests []Test

	tests = append(tests, Test{uspace: "w a 0 1,w b 0 1,w c 0 1,r c 0 1", blktrace: "w 12 1,w 13 1,w 14 1"})
	tests = append(tests, Test{uspace: "w a 0 1,r a 0 1", blktrace: "w 12 1,r 12 1"})

	setup()
	defer tearDown()

	for i, t := range(tests) {
		doResetDisk()
		doTest(i, t)
	}
}
