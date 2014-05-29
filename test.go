package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"
	"unsafe"
)

const (
	resetDisk = 0xDEADBEEF

	diskSize       = 76 << 10
	trackSize      = 4096
	pageSize       = 4096
	blockSize      = pageSize
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

func panicf(format string, v ...interface{}) {
	s := fmt.Sprintf(format, v...)
	panic(s)
}

func numUsableSectors() int {
	bandSize := bandSizeTracks * trackSize
	numBands := diskSize / bandSize
	numCacheBands := numBands * cachePercent / 100
	numDataBands := (numBands/numCacheBands - 1) * numCacheBands
	return numDataBands * bandSize / lbaSize
}

// |cmdLine| is a shell command that may include pipes.  Returns the slice of
// |exec.Command| objects all of which have been started with the exception of
// the last one.
func chainCmds(cmdLine string) (cmds []*exec.Cmd) {
	for _, s := range strings.Split(cmdLine, "|") {
		args := strings.Fields(s)
		cmds = append(cmds, exec.Command(args[0], args[1:]...))
	}

	for i, c := range cmds[:len(cmds)-1] {
		stdout, err := c.StdoutPipe()
		if err != nil {
			panicf("Failed to get stdout of %s: %v", c.Path, err)
		}
		err = c.Start()
		if err != nil {
			panicf("Failed to run %s: %v", c.Path, err)
		}
		cmds[i+1].Stdin = stdout
	}
	return
}

// Runs |cmdLine|, which is a shell command that may include pipes.
func runCmd(cmdLine string) {
	cmds := chainCmds(cmdLine)

	c := cmds[len(cmds)-1]

	if _, err := c.Output(); err != nil {
		panicf("Failed to run %s: %v\n", c.Path, err)
	}
}

// Runs |cmdLine|, which is a shell command that may include pipes and returns
// the stdout of the last command.
func startCmd(cmdLine string) io.ReadCloser {
	cmds := chainCmds(cmdLine)

	c := cmds[len(cmds)-1]

	stdout, err := c.StdoutPipe()
	if err != nil {
		panicf("Failed to get stdout of %s: %v\n", c.Path, err)
	}

	if err := c.Start(); err != nil {
		panicf("Failed to start %s: %v\n", c.Path, err)
	}
	return stdout
}

func setup() {
	fmt.Println("Setting up test environment...")

	runCmd("make clean")
	runCmd("make")
	runCmd("sudo insmod " + modulePath)
	runCmd("fallocate -l " + strconv.Itoa(fileSize) + " " + fileName)
	runCmd("sudo losetup " + loopDevice + " " + fileName)

	c := fmt.Sprintf("echo 0 %d %s %s %d %d %d %d | sudo dmsetup create %s",
		numUsableSectors(), targetName, loopDevice, trackSize,
		bandSizeTracks, cachePercent, diskSize, targetName)
	runCmd(c)
}

func tearDown() {
	fmt.Println("Tearing down test environment...")

	runCmd("sudo dmsetup remove " + targetName)
	runCmd("sudo rmmod " + moduleRmName)
	runCmd("sudo losetup -d " + loopDevice)
	runCmd("rm " + fileName)
}

// Allocates aligned blocks for direct I/O.
func alignedBlocks(c byte, count int) []byte {
	b := make([]byte, pageSize+blockSize*count)
	a := int(uintptr(unsafe.Pointer(&b[0])) & (pageSize - 1))

	o := 0
	if a != 0 {
		o = pageSize - a
	}
	b = b[o:o+blockSize*count]

	for i := range b {
		b[i] = c
	}
	return b
}

// Writes |count| number of blocks filled with |c|, starting at |blockNo|.
func writeBlocks(f *os.File, blockNo, count int, c byte) {
	b := alignedBlocks(c, count)

	offset := int64(blockNo * blockSize)
	if _, err := f.WriteAt(b, offset); err != nil {
		panicf("Write failed: %v", err)
	}
}

// Reads |count| number of blocks starting at |blockNo| and verifies that the
// read blocks are filled with |c|.
func readBlocks(f *os.File, blockNo, count int, c byte) {
	b := alignedBlocks(0, count)

	offset := int64(blockNo * blockSize)
	if _, err := f.ReadAt(b, offset); err != nil {
		panicf("Read failed: %v", err)
	}
	if c == '_' {
		return
	}
	for i := range b {
		if b[i] != c {
			panicf("Expected %c, got %c", c, b[i])
		}
	}
}

// Sends IOCTL to SMR emulator target to reset its state.
func doResetDisk() {
	f, err := os.Open(targetDevice)
	if err != nil {
		panicf("os.OpenFile(%s) failed: %v", targetDevice, err)
	}
	defer f.Close()

	_, _, errr := syscall.Syscall(syscall.SYS_IOCTL, f.Fd(), resetDisk, 0)
	if errr != 0 {
		panicf("Resetting %s failed: %v", targetDevice, errr)
	}
}

// A test consists of two strings: |userCmds| is a comma separated list of user
// commands to execute, |btEvents| is a comma separated list of blktrace events
// we expect due to these commands.
type Test struct {
	userCmds string
	btEvents string
}

// Verifies syntax of the tests.
func verify(tests []*Test) {
	var userCmdRegexp = regexp.MustCompile(`^[wr]\s[a-z_]\s\d+\s\d+$`)
	var btEventRegexp = regexp.MustCompile(`^[wr]\s\d+\s\d+$`)

	fmt.Println("Verifying syntax of tests...")
	for i, t := range tests {
		for _, c := range strings.Split(t.userCmds, ",") {
			if !userCmdRegexp.MatchString(c) {
				panicf("Bad user command %d: %s", i, c)
			}
		}
		for _, e := range strings.Split(t.btEvents, ",") {
			if !btEventRegexp.MatchString(e) {
				panicf("Bad blktrace event d: %s", i, e)
			}
		}
	}
}

// Executes a user test command.
func doUserCmd(f *os.File, cmd string) {
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

// Reads blktrace events from |pipe|.
func readBtEvents(pipe io.ReadCloser, ch chan []string) {
	var events []string
	go func() {
		scanner := bufio.NewScanner(pipe)
		for scanner.Scan() {
			s := scanner.Text()
			if !strings.HasPrefix(s, "!") {
				continue
			}
			events = append(events, s)
		}
	}()
	for {
		time.Sleep(1 * time.Second)
		if len(events) != 0 {
			break
		}
	}
	ch <- events
}

func frame(s string) string {
	return fmt.Sprintf("\n-------\n%s\n-------\n", s)
}

// Converts blktrace event to our test format.
func btToTest(btCmd string) string {
	fs := strings.Split(btCmd, ",")
	if len(fs) != 3 {
		return ""
	}

	op := strings.ToLower(fs[0])[1]
	offset, _ := strconv.Atoi(fs[1])
	blocks, _ := strconv.Atoi(fs[2])

	return fmt.Sprintf("%c %d %d", op, offset/8, blocks/8)
}

func eventsMatch(expectedEvents, readEvents []string) bool {
	if len(expectedEvents) != len(readEvents) {
		return false
	}
	for i := range expectedEvents {
		if expectedEvents[i] != btToTest(readEvents[i]) {
			return false
		}
	}
	return true
}

func doTest(i int, t *Test) {
	f, err := os.OpenFile(targetDevice, os.O_RDWR|syscall.O_DIRECT, 0666)
	if err != nil {
		panicf("os.OpenFile(%s) failed: %v", targetDevice, err)
	}
	defer f.Close()

	c := fmt.Sprintf("sudo blktrace -d %s -o - | blkparse -FQ,%s -i -",
		loopDevice, `!%d,%S,%n\n`)
	pipe := startCmd(c)
	defer func() {
		pipe.Close()
		runCmd("sudo pkill -15 blktrace")
	}()

	fmt.Printf("Running test %3d: ", i)

	ch := make(chan []string)
	go readBtEvents(pipe, ch)

	// Although at this point blktrace has already started running, there is
	// a race between blktrace issuing BLKTRACESETUP to the loopback device
	// and us executing the user commands.  Since there is no way of finding
	// out whether blktrace has issued BLKTRACESETUP, we sleep here and hope
	// that it does so before we start executing commands.
	time.Sleep(1 * time.Second)
	for _, cmd := range strings.Split(t.userCmds, ",") {
		doUserCmd(f, cmd)
	}

	readEvents := <-ch
	close(ch)
	expectedEvents := strings.Split(t.btEvents, ",")

	if eventsMatch(expectedEvents, readEvents) {
		fmt.Println("ok")
	} else {
		panicf("Failed: %s\n",
			fmt.Sprintf("Expected %s got %s",
				frame(strings.Join(expectedEvents, "\n")),
				frame(strings.Join(readEvents, "\n"))))
	}
}

func readTests(fileName string) (tests []*Test) {
	f, err := os.Open(fileName)
	if err != nil {
		panicf("os.Open(%s) failed: %s\n", fileName, err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		s := scanner.Text()
		if s == "" || strings.HasPrefix(s, "#") {
			continue
		}
		f := strings.Split(s, ":")
		tests = append(tests, &Test{f[0], f[1]})
	}
	return
}

func main() {
	testFile := flag.String("f", "", "File containig tests.")

	flag.Parse()

	if *testFile == "" {
		fmt.Println("Please specify a test file using -f")
		return
	}

	tests := readTests(*testFile)
	verify(tests)

	setup()
	defer tearDown()

	for i, t := range tests {
		doResetDisk()
		doTest(i, t)

		// Killing and immediately restarting blktrace does not work, so
		// we sleep a little.
		time.Sleep(time.Second)
	}
}
