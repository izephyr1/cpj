package main

import (
	"cpj/cp"
	"cpj/stack"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type copyJob struct {
	mu        sync.Mutex
	src, dest *stack.Stack
}

type copyError struct {
	id        int
	src, dest string
	err       error
}

var debug bool

func main() {
	var jobs int
	var link, recurse, useful, cont, verbose bool

	flag.BoolVar(&link, "link", false, "Hard link copied files if able.")
	flag.BoolVar(&recurse, "recurse", false, "Recurse the supplied directory.")
	flag.BoolVar(&useful, "useful", false, "Print some useful statisitcs.")
	flag.BoolVar(&cont, "continue", false, "Continue parallel copy even if individual file errors occur.")
	flag.BoolVar(&verbose, "verbose", false, "Provide verbose messages. Implies -useful.")
	flag.BoolVar(&debug, "debug", false, "Print debug messages. Implies -verbose.")
	flag.IntVar(&jobs, "jobs", 1, "Specify the number of jobs to run in parallel.")
	flag.Parse()

	args := flag.Args()

	if debug {
		verbose = true
	}

	if verbose {
		useful = true
	}

	if len(args) < 2 {
		fmt.Println("Usage: cpj.go [-link] [-recurse] [-useful] [-continue] [-jobs n] src dest")
		flag.PrintDefaults()
		os.Exit(1)
	}

	err := parallelCopy(args[0], args[1], link, recurse, useful, cont, verbose, jobs)
	if err != nil {
		log.Fatal(err)
	}
}

func parallelCopy(src, dest string, hardlink, recurse, useful, cont, verbose bool, jobs int) error {
	var srcFiles, destFiles stack.Stack
	var count int

	// Get the absolute paths to src and dest. If src is a single file, just call cp.CopyFile
	srcAbs, err := cp.AbsolutePath(src)
	if err != nil {
		return err
	}
	info, err := os.Lstat(srcAbs)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return cp.CopyFile(src, dest, hardlink)
	}
	// We know the supplied source is a directory, but did the user intend that?
	if !recurse {
		return errors.New("source is a directory, but you did not provide -recurse")
	}
	// Check to see if dest exists. If it does, check to see if it's a directory.
	// If it's not a directory then abort.
	destAbs, err := cp.AbsolutePath(dest)
	if err != nil {
		return err
	}
	info, err = os.Lstat(destAbs)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return errors.New("source is a directory but destination is not")
	}

	// We need to build a stack containing the source file tree so we can call
	// CopyFile in separate threads
	filepath.Walk(srcAbs, countFiles(&count))
	if debug {
		fmt.Printf("Count: %d\n", count)
	}

	srcFiles = make(stack.Stack, 0, count)
	destFiles = make(stack.Stack, count)

	srcFiles = recurseFileTree(srcAbs, srcFiles)

	// Then we need to create a mirrored file directory in the dest folder
	// First we need to copy the src stack, then subtract the src root directory
	// Then we can append the destination root directory to that tree
	// We also capture the number of copied paths for as a statistic for -useful
	numFiles := copy(destFiles, srcFiles)

	if useful {
		fmt.Printf("Number of files to be copied: %d\n", numFiles)
	}
	if !strings.HasSuffix(srcAbs, "/") {
		srcAbs = strings.Join([]string{srcAbs, "/"}, "")
	}
	if debug {
		fmt.Printf("srcAbs: %s\n", srcAbs)
	}
	for i, file := range destFiles {
		file = strings.TrimPrefix(file, srcAbs)
		destFiles[i] = file
	}
	if !strings.HasSuffix(destAbs, "/") {
		destAbs = strings.Join([]string{destAbs, "/"}, "")
	}
	if debug {
		fmt.Printf("destAbs: %s\n", destAbs)
	}
	for i, file := range destFiles {
		file = strings.Join([]string{destAbs, file}, "")
		destFiles[i] = file
	}
	// Now we have lists of source and destination strings that we can copy in parallel
	// We should build the copyJob object then start up dispatch.
	if debug {
		for n, str := range srcFiles {
			fmt.Printf("%d: src: %s dest: %s\n", n, str, (destFiles)[n])
		}
	}
	jobDispatcher(srcFiles, destFiles, hardlink, cont, verbose, jobs)
	return nil
}

func recurseFileTree(directory string, stk stack.Stack) stack.Stack {
	err := filepath.Walk(directory, visitDirectory(&stk))
	if err != nil {
		panic(err)
	}
	return stk
}

func countFiles(count *int) filepath.WalkFunc {
	return func(path string, info os.FileInfo, err error) error {
		if err != nil {
			log.Fatal(err)
		}
		if info.IsDir() {
			return nil
		}
		(*count)++
		return nil
	}
}

func visitDirectory(files *stack.Stack) filepath.WalkFunc {
	return func(path string, info os.FileInfo, err error) error {
		if err != nil {
			log.Fatal(err)
		}
		if info.IsDir() {
			if debug {
				fmt.Printf("visitDirectory: Found directory: %s\n", path)
			}
			return nil
		}
		if debug {
			fmt.Printf("visitDirectory: Found file: %s\n", path)
		}
		files = stack.Push(files, path)
		if debug {
			fmt.Printf("Stack: %s\n", (*files)[:])
		}
		return nil
	}
}

func copyRoutine(jobs *copyJob, errorChan chan copyError, cont, link, verbose bool, id int) {
	// Process jobs until none remain or an error occurs.
	// If cont = true then continue even if errors are encountered.
	var src, dest string

	if debug {
		fmt.Printf("Started thread %d\n", id)
		fmt.Printf("Current stack: %s \ndest: %s\n", (*(*jobs).src)[:], (*(*jobs).dest)[:])
	}

	for {
		if debug {
			fmt.Printf("Thread %d locking jobs.\n", id)
		}
		(*jobs).mu.Lock()
		src, (*jobs).src = stack.Pop((*jobs).src)
		dest, (*jobs).dest = stack.Pop((*jobs).dest)
		if (*jobs).src == nil {
			if debug {
				fmt.Printf("Thread %d out of jobs.\n", id)
			}
			errorChan <- copyError{id: id, err: nil, src: "", dest: ""}
			(*jobs).mu.Unlock()
			return
		}
		jobs.mu.Unlock()
		if debug {
			fmt.Printf("Thread %d unlocked jobs.\n", id)
		}
		if verbose {
			fmt.Printf("Copying %s to %s.\n", src, dest)
		}
		err := cp.CopyFile(src, dest, link)
		if err != nil {
			errorChan <- copyError{id: id, err: err, src: src, dest: dest}
			if !cont {
				return
			}
		}
	}

}

func jobDispatcher(src, dest stack.Stack, link, cont, verbose bool, jobs int) []error {
	// The dispatcher builds the copyJob locked struct
	// Then it spools up the desired number of jobs
	// It passes the struct to the jobs and waits for errors or completion
	copyLock := copyJob{src: &src, dest: &dest}
	size := len(src)
	var ret []error
	if jobs > size {
		jobs = size
	}
	if debug {
		fmt.Printf("Number of jobs: %d\n", jobs)
	}
	var errChannel chan copyError
	if cont {
		errChannel = make(chan copyError, jobs*2)
	} else {
		errChannel = make(chan copyError, jobs)
	}
	for i := 0; i < jobs; i++ {
		if debug {
			fmt.Printf("Starting thread %d\n", i)
		}
		go copyRoutine(&copyLock, errChannel, cont, link, verbose, i)
	}
	total := jobs
	for err := range errChannel {
		if err.err != nil {
			if verbose {
				fmt.Printf("Error in thread %d: %s, src: %s dest: %s\n", err.id, err.err, err.src, err.dest)
				if cont {
					fmt.Printf("Thread %d is continuing...\n", err.id)
				}
			}
			ret = append(ret, err.err)
		} else {
			total -= 1
			if verbose {
				fmt.Printf("Thread %d finished. %d threads remain.\n", err.id, total)
			}
			if total == 0 {
				return ret
			}
		}
	}
	return ret
}
