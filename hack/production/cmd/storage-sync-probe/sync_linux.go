package main

import (
	"os"

	"golang.org/x/sys/unix"
)

func dataSync(f *os.File) error { return unix.Fdatasync(int(f.Fd())) }
