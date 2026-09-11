//go:build !linux

package main

import (
	"errors"
	"os"
)

func dataSync(*os.File) error { return errors.New("fdatasync probe requires Linux") }
