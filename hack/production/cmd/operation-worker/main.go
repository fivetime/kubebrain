package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"time"

	"github.com/kubewharf/kubebrain/hack/production/internal/processgroup"
)

func main() {
	var executable string
	var idleDelay, failureDelay time.Duration
	var once bool
	flag.StringVar(&executable, "executable", "", "absolute operation executor path")
	flag.DurationVar(&idleDelay, "idle-delay", 10*time.Second, "delay after a successful executor run")
	flag.DurationVar(&failureDelay, "failure-delay", 15*time.Second, "delay after a failed executor run")
	flag.BoolVar(&once, "once", false, "run the executor once and return its status")
	flag.Parse()
	if idleDelay < time.Second || failureDelay < time.Second {
		log.Fatal("idle-delay and failure-delay must be at least 1s")
	}
	if err := processgroup.ValidateExecutable(executable); err != nil {
		log.Fatal(err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	for {
		err := run(ctx, executable)
		if once {
			if err != nil {
				log.Printf("executor failed: %v", err)
				os.Exit(1)
			}
			return
		}
		delay := idleDelay
		if err != nil {
			log.Printf("executor failed: %v", err)
			delay = failureDelay
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func run(ctx context.Context, executable string) error {
	command := exec.CommandContext(ctx, executable)
	processgroup.Configure(command)
	command.WaitDelay = processgroup.DefaultWaitDelay
	command.Stdin = os.Stdin
	command.Stdout = os.Stdout
	command.Stderr = os.Stderr
	err := command.Run()
	if ctx.Err() != nil {
		return ctx.Err()
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return errors.New("executor exited with status " + exitErr.ProcessState.String())
	}
	return err
}

func init() {
	log.SetFlags(0)
}
