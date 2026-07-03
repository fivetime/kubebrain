//go:build race
// +build race

package backend

// raceDetectorEnabled is true when the binary is built with -race. Used to skip
// tests that only fail because they trip a data race inside a vendored
// dependency we do not own (client-go 2019 leaderelection, cmux 0.1.5), not in
// KubeBrain code. Those tests pass without -race.
const raceDetectorEnabled = true
