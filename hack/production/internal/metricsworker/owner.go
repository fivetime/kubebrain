package metricsworker

import (
	"errors"
	"os"
	"path/filepath"
)

// ownerGuard binds the already-claimed experiment's directory identities.
// It is an observation guard, not an atomic lock against a recovery controller.
type ownerGuard struct {
	path             string
	directory, claim os.FileInfo
}

func bindOwner(path string) (*ownerGuard, error) {
	g := &ownerGuard{path: path}
	var err error
	g.directory, err = os.Lstat(path)
	if err != nil || !g.directory.IsDir() {
		return nil, errors.New("invalid experiment owner directory")
	}
	g.claim, err = os.Lstat(filepath.Join(path, "deployment-claimed"))
	if err != nil || !g.claim.IsDir() {
		return nil, errors.New("experiment owner is not claimed")
	}
	return g, g.check()
}

func (g *ownerGuard) check() error {
	bad := errors.New("experiment owner inactive or replaced")
	for name, original := range map[string]os.FileInfo{"": g.directory, "deployment-claimed": g.claim} {
		current, err := os.Lstat(filepath.Join(g.path, name))
		if err != nil || !current.IsDir() || !os.SameFile(original, current) {
			return bad
		}
	}
	for _, name := range []string{"HOLD", "final-exit-code"} {
		_, err := os.Lstat(filepath.Join(g.path, name))
		// A dangling symlink is a terminal marker too; lookup errors fail closed.
		if !errors.Is(err, os.ErrNotExist) {
			return bad
		}
	}
	return nil
}
