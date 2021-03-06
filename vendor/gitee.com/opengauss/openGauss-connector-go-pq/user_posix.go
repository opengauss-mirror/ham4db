// Package pq is a pure Go openGauss driver for the database/sql package.

// +build aix darwin dragonfly freebsd linux nacl netbsd openbsd plan9 solaris rumprun

package pq

import (
	"os"
	"os/user"
)

func userCurrent() (string, error) {
	u, err := user.Current()
	if err == nil {
		return u.Username, nil
	}

	name := os.Getenv("USER")
	if name != "" {
		return name, nil
	}

	return "", ErrCouldNotDetectUsername
}
