//go:build !cmfa && !windows

package adapter

import (
	"os"
	"time"

	"golang.org/x/sys/unix"
)

func withConnectivityStatsDiskLock(fn func()) {
	lockPath := desktopStatsPath() + ".lock"
	deadline := time.Now().Add(3 * time.Second)
	for {
		f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o644)
		if err != nil {
			return
		}
		err = unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB)
		if err == nil {
			defer func() {
				_ = unix.Flock(int(f.Fd()), unix.LOCK_UN)
				_ = f.Close()
			}()
			fn()
			return
		}
		_ = f.Close()
		if time.Now().After(deadline) {
			return
		}
		time.Sleep(15 * time.Millisecond)
	}
}
