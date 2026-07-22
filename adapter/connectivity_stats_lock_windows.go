//go:build !cmfa && windows

package adapter

import (
	"os"
	"time"

	"golang.org/x/sys/windows"
)

func withConnectivityStatsDiskLock(fn func()) {
	lockPath := desktopStatsPath() + ".lock"
	deadline := time.Now().Add(3 * time.Second)
	for {
		f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o644)
		if err != nil {
			fn()
			return
		}
		var ol windows.Overlapped
		err = windows.LockFileEx(
			windows.Handle(f.Fd()),
			windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY,
			0,
			1,
			0,
			&ol,
		)
		if err == nil {
			defer func() {
				var uol windows.Overlapped
				_ = windows.UnlockFileEx(windows.Handle(f.Fd()), 0, 1, 0, &uol)
				_ = f.Close()
			}()
			fn()
			return
		}
		_ = f.Close()
		if time.Now().After(deadline) {
			fn()
			return
		}
		time.Sleep(15 * time.Millisecond)
	}
}
