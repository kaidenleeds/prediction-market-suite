//go:build windows

package main

import (
	"log/slog"
	"unsafe"

	"golang.org/x/sys/windows"
)

// killOnCloseJob wraps a Windows Job Object configured with JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE.
// We assign the ML sidecar and the pcrypto mini-server to it. The suite holds the only handle to
// the job; when the suite process exits FOR ANY REASON (Ctrl+C, closing the window with X, Task
// Manager "End task", or a crash) the OS closes that handle, the job closes, and every assigned
// child is killed too. This is what guarantees "close the suite → the mini-server closes" even
// when the graceful Process.Kill path in main doesn't get to run.
type killOnCloseJob struct {
	h windows.Handle
}

// newKillOnCloseJob creates the job. Best-effort: returns nil on any failure (the suite still runs;
// graceful Ctrl+C shutdown falls back to the explicit Process.Kill calls in main).
func newKillOnCloseJob(log *slog.Logger) *killOnCloseJob {
	h, err := windows.CreateJobObject(nil, nil)
	if err != nil {
		log.Info("kill-on-close job unavailable — children rely on graceful-exit kill only", "err", err)
		return nil
	}
	info := windows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION{
		BasicLimitInformation: windows.JOBOBJECT_BASIC_LIMIT_INFORMATION{
			LimitFlags: windows.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
		},
	}
	if _, err := windows.SetInformationJobObject(
		h,
		windows.JobObjectExtendedLimitInformation,
		uintptr(unsafe.Pointer(&info)),
		uint32(unsafe.Sizeof(info)),
	); err != nil {
		_ = windows.CloseHandle(h)
		log.Info("kill-on-close job limit not set — falling back to graceful-exit kill", "err", err)
		return nil
	}
	log.Info("kill-on-close job created — ML sidecar + mini-server will die with the suite no matter how it's closed")
	return &killOnCloseJob{h: h}
}

// assign adds an already-started child (by PID) to the job. Best-effort; logs and continues on error.
func (j *killOnCloseJob) assign(pid int, name string, log *slog.Logger) {
	if j == nil || pid <= 0 {
		return
	}
	ph, err := windows.OpenProcess(windows.PROCESS_SET_QUOTA|windows.PROCESS_TERMINATE, false, uint32(pid))
	if err != nil {
		log.Info("could not open child to assign to kill-on-close job", "child", name, "pid", pid, "err", err)
		return
	}
	defer windows.CloseHandle(ph)
	if err := windows.AssignProcessToJobObject(j.h, ph); err != nil {
		log.Info("could not assign child to kill-on-close job", "child", name, "pid", pid, "err", err)
		return
	}
	log.Info("child assigned to kill-on-close job", "child", name, "pid", pid)
}
