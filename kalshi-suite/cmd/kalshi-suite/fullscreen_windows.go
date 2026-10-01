//go:build windows

package main

// R67c: native fullscreen fallback for the ⛶ dashboard button. WebView2 hosts can reject the
// element-fullscreen API (requestFullscreen resolves only with host support + user gesture), so
// GET /api/fullscreen asks THIS process to toggle the app window itself: find the kalshi-app
// window by its title ("Prediction Market Suite"), strip the caption/frame styles and size it to
// the monitor (borderless maximize), or restore the saved style+rect on the second press.

import (
	"errors"
	"sync"
	"syscall"
	"unsafe"
)

var (
	user32               = syscall.NewLazyDLL("user32.dll")
	pFindWindowW         = user32.NewProc("FindWindowW")
	pGetWindowLongW      = user32.NewProc("GetWindowLongW")
	pSetWindowLongW      = user32.NewProc("SetWindowLongW")
	pGetWindowRect       = user32.NewProc("GetWindowRect")
	pSetWindowPos        = user32.NewProc("SetWindowPos")
	pShowWindow          = user32.NewProc("ShowWindow")
	pMonitorFromWindow   = user32.NewProc("MonitorFromWindow")
	pGetMonitorInfoW     = user32.NewProc("GetMonitorInfoW")
	pSetForegroundWindow = user32.NewProc("SetForegroundWindow")
)

const (
	gwlStyle            = ^uintptr(15) // -16 as uintptr (GWL_STYLE)
	wsOverlappedWindow  = 0x00CF0000
	swRestore           = 9
	swpNoZOrder         = 0x0004
	swpFrameChanged     = 0x0020
	swpNoOwnerZOrder    = 0x0200
	monitorDefaultToNearest = 2
)

type winRect struct{ left, top, right, bottom int32 }

type monitorInfo struct {
	cbSize    uint32
	rcMonitor winRect
	rcWork    winRect
	dwFlags   uint32
}

var (
	fsMu       sync.Mutex
	fsOn       bool
	fsPrevStyle uintptr
	fsPrevRect winRect
)

// appWindowFullscreen toggles the "Prediction Market Suite" window between a borderless
// monitor-filling state and its saved windowed state. Returns (nowFullscreen, error).
func appWindowFullscreen() (bool, error) {
	fsMu.Lock()
	defer fsMu.Unlock()

	title, _ := syscall.UTF16PtrFromString("Prediction Market Suite")
	hwnd, _, _ := pFindWindowW.Call(0, uintptr(unsafe.Pointer(title)))
	if hwnd == 0 {
		return false, errors.New("app window not found (title 'Prediction Market Suite')")
	}

	if !fsOn {
		style, _, _ := pGetWindowLongW.Call(hwnd, gwlStyle)
		var r winRect
		if ret, _, _ := pGetWindowRect.Call(hwnd, uintptr(unsafe.Pointer(&r))); ret == 0 {
			return false, errors.New("GetWindowRect failed")
		}
		hmon, _, _ := pMonitorFromWindow.Call(hwnd, monitorDefaultToNearest)
		mi := monitorInfo{cbSize: uint32(unsafe.Sizeof(monitorInfo{}))}
		if ret, _, _ := pGetMonitorInfoW.Call(hmon, uintptr(unsafe.Pointer(&mi))); ret == 0 {
			return false, errors.New("GetMonitorInfoW failed")
		}
		fsPrevStyle, fsPrevRect = style, r
		pSetWindowLongW.Call(hwnd, gwlStyle, style&^uintptr(wsOverlappedWindow))
		m := mi.rcMonitor
		pSetWindowPos.Call(hwnd, 0,
			uintptr(uint32(m.left)), uintptr(uint32(m.top)),
			uintptr(uint32(m.right-m.left)), uintptr(uint32(m.bottom-m.top)),
			swpNoZOrder|swpNoOwnerZOrder|swpFrameChanged)
		pSetForegroundWindow.Call(hwnd)
		fsOn = true
		return true, nil
	}

	pSetWindowLongW.Call(hwnd, gwlStyle, fsPrevStyle)
	r := fsPrevRect
	pSetWindowPos.Call(hwnd, 0,
		uintptr(uint32(r.left)), uintptr(uint32(r.top)),
		uintptr(uint32(r.right-r.left)), uintptr(uint32(r.bottom-r.top)),
		swpNoZOrder|swpNoOwnerZOrder|swpFrameChanged)
	pShowWindow.Call(hwnd, swRestore)
	fsOn = false
	return false, nil
}
