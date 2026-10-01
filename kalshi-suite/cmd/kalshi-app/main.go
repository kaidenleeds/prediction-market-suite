// kalshi-app.exe — R51/R53 (operator: "a massive ui overhaul that makes it an app/exe … a standalone
// program … the app, not the chrome tab, needs to open. make it open by default. and make it fit to
// my monitor correctly"): the suite as a native Windows desktop app, and the DEFAULT viewport.
//
// What this is: a WebView2 shell (pure Go, no cgo — github.com/jchv/go-webview2) around the existing
// dashboard server. Same trading engine, same data, but its own app window instead of a browser tab.
// Rendering runs on Chromium's GPU compositor via WebView2 — for the record, CUDA is the ML sidecar's
// business (R52), not the window's; the perceived speed came from the snap-bust push (snap.go R51).
//
// Fit-to-monitor (R53): the process declares Per-Monitor-V2 DPI awareness BEFORE any window exists
// (on a scaled laptop panel an unaware window is bitmap-stretched — blurry and the wrong size), then
// the window is maximized into the monitor's WORK AREA (taskbar-aware) rather than guessing pixels.
//
// Launch matrix (R53, loop-proof):
//   - build-suite.bat starts the suite → the suite starts THIS APP with -attach (wait for /health,
//     never spawn a second suite — that's a port fight, not a feature).
//   - You double-click the app with the suite already running → it just attaches.
//   - You double-click the app cold → it launches kalshi-suite.exe with KALSHI_APP_SPAWNED=1 so the
//     suite does NOT open another window (output → data\app_launch.log), then attaches.
//   - CLOSING THE WINDOW NEVER STOPS TRADING. The engine keeps running headless; the window is only
//     a viewport. Stopping stays where it was: the suite console (Ctrl+C) or the kill switch.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"time"
	"unsafe"

	webview2 "github.com/jchv/go-webview2"
)

func main() {
	setDPIAware() // MUST precede window creation — after that the scaling die is cast

	attach := flag.Bool("attach", false, "suite is already starting/running: only wait for it, never launch one")
	flag.Parse()

	exePath, _ := os.Executable()
	dir := filepath.Dir(exePath)

	addr := "127.0.0.1:8787" // matches config.json server_addr default
	if b, err := os.ReadFile(filepath.Join(dir, "config.json")); err == nil {
		var c struct {
			ServerAddr string `json:"server_addr"`
		}
		if json.Unmarshal(b, &c) == nil && c.ServerAddr != "" {
			addr = c.ServerAddr
		}
	}
	url := "http://" + addr

	if !reachable(url) {
		if !*attach { // cold double-click → we own booting the engine
			if err := launchSuite(dir); err != nil {
				fatalBox(fmt.Sprintf("The suite isn't running and I couldn't start kalshi-suite.exe:\n%v\n\nRun build-suite.bat once, then reopen this app.", err))
			}
		}
		deadline := time.Now().Add(90 * time.Second)
		for !reachable(url) {
			if time.Now().After(deadline) {
				fatalBox("The dashboard never answered on " + url + " within 90s.\nCheck the suite console / data\\app_launch.log for the error.")
			}
			time.Sleep(500 * time.Millisecond)
		}
	}

	w := webview2.NewWithOptions(webview2.WebViewOptions{
		Debug:     false,
		AutoFocus: true,
		WindowOptions: webview2.WindowOptions{
			Title:  "Prediction Market Suite",
			Width:  1280, // fallback size for the instant before maximize
			Height: 800,
			Center: true,
		},
	})
	if w == nil {
		fatalBox("WebView2 runtime not found. It ships with Windows 11 / Edge; install it once from:\nhttps://developer.microsoft.com/microsoft-edge/webview2/")
	}
	defer w.Destroy()
	maximize(w.Window()) // R53: fill THIS monitor's work area (taskbar-aware), whatever its size/DPI
	w.Navigate(url)
	w.Run() // window closed → app exits; the trading engine keeps running headless (by design)
}

// ---- Win32 (pure syscall, no cgo) ----

var (
	user32                        = syscall.NewLazyDLL("user32.dll")
	procSetProcessDpiAwarenessCtx = user32.NewProc("SetProcessDpiAwarenessContext")
	procShowWindow                = user32.NewProc("ShowWindow")
)

// setDPIAware declares Per-Monitor-V2 DPI awareness (context handle -4). Without it, Windows lies
// about the screen size and bitmap-scales the window on any display with >100% scaling — the exact
// "doesn't fit my monitor" complaint. Failure is fine (older Windows): we just inherit the default.
func setDPIAware() {
	const dpiAwarenessContextPerMonitorAwareV2 = ^uintptr(3) // (DPI_AWARENESS_CONTEXT)-4
	_, _, _ = procSetProcessDpiAwarenessCtx.Call(dpiAwarenessContextPerMonitorAwareV2)
}

// maximize fills the current monitor's work area — the OS-correct "fit my screen" for any monitor,
// resolution, or taskbar position. SW_MAXIMIZE = 3.
func maximize(hwnd unsafe.Pointer) {
	if hwnd == nil {
		return
	}
	_, _, _ = procShowWindow.Call(uintptr(hwnd), 3)
}

// reachable = the dashboard answers /health quickly (any HTTP response counts — even 403 means alive).
func reachable(url string) bool {
	c := &http.Client{Timeout: 900 * time.Millisecond}
	resp, err := c.Get(url + "/health")
	if err != nil {
		return false
	}
	_ = resp.Body.Close()
	return true
}

// launchSuite starts kalshi-suite.exe detached with KALSHI_APP_SPAWNED=1 (so the suite won't open a
// second window — we ARE the window), console output → data\app_launch.log so a crash during hidden
// startup is never invisible.
func launchSuite(dir string) error {
	exe := filepath.Join(dir, "kalshi-suite.exe")
	if _, err := os.Stat(exe); err != nil {
		return fmt.Errorf("kalshi-suite.exe not found next to the app (%s)", dir)
	}
	_ = os.MkdirAll(filepath.Join(dir, "data"), 0o755)
	logf, err := os.OpenFile(filepath.Join(dir, "data", "app_launch.log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	fmt.Fprintf(logf, "\n===== kalshi-app launching suite %s =====\n", time.Now().Format(time.RFC3339))
	cmd := exec.Command(exe, "serve")
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "KALSHI_APP_SPAWNED=1")
	cmd.Stdout, cmd.Stderr = logf, logf
	return cmd.Start() // detached enough: on Windows the child survives this process exiting
}

// fatalBox: no console to print to (windowsgui build), so errors go to a log + a best-effort msgbox.
func fatalBox(msg string) {
	_ = os.WriteFile(filepath.Join(filepath.Dir(os.Args[0]), "kalshi-app-error.txt"), []byte(msg+"\n"), 0o644)
	_ = exec.Command("mshta", "javascript:alert('"+jsEscape(msg)+"');close();").Run()
	os.Exit(1)
}

func jsEscape(s string) string {
	out := make([]rune, 0, len(s))
	for _, r := range s {
		switch r {
		case '\'', '\\':
			out = append(out, '\\', r)
		case '\n':
			out = append(out, '\\', 'n')
		default:
			out = append(out, r)
		}
	}
	return string(out)
}
