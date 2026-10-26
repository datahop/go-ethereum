package main

import (
	"os"
	"os/exec"
	"runtime"
	"runtime/debug"
	"strings"
	"time"
)

// platform describes the build and host. Runs without this record are not
// admissible as performance evidence.
type platform struct {
	Revision         string `json:"revision"`
	Dirty            bool   `json:"dirty"`
	RevisionSource   string `json:"revision_source"`
	GoVersion        string `json:"go_version"`
	GOOS             string `json:"goos"`
	GOARCH           string `json:"goarch"`
	CPU              string `json:"cpu_model"`
	PerfCores        int    `json:"performance_cores,omitempty"`
	EfficiencyCores  int    `json:"efficiency_cores,omitempty"`
	Translated       bool   `json:"rosetta_translated"`
	TranslatedSource string `json:"rosetta_source"`
	GOMAXPROCS       int    `json:"gomaxprocs"`
	TimerResolution  int64  `json:"timer_resolution_ns"`
	CoreType         string `json:"core_type"`
}

func sysctl(name string) (string, bool) {
	out, err := exec.Command("sysctl", "-n", name).Output()
	if err != nil {
		return "", false
	}
	return strings.TrimSpace(string(out)), true
}

func atoi(s string) int {
	n := 0
	for _, c := range s {
		if c < '0' || c > '9' {
			return 0
		}
		n = n*10 + int(c-'0')
	}
	return n
}

// buildRevision prefers the VCS stamp embedded by 'go build'; it falls back to
// asking git in the working directory (e.g. under 'go test').
func buildRevision() (rev string, dirty bool, source string) {
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, s := range info.Settings {
			switch s.Key {
			case "vcs.revision":
				rev = s.Value
			case "vcs.modified":
				dirty = s.Value == "true"
			}
		}
		if rev != "" {
			return rev, dirty, "buildinfo"
		}
	}
	out, err := exec.Command("git", "rev-parse", "HEAD").Output()
	if err != nil {
		return "unknown", true, "none"
	}
	status, err := exec.Command("git", "status", "--porcelain", "--untracked-files=no").Output()
	return strings.TrimSpace(string(out)), err != nil || len(strings.TrimSpace(string(status))) > 0, "git"
}

// timerResolution returns the smallest positive difference observed between
// consecutive time.Now readings (41.67ns on Apple Silicon).
func timerResolution() int64 {
	best := int64(1 << 62)
	for i := 0; i < 200000; i++ {
		a := time.Now()
		b := time.Now()
		for b.Equal(a) {
			b = time.Now()
		}
		if d := int64(b.Sub(a)); d > 0 && d < best {
			best = d
		}
	}
	return best
}

func detectPlatform() platform {
	p := platform{GoVersion: runtime.Version(), GOOS: runtime.GOOS, GOARCH: runtime.GOARCH, GOMAXPROCS: runtime.GOMAXPROCS(0), TranslatedSource: "not-applicable"}
	p.Revision, p.Dirty, p.RevisionSource = buildRevision()
	p.TimerResolution = timerResolution()
	// macOS exposes no per-thread core type; the scheduler may migrate the
	// worker between P- and E-cores.
	p.CoreType = "unknown"
	switch runtime.GOOS {
	case "darwin":
		p.CPU, _ = sysctl("machdep.cpu.brand_string")
		if v, ok := sysctl("hw.perflevel0.logicalcpu"); ok {
			p.PerfCores = atoi(v)
		}
		if v, ok := sysctl("hw.perflevel1.logicalcpu"); ok {
			p.EfficiencyCores = atoi(v)
		}
		// The key is absent on Intel Macs, which cannot be translated.
		v, ok := sysctl("sysctl.proc_translated")
		p.Translated, p.TranslatedSource = ok && v == "1", "sysctl.proc_translated"
		if !ok {
			p.TranslatedSource = "sysctl.proc_translated-absent"
		}
	case "linux":
		if data, err := os.ReadFile("/proc/cpuinfo"); err == nil {
			for _, line := range strings.Split(string(data), "\n") {
				if k, v, ok := strings.Cut(line, ":"); ok && strings.TrimSpace(k) == "model name" {
					p.CPU = strings.TrimSpace(v)
					break
				}
			}
		}
	}
	return p
}

// performanceValid reports whether wall-clock timing from this run may be
// used as performance evidence.
func performanceValid(c config, p platform) (bool, []string) {
	var reasons []string
	if c.AllocationAudit {
		reasons = append(reasons, "allocation audit instrumentation")
	}
	if p.Translated && !c.AllowTranslated {
		reasons = append(reasons, "binary ran under Rosetta 2 translation")
	}
	if p.Revision == "unknown" {
		reasons = append(reasons, "unknown revision")
	}
	return len(reasons) == 0, reasons
}
