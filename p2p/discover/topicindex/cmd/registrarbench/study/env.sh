#!/bin/bash
# Writes the run environment as JSON to stdout. Usage: env.sh <binary>
set -euo pipefail
bin="$1"
cd "$(git rev-parse --show-toplevel)"
dirty=false
[ -n "$(git status --porcelain --untracked-files=no)" ] && dirty=true
translated=$(sysctl -n sysctl.proc_translated 2>/dev/null || echo absent)
cat <<JSON
{"revision": "$(git rev-parse HEAD)", "dirty": $dirty, "go_version": "$(go env GOVERSION)",
 "goos": "$(go env GOOS)", "goarch_toolchain": "$(go env GOARCH)", "binary": "$(file -b "$bin")",
 "cpu_model": "$(sysctl -n machdep.cpu.brand_string 2>/dev/null || grep -m1 'model name' /proc/cpuinfo | cut -d: -f2)",
 "performance_cores": "$(sysctl -n hw.perflevel0.logicalcpu 2>/dev/null || echo)",
 "efficiency_cores": "$(sysctl -n hw.perflevel1.logicalcpu 2>/dev/null || echo)",
 "shell_proc_translated": "$translated", "date": "$(date -u +%Y-%m-%dT%H:%M:%SZ)"}
JSON
