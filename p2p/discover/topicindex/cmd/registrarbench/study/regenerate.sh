#!/bin/bash
# Regenerates every table, figure and value in docs/registrar-overhead from the
# raw result files, and renders registrar-overhead.md. It runs no experiments.
# Usage (from p2p/discover): regenerate.sh [results-root] [docs-dir]
# Requires python3 with numpy, scipy and matplotlib (PYTHON overrides the interpreter).
set -euo pipefail
root="${1:-results/registrar-overhead}"
docs="${2:-docs/registrar-overhead}"
"${PYTHON:-python3}" "$(dirname "$0")/study.py" "$root" "$docs"
