#!/bin/bash
# Builds a native registrarbench stamped with the git revision. The tracked
# working tree must be clean (untracked files are ignored; Go's vcs.modified
# would count them). Usage (from p2p/discover): build.sh <output>
set -euo pipefail
out="$1"
if [ -n "$(git status --porcelain --untracked-files=no)" ]; then
  echo "tracked files modified; commit before building a study binary" >&2
  git status --short --untracked-files=no >&2
  exit 1
fi
arch=$(uname -m); [ "$arch" = x86_64 ] && arch=amd64
rev=$(git rev-parse HEAD)
mkdir -p "$(dirname "$out")"
GOARCH=$arch go build -ldflags "-X main.stampRevision=$rev -X main.stampDirty=false" -o "$out" ./topicindex/cmd/registrarbench
case "$(file -b "$out")" in
  *"$(uname -m)"*) ;;
  *) echo "binary is not native: $(file -b "$out")" >&2; exit 1 ;;
esac
