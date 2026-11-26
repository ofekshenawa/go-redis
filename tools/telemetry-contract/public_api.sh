#!/usr/bin/env bash
# Compare the exported root Go package. Zero output means source-compatible.
# The caller supplies trusted tooling and two data checkouts in a read-only job.
set -euo pipefail
base=$1
candidate=$2
output=$3
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT

export GOWORK=off CGO_ENABLED=0
GOBIN="$scratch" go install golang.org/x/exp/cmd/apidiff@v0.0.0-20261124000051-d6646c509b6d
# Export the same package from both revisions using one pinned apidiff binary.
(cd "$base" && "$scratch/apidiff" -w "$scratch/base.export" .)
(cd "$candidate" && "$scratch/apidiff" -w "$scratch/candidate.export" .)
# The report builder reads one incompatible change per line from the output file.
"$scratch/apidiff" -incompatible "$scratch/base.export" "$scratch/candidate.export" > "$scratch/findings"
cat "$scratch/findings" > "$output"
