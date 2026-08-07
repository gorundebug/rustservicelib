#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
source_schema=${1:-"$root/../servicelib/api/serviceapi.yaml"}

python3 "$root/src/api/generate.py" "$source_schema"
