#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
canonical_schema="$root/../servicelib/api/serviceapi.yaml"

if [ -f "$canonical_schema" ]; then
  python3 "$root/src/api/generate.py" "$canonical_schema" --check
else
  echo "canonical serviceapi.yaml is not present; checking committed Rust code only"
fi
