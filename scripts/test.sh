#!/bin/sh
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
"$root/scripts/check_api.sh"
docker build --target test --tag rustservicelib-test .
