#!/bin/bash

set -e

TESTS_CHDIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export TESTS_CHDIR
cd "$TESTS_CHDIR" || exit
./tools/venv tests -q "$@"
