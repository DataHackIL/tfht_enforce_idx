#!/bin/sh
set -eu

if [ -n "${PYTHONPATH:-}" ]; then
    export PYTHONPATH="src:${PYTHONPATH}"
else
    export PYTHONPATH="src"
fi

exec python "$@"
