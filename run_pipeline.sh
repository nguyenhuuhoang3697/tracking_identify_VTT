#!/usr/bin/env bash
set -euo pipefail

# run_pipeline.sh — regenerate data.json and updated Excel
# Usage: ./run_pipeline.sh [args-for-luong_du_lieu.py]
# Example: ./run_pipeline.sh --from-date 2026-04-19 --report-date 2026-04-22

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Prefer venv python (Unix or Windows path), else fallback to system python
if [ -x "$SCRIPT_DIR/.venv/bin/python" ]; then
  PY="$SCRIPT_DIR/.venv/bin/python"
elif [ -x "$SCRIPT_DIR/.venv/Scripts/python.exe" ]; then
  PY="$SCRIPT_DIR/.venv/Scripts/python.exe"
elif command -v python3 >/dev/null 2>&1; then
  PY=python3
elif command -v python >/dev/null 2>&1; then
  PY=python
else
  echo "Error: No python interpreter found. Create a virtualenv or install Python."
  exit 1
fi

export PYTHONIOENCODING=UTF-8

echo "Using Python: $PY"

echo "Running luong_du_lieu.py..."
# forward any args to the script (e.g. --from-date, --report-date)
"$PY" luong_du_lieu.py "$@"

RET=$?
if [ $RET -ne 0 ]; then
  echo "luong_du_lieu.py failed with exit code $RET"
  exit $RET
fi

echo "Pipeline finished — data.json and f_total_cap_nhat.xlsx updated."

echo "To serve report locally, you can run:"
echo "  $PY server.py"

echo "Or with Python's simple server (static):"
echo "  python -m http.server 8000 --directory ."
