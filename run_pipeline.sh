#!/usr/bin/env bash
# run_pipeline.sh — Chạy full luồng xử lý dữ liệu VBI
# Dùng với Git Bash hoặc WSL trên Windows

set -e  # Dừng ngay nếu có lỗi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON="$SCRIPT_DIR/venv/Scripts/python.exe"

echo "========================================"
echo "  VBI Pipeline — $(date '+%Y-%m-%d %H:%M:%S')"
echo "========================================"

echo ""
echo "[1/2] Chạy modify_data.py ..."
"$PYTHON" "$SCRIPT_DIR/modify_data.py"

echo ""
echo "[2/2] Chạy push_to_input_data.py ..."
"$PYTHON" "$SCRIPT_DIR/push_to_input_data.py"

echo ""
echo "========================================"
echo "  ✓ Pipeline hoàn thành!"
echo "========================================"
