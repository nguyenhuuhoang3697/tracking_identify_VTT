#!/usr/bin/env python3
"""Remove lines with a specific date from xac_thuc file"""

import sys
from pathlib import Path

xac_thuc_file = Path("xac_thuc_theo_dinh_nghia_v_xac_thuc_luy_ke_20260421.txt")
target_date = "20260428"

if not xac_thuc_file.exists():
    print(f"Error: {xac_thuc_file} not found")
    sys.exit(1)

print(f"Removing data from {target_date} in {xac_thuc_file.name}...")

# Read all lines
with open(xac_thuc_file, 'r', encoding='utf-8') as f:
    lines = f.readlines()

# Keep header + lines that don't start with target date
header = lines[0]
data_lines = [line for line in lines[1:] if not line.startswith(target_date)]

# Count removed lines
removed_count = len(lines) - 1 - len(data_lines)
print(f"Removed {removed_count} lines with date {target_date}")

# Write back
with open(xac_thuc_file, 'w', encoding='utf-8') as f:
    f.write(header)
    f.writelines(data_lines)

print(f"Successfully updated {xac_thuc_file.name}")
