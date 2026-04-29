#!/usr/bin/env python3
"""
Append VBI_TT08.txt data to xac_thuc file, removing unnecessary columns
"""
import pandas as pd

# File paths
vbi_file = "VBI_TT08.txt"
xac_thuc_file = "xac_thuc_theo_dinh_nghia_v_xac_thuc_luy_ke_20260421.txt"

# Columns to remove from VBI
cols_to_remove = {
    'sltb_01', 'sltb_02',
    'sltb_01_trung_cccd_yeu_the', 'sltb_01_trung_cccd_con_lai',
    'sltb_01_full', 'sltb_02_full',
    'sltb_01_trung_cccd_yeu_the_full', 'sltb_01_trung_cccd_con_lai_full'
}

# Column mapping from VBI to xac_thuc (now using lowercase to match VBI format)
column_mapping = {
    'prd_id': None,  # Skip this column if exists
    'khunggio': 'khunggio',
    'province_code_home': 'province_code_home',
    'kenh': 'kenh',
    'user_name': 'user_name',
    'cmnd_9so': 'cmnd_9so',
    'slgiaodich': 'slgiaodich',
    'sltb': 'sltb',
    'sltb_loaitru': 'sltb_loaitru',
    'sltb_xac_thuc': 'sltb_xac_thuc_final',
    'sltb_xac_thuc_gboc': 'sltb_xac_thuc_final_giao_gboc',
    'sltb_xacthuc_gboc_offline': 'sltb_xac_thuc_final_giao_gboc_offline',  # Note: xacthuc not xac_thuc
    'sltb_gboc_60tuoi': 'sltb_gboc_60tuoi',
    'sltb_gboc_ko_nfc': 'sltb_gboc_ko_nfc',
    'sltb_gboc_vungsau_vungxa': 'sltb_gboc_vung_sau_vung_xa',
    'sltb_gboc_cmt': 'sltb_gboc_cmt',
}

# Read VBI file
print(f"Reading {vbi_file}...")
df_vbi = pd.read_csv(vbi_file, sep='|', dtype=str)

# Try to get report date from PRD_ID or ngay column
if 'PRD_ID' in df_vbi.columns:
    prd_values = df_vbi['PRD_ID'].dropna().astype(str).str.strip()
elif 'ngay' in df_vbi.columns:
    prd_values = df_vbi['ngay'].dropna().astype(str).str.strip()
else:
    raise ValueError("VBI file is missing both PRD_ID and ngay columns")

if prd_values.empty:
    raise ValueError("VBI file does not contain any date values")

report_dates = sorted(prd_values.unique())
if len(report_dates) != 1:
    raise ValueError(f"VBI file contains multiple dates: {report_dates}")

report_date = report_dates[0]
print(f"Detected report date: {report_date}")

print(f"VBI columns: {list(df_vbi.columns)}")

# Remove unwanted columns
cols_to_drop = [c for c in df_vbi.columns if c in cols_to_remove]
if cols_to_drop:
    print(f"Removing columns: {cols_to_drop}")
    df_vbi = df_vbi.drop(columns=cols_to_drop, errors='ignore')

# Rename columns to match xac_thuc format (lowercase)
rename_dict = {}
for vbi_col in df_vbi.columns:
    if vbi_col == 'PRD_ID':
        continue  # Skip PRD_ID
    if vbi_col in column_mapping:
        xac_col = column_mapping[vbi_col]
        if xac_col:  # Skip None
            rename_dict[vbi_col] = xac_col
    else:
        # For unmapped columns, convert to lowercase
        rename_dict[vbi_col] = vbi_col.lower()

# Drop PRD_ID if present
if 'PRD_ID' in df_vbi.columns:
    df_vbi = df_vbi.drop(columns=['PRD_ID'])

# Rename columns
df_vbi = df_vbi.rename(columns=rename_dict)

# Ensure ngay column exists and is at the beginning
if 'ngay' not in df_vbi.columns:
    df_vbi.insert(0, 'ngay', report_date)
else:
    # Move ngay to the beginning if it's not already
    ngay_col = df_vbi.pop('ngay')
    df_vbi.insert(0, 'ngay', ngay_col)

print(f"Processed VBI columns: {list(df_vbi.columns)}")
print(f"VBI data shape: {df_vbi.shape}")

# Read existing xac_thuc file
print(f"\nReading {xac_thuc_file}...")
df_xac = pd.read_csv(xac_thuc_file, sep='|', dtype=str)
print(f"Existing xac_thuc shape: {df_xac.shape}")
print(f"Existing columns: {list(df_xac.columns)}")

# Make reruns idempotent by replacing any existing rows for the same report date.
existing_same_day = (df_xac['ngay'].astype(str).str.strip() == report_date).sum()
if existing_same_day:
    print(f"Found {existing_same_day} existing rows for {report_date}; removing them before append...")
    df_xac = df_xac[df_xac['ngay'].astype(str).str.strip() != report_date].copy()

# Ensure columns match before appending
expected_cols = list(df_xac.columns)
df_vbi_aligned = df_vbi[expected_cols].copy()

# Append VBI data to xac_thuc
df_combined = pd.concat([df_xac, df_vbi_aligned], ignore_index=True)
print(f"Combined data shape: {df_combined.shape}")

# Write back to file
print(f"\nAppending to {xac_thuc_file}...")
df_combined.to_csv(xac_thuc_file, sep='|', index=False, quoting=3)

print(f"✅ Done! Updated {xac_thuc_file} with {len(df_vbi_aligned)} new rows")
print(f"New total rows: {len(df_combined)}")
