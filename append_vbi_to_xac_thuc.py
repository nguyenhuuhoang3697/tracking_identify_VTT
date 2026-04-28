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
    'SLTB_01', 'SLTB_02',
    'SLTB_01_TRUNG_CCCD_YEU_THE', 'SLTB_01_TRUNG_CCCD_CON_LAI',
    'SLTB_01_FULL', 'SLTB_02_FULL',
    'SLTB_01_TRUNG_CCCD_YEU_THE_FULL', 'SLTB_01_TRUNG_CCCD_CON_LAI_FULL'
}

# Column mapping from VBI to xac_thuc (case-insensitive)
column_mapping = {
    'PRD_ID': None,  # Skip this column
    'KHUNGGIO': 'khunggio',
    'PROVINCE_CODE_HOME': 'province_code_home',
    'KENH': 'kenh',
    'USER_NAME': 'user_name',
    'CMND_9SO': 'cmnd_9so',
    'SLGIAODICH': 'slgiaodich',
    'SLTB': 'sltb',
    'SLTB_LOAITRU': 'sltb_loaitru',
    'SLTB_XAC_THUC': 'sltb_xac_thuc_final',
    'SLTB_XAC_THUC_GBOC': 'sltb_xac_thuc_final_giao_gboc',
    'SLTB_XAC_THUC_GBOC_OFFLINE': 'sltb_xac_thuc_final_giao_gboc_offline',
    'SLTB_GBOC_60TUOI': 'sltb_gboc_60tuoi',
    'SLTB_GBOC_KO_NFC': 'sltb_gboc_ko_nfc',
    'SLTB_GBOC_VUNGSAU_VUNGXA': 'sltb_gboc_vung_sau_vung_xa',
    'SLTB_GBOC_CMT': 'sltb_gboc_cmt',
}

# Read VBI file
print(f"Reading {vbi_file}...")
df_vbi = pd.read_csv(vbi_file, sep='|', dtype=str)

if 'PRD_ID' not in df_vbi.columns:
    raise ValueError("VBI file is missing PRD_ID column")

prd_values = df_vbi['PRD_ID'].dropna().astype(str).str.strip()
if prd_values.empty:
    raise ValueError("VBI file does not contain any PRD_ID values")

report_dates = sorted(prd_values.unique())
if len(report_dates) != 1:
    raise ValueError(f"VBI file contains multiple PRD_ID dates: {report_dates}")

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
    if vbi_col in column_mapping:
        xac_col = column_mapping[vbi_col]
        if xac_col:  # Skip None (PRD_ID)
            rename_dict[vbi_col] = xac_col
    else:
        # For unmapped columns, convert to lowercase
        rename_dict[vbi_col] = vbi_col.lower()

# Drop PRD_ID if present
df_vbi = df_vbi.drop(columns=['PRD_ID'], errors='ignore')

# Rename columns
df_vbi = df_vbi.rename(columns=rename_dict)

# Add ngay column at the beginning based on PRD_ID from the VBI file
df_vbi.insert(0, 'ngay', report_date)

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
