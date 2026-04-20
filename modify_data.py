# -*- coding: utf-8 -*-
"""
modify_data.py
--------------
Bước 1: Map user_name (logch_theokhunggio) → MA_USER (Map Usser) để bổ sung cột Shop
Bước 2: Nếu Shop nằm trong cột "Nhân viên tham gia" (sheet N14, ngăn cách bởi ;)
         → tạo cột Channel = 'Điểm hỗ trợ', còn lại để blank
Kết quả lưu ra logch_theokhunggio_out.csv (UTF-8 BOM)
"""

import pandas as pd
import os
import glob
import sys
_stdout_reconfigure = getattr(sys.stdout, "reconfigure", None)
if callable(_stdout_reconfigure):
    _stdout_reconfigure(encoding='utf-8')

BASE = os.path.dirname(os.path.abspath(__file__))

# ── Đường dẫn file ───────────────────────────────────────────
LOGCH_PATH  = os.path.join(BASE, "logch_theokhunggio.txt")
MAP_PATH    = os.path.join(BASE, "Map Usser.xlsx")
DIEM_PATH   = os.path.join(BASE, "Điểm xác thực + User xác thực.xlsx")
OUT_PATH    = os.path.join(BASE, "logch_theokhunggio_out2.csv")

# Map ngày → tên sheet trong file Điểm xác thực
DIEM_DEFAULT_SHEET = "Lũy kế"


def build_diem_sheet_map(excel_path, ngay_values, default_sheet):
    """Tự động map ngày trong dữ liệu sang sheet tương ứng trong workbook."""
    excel = pd.ExcelFile(excel_path)
    sheet_names = [str(name).strip() for name in excel.sheet_names]
    available = set(sheet_names)

    sheet_map = {}
    unmatched = []

    for ngay in ngay_values:
        ngay_str = str(ngay).strip()
        if not ngay_str:
            continue

        if ngay_str in available:
            sheet_map[ngay_str] = ngay_str
            continue

        day_suffix = ngay_str[-2:]
        matched_sheets = [name for name in sheet_names if name.isdigit() and name.zfill(2) == day_suffix]

        if len(matched_sheets) == 1:
            sheet_map[ngay_str] = matched_sheets[0]
        else:
            unmatched.append(ngay_str)

    if unmatched:
        print(f"  [Cảnh báo] Không tự map được sheet cho các ngày: {sorted(set(unmatched))}")
        print(f"  [Cảnh báo] Các ngày này sẽ dùng sheet mặc định '{default_sheet}'")

    return sheet_map, sheet_names

# ── Bước -1: Quét file VBI_TT08* và cập nhật logch_theokhunggio.txt ────
print("[Bước -1] Quét file VBI_TT08* ...")
vbi_files = sorted(glob.glob(os.path.join(BASE, "VBI_TT08*.txt")))
if vbi_files:
    print(f"  Tìm thấy {len(vbi_files)} file: {[os.path.basename(f) for f in vbi_files]}")
    logch_base = pd.read_csv(LOGCH_PATH, sep='|', encoding='utf-8-sig', dtype=str)
    base_cols = logch_base.columns.tolist()
    for vbi_file in vbi_files:
        # Read VBI files with flexible separator detection. Some files use '|' while
        # others (exported differently) may use commas. Try '|' first for
        # backward compatibility, then fall back to python engine auto-detection.
        try:
            vbi_df = pd.read_csv(vbi_file, sep='|', encoding='utf-8-sig', dtype=str)
            if 'ngay' not in vbi_df.columns:
                raise ValueError("missing 'ngay' column with '|' sep")
        except Exception:
            vbi_df = pd.read_csv(vbi_file, sep=None, engine='python', encoding='utf-8-sig', dtype=str)
        # If new VBI files include 'sltb' and 'sltb_loaitru', compute 'slgiaodich' = sltb - sltb_loaitru
        if 'sltb' in vbi_df.columns and 'sltb_loaitru' in vbi_df.columns:
            vbi_df['sltb'] = pd.to_numeric(vbi_df['sltb'], errors='coerce').fillna(0)
            vbi_df['sltb_loaitru'] = pd.to_numeric(vbi_df['sltb_loaitru'], errors='coerce').fillna(0)
            vbi_df['slgiaodich'] = (vbi_df['sltb'] - vbi_df['sltb_loaitru']).astype(int)
            # keep output column as string to match existing files
            vbi_df['slgiaodich'] = vbi_df['slgiaodich'].astype(str)
        # Normalize ngày column name if there are variants (e.g., 'ngày' with accent)
        if 'ngay' not in vbi_df.columns and 'ngày' in vbi_df.columns:
            vbi_df['ngay'] = vbi_df['ngày']
        replace_days = vbi_df['ngay'].dropna().unique().tolist()
        print(f"  {os.path.basename(vbi_file)}: ngày {replace_days}")
        logch_base = logch_base[~logch_base['ngay'].isin(replace_days)]
        new_rows = vbi_df.reindex(columns=base_cols)
        logch_base = pd.concat([logch_base, new_rows], ignore_index=True)
        print(f"    → Thay {len(new_rows):,} dòng")
    logch_base.to_csv(LOGCH_PATH, sep='|', index=False, encoding='utf-8-sig')
    print(f"  ✓ Đã cập nhật logch_theokhunggio.txt ({len(logch_base):,} dòng tổng)")
else:
    print("  Không tìm thấy file VBI_TT08* nào, bỏ qua.")

# ── Bước 0: Đọc dữ liệu ─────────────────────────────────────
print("Đọc logch_theokhunggio.txt ...")
logch = pd.read_csv(LOGCH_PATH, sep=None, engine="python", on_bad_lines='warn', encoding='utf-8-sig')
print(f"  → {len(logch):,} dòng | cols: {logch.columns.tolist()}")

logch["ngay"] = logch["ngay"].astype(str).str.strip()
diem_sheet_map, _available_sheets = build_diem_sheet_map(
    DIEM_PATH,
    logch["ngay"].dropna().unique().tolist(),
    DIEM_DEFAULT_SHEET,
)
print(f"  → Tự map được {len(diem_sheet_map):,} ngày sang sheet")

print("Đọc Map Usser.xlsx ...")
map_user = pd.read_excel(MAP_PATH)
# Chuẩn hoá kiểu string để join an toàn
map_user["MA_USER"] = map_user["MA_USER"].astype(str).str.strip()
map_user["Shop"]    = map_user["Shop"].astype(str).str.strip()
# Giữ lại map 1-1 (ưu tiên dòng đầu nếu MA_USER trùng)
map_user = map_user.drop_duplicates(subset="MA_USER", keep="first")
print(f"  → {len(map_user):,} user | cols: {map_user.columns.tolist()}")

print("Đọc các sheet Điểm xác thực theo ngày ...")
_nv_col = "Nhân viên tham gia"
shop_sets = {}
for _sh in set(diem_sheet_map.values()) | {DIEM_DEFAULT_SHEET}:
    _df = pd.read_excel(DIEM_PATH, sheet_name=_sh)
    _s = set()
    for _cell in _df[_nv_col].dropna():
        for _item in str(_cell).split(";"):
            _v = _item.strip()
            if _v:
                _s.add(_v)
    shop_sets[_sh] = _s
    print(f"  Sheet '{_sh}': {len(_s):,} giá trị duy nhất")

# ── Bước 1: Bổ sung cột Shop vào logch ──────────────────────
print("\n[Bước 1] Merge Shop theo user_name ↔ MA_USER ...")
logch["user_name"] = logch["user_name"].astype(str).str.strip()

logch = logch.merge(
    map_user[["MA_USER", "Shop"]],
    left_on="user_name",
    right_on="MA_USER",
    how="left"
).drop(columns=["MA_USER"])  # bỏ cột key thừa

matched = logch["Shop"].notna().sum()
print(f"  → Map được {matched:,}/{len(logch):,} dòng ({matched/len(logch)*100:.1f}%)")

# ── Bước 2+3: Tạo cột Channel theo ngày ────────────────────
print("\n[Bước 2+3] Tạo cột Channel theo ngày (sheet tương ứng) ...")

logch["Channel"] = ""

# Các ngày có sheet riêng
for _ngay_val, _sheet_name in diem_sheet_map.items():
    _mask = (logch["ngay"] == _ngay_val) & logch["Shop"].notna()
    logch.loc[_mask, "Channel"] = logch.loc[_mask, "Shop"].apply(
        lambda s: "Điểm hỗ trợ" if str(s).strip() in shop_sets[_sheet_name] else ""
    )
    print(f"  Ngày {_ngay_val} (sheet '{_sheet_name}'): {(_mask & (logch['Channel'] == 'Điểm hỗ trợ')).sum():,} dòng")

# Các ngày còn lại → sheet mặc định
_mask_default = ~logch["ngay"].isin(diem_sheet_map.keys()) & logch["Shop"].notna()
logch.loc[_mask_default, "Channel"] = logch.loc[_mask_default, "Shop"].apply(
    lambda s: "Điểm hỗ trợ" if str(s).strip() in shop_sets[DIEM_DEFAULT_SHEET] else ""
)

diem_count = (logch["Channel"] == "Điểm hỗ trợ").sum()
print(f"  → Tổng: {diem_count:,} dòng được gán Channel = 'Điểm hỗ trợ'")

# ── Bước 4: Shop có tiền tố CNKD + Channel đang blank → CNKD ─
print("\n[Bước 4] Gán Channel = 'CNKD' cho Shop tiền tố 'CNKD' ...")
mask_cnkd = (
    logch["Channel"] == ""
) & (
    logch["Shop"].apply(lambda s: pd.notna(s) and str(s).strip().upper().startswith("CNKD"))
)
logch.loc[mask_cnkd, "Channel"] = "CNKD"

cnkd_count = mask_cnkd.sum()
print(f"  → {cnkd_count:,} dòng được gán Channel = 'CNKD'")
print(f"  → {(logch['Channel'] == '').sum():,} dòng vẫn còn blank")

# ── Bước 5: kenh == 'MYVT' → Channel = 'MYVIETTEL' ──────────
print("\n[Bước 5] Gán Channel = 'MYVIETTEL' cho kenh = 'MYVT' ...")
mask_myvt = logch["kenh"].astype(str).str.strip() == "MYVT"
logch.loc[mask_myvt, "Channel"] = "MYVIETTEL"
print(f"  → {mask_myvt.sum():,} dòng được gán Channel = 'MYVIETTEL'")
print(f"  → {(logch['Channel'] == '').sum():,} dòng vẫn còn blank")

# ── Bước 6: Shop blank → map user_name với MA_NHAN_VIEN, fill Shop = KENH ──
print("\n[Bước 6] Đọc sqlxlsx_export_nv_kenh.xlsx (calamine engine) ...")
NV_KENH_XLSX = os.path.join(BASE, "sqlxlsx_export_nv_kenh.xlsx")

nv_kenh = pd.read_excel(NV_KENH_XLSX, usecols=["MA_NHAN_VIEN", "KENH"],
                        dtype=str, engine="calamine")
nv_kenh["MA_NHAN_VIEN"] = nv_kenh["MA_NHAN_VIEN"].str.strip()
nv_kenh["KENH"]         = nv_kenh["KENH"].str.strip()
nv_kenh = nv_kenh.drop_duplicates(subset="MA_NHAN_VIEN", keep="first")
print(f"  → {len(nv_kenh):,} bản ghi")

mask_blank_shop = logch["Shop"].isna()
print(f"  → Số dòng Shop đang blank: {mask_blank_shop.sum():,}")

logch_blank = logch[mask_blank_shop].merge(
    nv_kenh,
    left_on="user_name",
    right_on="MA_NHAN_VIEN",
    how="left"
)
logch.loc[mask_blank_shop, "Shop"] = logch_blank["KENH"].values

filled = logch.loc[mask_blank_shop, "Shop"].notna().sum()
print(f"  → Map được {filled:,}/{mask_blank_shop.sum():,} dòng Shop blank")

# ── Bước 7: Channel blank → suy ra từ giá trị cột Shop ──────
print("\n[Bước 7] Fill Channel blank dựa vào Shop ...")

def infer_channel(shop):
    if pd.isna(shop):
        return ""
    s = str(shop).strip()
    su = s.upper()
    # CHTT/CHUQ: giá trị 'CHUQ', 'CHTT', hoặc tiền tố '1600'
    if su in ("CHUQ", "CHTT") or su.startswith("1600"):
        return "CHTT/CHUQ"
    # CNKD: hậu tố 'DVTM'
    if su.endswith("DVTM"):
        return "CNKD"
    # Điểm bán: giá trị 'HKD' hoặc tiền tố '6100'
    if su == "HKD" or su.startswith("6100"):
        return "Điểm bán"
    # KHDN
    if su == "KHDN":
        return "KHDN"
    return ""

mask_blank_ch = logch["Channel"] == ""
logch.loc[mask_blank_ch, "Channel"] = logch.loc[mask_blank_ch, "Shop"].apply(infer_channel)

for val in ["CHTT/CHUQ", "CNKD", "Điểm bán"]:
    cnt = (logch["Channel"] == val).sum()
    print(f"  → '{val}': {cnt:,} dòng")
print(f"  → Còn blank: {(logch['Channel'] == '').sum():,} dòng")

# ── Lưu kết quả ─────────────────────────────────────────────
logch.to_csv(OUT_PATH, index=False, encoding="utf-8-sig")
print(f"\n✓ Đã lưu → {OUT_PATH}")
print(f"  Cột trong file output: {logch.columns.tolist()}")
print(logch.head(5).to_string())
