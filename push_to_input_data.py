# -*- coding: utf-8 -*-
"""
push_to_input_data.py
----------------------
Đẩy dữ liệu từ logch_theokhunggio_out2.csv sang _ghpages/input_data.csv.
Script sẽ tự lấy các ngày có trong nguồn và thay thế đúng các ngày đó trong file đích.

Mapping cột:
    ngay              → Ngày   (20260413 → "13/4")
    khunggio          → Giờ    (7        → "7h")
    province_code_home→ Tỉnh   (mã tỉnh → tên tỉnh)
    Channel           → Kênh
    slgiaodich (sum)  → SLTB
"""

import pandas as pd
import os

BASE      = os.path.dirname(os.path.abspath(__file__))
LOGCH_OUT = os.path.join(BASE, "_ghpages", "input_data.csv")
SRC_PATH  = os.path.join(BASE, "logch_theokhunggio_out2.csv")
OUT_PATH  = LOGCH_OUT   # ghi đè input_data.csv

# ── Mapping mã tỉnh → tên tỉnh ───────────────────────────────
PROVINCE_MAP = {
    # 15 tỉnh hiện có trong input_data
    "HNI": "Hà Nội",
    "HCM": "TP.HCM",
    "DNG": "Đà Nẵng",
    "HPG": "Hải Phòng",
    "CTO": "Cần Thơ",
    "DNI": "Đồng Nai",
    "KHA": "Khánh Hòa",
    "LDG": "Lâm Đồng",
    "BNH": "Bắc Ninh",
    "QNI": "Quảng Ngãi",
    "NAN": "Nghệ An",
    "HUE": "Thừa Thiên Huế",
    "AGG": "An Giang",
    # Các tỉnh khác (sẽ được thêm vào nếu có trong logch)
    "CBG": "Cao Bằng",
    "CMU": "Cà Mau",
    "DBN": "Điện Biên",
    "DLK": "Đắk Lắk",
    "DTP": "Đồng Tháp",
    "GLI": "Gia Lai",
    "HTH": "Hà Tĩnh",
    "HYN": "Hưng Yên",
    "LCU": "Lai Châu",
    "LCI": "Lào Cai",
    "LSN": "Lạng Sơn",
    "NBH": "Ninh Bình",
    "PTO": "Phú Thọ",
    "QNH": "Quảng Ninh",
    "QTI": "Quảng Trị",
    "SLA": "Sơn La",
    "THA": "Thanh Hóa",
    "TNH": "Tây Ninh",
    "TNN": "Thái Nguyên",
    "TQG": "Tuyên Quang",
    "VLG": "Vĩnh Long",
}

# ── Đọc nguồn ────────────────────────────────────────────────
print("Đọc logch_theokhunggio_out.csv ...")
src = pd.read_csv(SRC_PATH, dtype=str, encoding_errors="replace")
src.columns = [c.lstrip('\ufeff') for c in src.columns]

# Lấy tất cả ngày có mặt trong nguồn và cập nhật đúng các ngày đó
SOURCE_DAYS = sorted(src["ngay"].dropna().astype(str).str.strip().unique().tolist())
src = src[src["ngay"].isin(SOURCE_DAYS)].copy()
print(f"  → {len(src):,} dòng (ngày nguồn: {SOURCE_DAYS})")

# Bỏ Channel blank (493 dòng không xác định được kênh)
before = len(src)
src = src[src["Channel"].notna() & (src["Channel"].astype(str).str.strip() != "")]
print(f"  → {before - len(src)} dòng bỏ qua (Channel blank)")

# Convert slgiaodich sang số
src["slgiaodich"] = pd.to_numeric(src["slgiaodich"], errors="coerce").fillna(0)

# ── Transform cột ────────────────────────────────────────────
# Ngày: 20260413 → 13/4
src["Ngày"] = src["ngay"].apply(lambda x: f"{int(x[6:8])}/{int(x[4:6])}")

# Giờ: 7 → 7h
src["Giờ"] = src["khunggio"].apply(lambda x: f"{int(x)}h")

# Tỉnh: mã → tên
src["Tỉnh"] = src["province_code_home"].map(PROVINCE_MAP)
unmapped = src["Tỉnh"].isna().sum()
if unmapped:
    codes_not_found = src.loc[src["Tỉnh"].isna(), "province_code_home"].value_counts()
    print(f"  ⚠ {unmapped} dòng không map được mã tỉnh:")
    print(codes_not_found.to_string())
    # Giữ lại mã gốc thay vì bỏ
    src["Tỉnh"] = src["Tỉnh"].fillna(src["province_code_home"])

# Kênh
src["Kênh"] = src["Channel"].astype(str).str.strip()

# ── Aggregate (sum slgiaodich theo Tỉnh, Ngày, Giờ, Kênh) ───
print("\nAggregate theo (Tỉnh, Ngày, Giờ, Kênh) ...")
new_rows = (
    src.groupby(["Tỉnh", "Ngày", "Giờ", "Kênh"], as_index=False)["slgiaodich"]
    .sum()
    .rename(columns={"slgiaodich": "SLTB"})
)[["Tỉnh", "Ngày", "Giờ", "Kênh", "SLTB"]]

print(f"  → {len(new_rows):,} dòng sau aggregate")
print(f"  → Tỉnh unique: {sorted(new_rows['Tỉnh'].unique())}")
print(f"  → Kênh unique: {sorted(new_rows['Kênh'].unique())}")
print(f"  → Ngày unique: {sorted(new_rows['Ngày'].unique())}")

# ── Đọc input_data hiện tại, bỏ ngày 13+14/4 ────────────────
print("\nĐọc input_data.csv hiện tại ...")
existing = pd.read_csv(LOGCH_OUT, encoding="utf-8-sig")
print(f"  → {len(existing):,} dòng hiện có")
source_days_display = [f"{int(day[6:8])}/{int(day[4:6])}" for day in SOURCE_DAYS]
existing = existing[~existing["Ngày"].isin(source_days_display)]
print(f"  → {len(existing):,} dòng sau khi bỏ ngày {', '.join(source_days_display)}")

# ── Gộp và lưu ───────────────────────────────────────────────
result = pd.concat([existing, new_rows], ignore_index=True)

# Loại bỏ trùng theo khóa tổng hợp để tránh lặp dữ liệu khi chạy lại pipeline
result = result.drop_duplicates(subset=["Tỉnh", "Ngày", "Giờ", "Kênh"], keep="last")

# Sắp xếp theo Ngày, Giờ số, Tỉnh, Kênh
result["_day_sort"] = result["Ngày"].apply(lambda x: int(str(x).split("/")[0]))
result["_hour_sort"] = result["Giờ"].apply(lambda x: int(str(x).replace("h", "")))
result = result.sort_values(["_day_sort", "_hour_sort", "Tỉnh", "Kênh"]).drop(
    columns=["_day_sort", "_hour_sort"]
)

result.to_csv(OUT_PATH, index=False, encoding="utf-8-sig")
print(f"\n✓ Đã lưu → {OUT_PATH}")
print(f"  Tổng: {len(result):,} dòng")
print(f"  Ngày: {sorted(result['Ngày'].unique(), key=lambda x: int(x.split('/')[0]))}")
print(f"  Kênh: {sorted(result['Kênh'].unique())}")
print()
print("Preview 5 dòng ngày 13/4:")
print(result[result["Ngày"] == "13/4"].head(5).to_string(index=False))
print()
print("Preview 5 dòng ngày 14/4:")
print(result[result["Ngày"] == "14/4"].head(5).to_string(index=False))
