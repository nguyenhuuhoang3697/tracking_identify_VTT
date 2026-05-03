from __future__ import annotations

import argparse
import json
from datetime import date, datetime, timedelta
from typing import cast

import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Cap nhat cot 'Luy ke TH' trong f_total tu du lieu xac_thuc va xuat data.json"
    )
    parser.add_argument(
        "--f-total",
        default="f_total.xlsx",
        help="Duong dan file f_total (mac dinh: f_total.xlsx)",
    )
    parser.add_argument(
        "--xac-thuc",
        default="xac_thuc_theo_dinh_nghia_v_xac_thuc_luy_ke_20260421.txt",
        help="Duong dan file xac_thuc txt (mac dinh: file txt hien co)",
    )
    parser.add_argument(
        "--output",
        default="f_total_cap_nhat.xlsx",
        help="Duong dan file output (mac dinh: f_total_cap_nhat.xlsx)",
    )
    parser.add_argument(
        "--json-output",
        default="data.json",
        help="Duong dan file JSON cho report.html (mac dinh: data.json)",
    )
    parser.add_argument(
        "--from-date",
        default="2026-04-19",
        help="Chi tinh du lieu tu ngay nay tro di, dinh dang YYYY-MM-DD",
    )
    parser.add_argument(
        "--report-date",
        default=None,
        help="Ngay bao cao (mac dinh: hom qua, dinh dang YYYY-MM-DD)",
    )
    return parser.parse_args()


def normalize_numeric(series: pd.Series) -> pd.Series:
    return cast(pd.Series, pd.to_numeric(series, errors="coerce")).fillna(0)


def build_luy_ke_map(xac_thuc_path: str, from_date: str):
    df_xt = pd.read_csv(xac_thuc_path, sep="|", low_memory=False)

    df_xt["ngay"] = pd.to_datetime(df_xt["ngay"].astype(str), format="%Y%m%d", errors="coerce")
    from_dt = datetime.strptime(from_date, "%Y-%m-%d")
    df_xt = df_xt[df_xt["ngay"] >= from_dt]

    df_xt["province_code_home"] = df_xt["province_code_home"].astype(str).str.strip()
    df_xt["kenh"] = df_xt["kenh"].astype(str).str.strip()
    df_xt["sltb_xac_thuc_final_giao_gboc"] = normalize_numeric(
        df_xt["sltb_xac_thuc_final_giao_gboc"]
    )
    df_xt["sltb_xac_thuc_final_giao_gboc_offline"] = normalize_numeric(
        df_xt["sltb_xac_thuc_final_giao_gboc_offline"]
    )
    df_xt["sltb_gboc_60tuoi"] = normalize_numeric(df_xt["sltb_gboc_60tuoi"])
    df_xt["sltb_gboc_ko_nfc"] = normalize_numeric(df_xt["sltb_gboc_ko_nfc"])
    df_xt["sltb_gboc_vung_sau_vung_xa"] = normalize_numeric(df_xt["sltb_gboc_vung_sau_vung_xa"])
    df_xt["sltb_gboc_cmt"] = normalize_numeric(df_xt["sltb_gboc_cmt"])

    # Totals (no kenh filter)
    map_303 = (
        df_xt.groupby("province_code_home", dropna=False)["sltb_xac_thuc_final_giao_gboc"].sum().to_dict()
    )
    # Online (kenh = MYVT)
    df_myvt = df_xt[df_xt["kenh"].str.upper() == "MYVT"]
    map_online_myvt_303 = df_myvt.groupby("province_code_home", dropna=False)["sltb_xac_thuc_final_giao_gboc"].sum().to_dict()

    # Offline (kenh = MBCCS)
    df_mbccs = df_xt[df_xt["kenh"].str.upper() == "MBCCS"]
    map_offline_mbccs_303 = df_mbccs.groupby("province_code_home", dropna=False)["sltb_xac_thuc_final_giao_gboc"].sum().to_dict()

    return {
        "total_303": map_303,
        "online_myvt_303": map_online_myvt_303,
        "offline_mbccs_303": map_offline_mbccs_303,
        "grand_total_303": float(df_xt["sltb_xac_thuc_final_giao_gboc"].sum()),
    }


def build_total_card_summary(df_total: pd.DataFrame, maps: dict) -> dict:
    """Tinh KPI cho the 'Can xac thuc' theo du lieu 30.3tr."""
    df303 = df_total[df_total["Loại"] == "30.3tr"].copy()

    dau_ky = float(normalize_numeric(df303["Tổng giao"]).sum())
    th_ngay_18 = float(normalize_numeric(df303["TH ngày 18"]).sum()) if "TH ngày 18" in df303.columns else 0.0
    th_luy_ke_tu_19_4 = float(maps.get("grand_total_303", 0.0))
    con_phai_th = dau_ky - th_ngay_18 - th_luy_ke_tu_19_4

    return {
        "dau_ky": int(round(dau_ky)),
        "th_1_4_18_4": int(round(th_ngay_18)),
        "th_luy_ke_tu_19_4": int(round(th_luy_ke_tu_19_4)),
        "con_phai_th": int(round(con_phai_th)),
    }


def compute_days_from_start(report_date: date, from_date_str: str) -> int:
    """So ngay tu ngay bat dau (from_date) den report_date, tinh ca hai dau."""
    from_dt = datetime.strptime(from_date_str, "%Y-%m-%d").date()
    return (report_date - from_dt).days + 1


def safe_pct(numerator: float, denominator: float) -> float:
    """Tra ve 0.0 neu denominator = 0."""
    if denominator == 0:
        return 0.0
    return round(numerator / denominator, 6)


def row_num(r: pd.Series, col: str, default: float = 0.0) -> float:
    """Lay gia tri so tu 1 dong, tra default neu cot thieu/NaN."""
    if col not in r.index:
        return default
    val = cast(pd.Series, pd.to_numeric(pd.Series([r[col]]), errors="coerce")).iloc[0]
    if pd.isna(val):
        return default
    return float(val)


def build_t1_rows(df303: pd.DataFrame, days: int) -> list[dict]:
    """Tinh toan cac cot phai sinh cho bang t1 (30.3tr)."""
    rows = []
    for _, r in df303.iterrows():
        province = str(r.get("Tỉnh", "")).strip()
        dau_ky = int(round(row_num(r, "Tổng giao", 0.0)))
        kh_den_ngay = round(row_num(r, "KH ngày", 0.0) * days)
        luy_ke = int(round(row_num(r, "Lũy kế TH", 0.0)))
        online = int(round(row_num(r, "Online", 0.0)))
        offline = int(round(row_num(r, "Offline", 0.0)))
        kh_con_lai = int(round(row_num(r, "KH còn lại", float(dau_ky))))

        pct_th = safe_pct(luy_ke, kh_den_ngay)
        pct_online = safe_pct(online, luy_ke)
        pct_offline = safe_pct(offline, luy_ke)
        kh_thang4 = kh_con_lai - luy_ke
        pct_th_dau_ky = safe_pct(luy_ke, dau_ky)
        can_xac_thuc = dau_ky - luy_ke

        rows.append({
            "province": province,
            "dau_ky": dau_ky,
            "kh_den_ngay": kh_den_ngay,
            "luy_ke": luy_ke,
            "online": online,
            "offline": offline,
            "pct_th": pct_th,
            "pct_online": pct_online,
            "pct_offline": pct_offline,
            "kh_thang4": kh_thang4,
            "pct_th_dau_ky": pct_th_dau_ky,
            "can_xac_thuc": can_xac_thuc,
        })

    # Xep hang theo pct_th giam dan (hang 1 = tot nhat)
    rows.sort(key=lambda x: x["pct_th"], reverse=True)
    for i, row in enumerate(rows):
        row["rank"] = i + 1

    return rows


def build_trend_data(xac_thuc_path: str, trend_from_date: str, t1_worst: list[dict],
                     all_t1_rows: list[dict] | None = None) -> dict:
    """Doc du lieu xac_thuc tu trend_from_date, nhom theo ngay va tinh.
    Tra ve trend cho 10 tinh kem nhat va tat ca tinh (all_provinces)."""
    worst_provinces = [r["province"] for r in t1_worst]

    df = pd.read_csv(
        xac_thuc_path, sep="|", low_memory=False,
        usecols=["ngay", "province_code_home",
                 "sltb_xac_thuc_final_giao_gboc",
                 "sltb_xac_thuc_final_giao_gboc_offline"],
    )
    df["ngay"] = pd.to_datetime(df["ngay"].astype(str), format="%Y%m%d", errors="coerce")
    trend_from_dt = datetime.strptime(trend_from_date, "%Y-%m-%d")
    df = df[df["ngay"] >= trend_from_dt]
    df["province_code_home"] = df["province_code_home"].astype(str).str.strip()

    df["sltb_xac_thuc_final_giao_gboc"] = normalize_numeric(df["sltb_xac_thuc_final_giao_gboc"])
    df["sltb_xac_thuc_final_giao_gboc_offline"] = normalize_numeric(
        df["sltb_xac_thuc_final_giao_gboc_offline"]
    )

    # National totals (all provinces) per day
    national_grp = (
        df.groupby("ngay", dropna=False)
        .agg(national_total=("sltb_xac_thuc_final_giao_gboc", "sum"))
        .reset_index()
        .set_index("ngay")
    )

    grp = (
        df.groupby(["ngay", "province_code_home"], dropna=False)
        .agg(
            total=("sltb_xac_thuc_final_giao_gboc", "sum"),
            offline=("sltb_xac_thuc_final_giao_gboc_offline", "sum"),
        )
        .reset_index()
    )

    all_dates = sorted(grp["ngay"].dropna().unique())
    labels = [f"{d.day}/{d.month}" for d in all_dates]
    national_series = [
        int(national_grp.loc[d, "national_total"]) if d in national_grp.index else 0
        for d in all_dates
    ]

    # 10 worst provinces
    province_list = []
    for r in t1_worst:
        p = r["province"]
        p_df = grp[grp["province_code_home"] == p].set_index("ngay")
        total_series = [int(p_df.loc[d, "total"]) if d in p_df.index else 0 for d in all_dates]
        offline_series = [int(p_df.loc[d, "offline"]) if d in p_df.index else 0 for d in all_dates]
        province_list.append({
            "code": p,
            "rank_t1": r["rank"],
            "pct_th": r["pct_th"],
            "total": total_series,
            "offline": offline_series,
        })

    # All provinces for t4 filter view
    rank_map = {r["province"]: r for r in (all_t1_rows or [])}
    all_province_codes = sorted(p for p in grp["province_code_home"].dropna().unique() if p.lower() != "nan")
    all_province_list = []
    for p in all_province_codes:
        p_df = grp[grp["province_code_home"] == p].set_index("ngay")
        total_series = [int(p_df.loc[d, "total"]) if d in p_df.index else 0 for d in all_dates]
        offline_series = [int(p_df.loc[d, "offline"]) if d in p_df.index else 0 for d in all_dates]
        r = rank_map.get(p, {})
        all_province_list.append({
            "code": p,
            "rank_t1": r.get("rank", "-"),
            "pct_th": r.get("pct_th", 0),
            "total": total_series,
            "offline": offline_series,
        })

    return {
        "labels": labels,
        "provinces": province_list,
        "national": national_series,
        "all_provinces": all_province_list,
    }


def main() -> None:
    args = parse_args()

    # --- Xac dinh ngay bao cao ---
    if args.report_date:
        report_dt = datetime.strptime(args.report_date, "%Y-%m-%d").date()
    else:
        report_dt = date.today() - timedelta(days=1)

    days = compute_days_from_start(report_dt, args.from_date)
    report_date_str = report_dt.strftime("%d/%m/%Y")
    from_date_display = datetime.strptime(args.from_date, "%Y-%m-%d").strftime("%d/%m/%Y")

    # --- B1: Doc f_total, them cot moi ---
    df_total: pd.DataFrame = pd.read_excel(args.f_total)
    df_total["Tỉnh"] = df_total["Tỉnh"].astype(str).str.strip()
    df_total["Loại"] = df_total["Loại"].astype(str).str.strip()

    for col in ["Lũy kế TH", "Online", "Offline"]:
        if col not in df_total.columns:
            df_total[col] = 0

    # --- B2: Tinh Luy ke TH, Online, Offline tu xac_thuc (chi ap dung cho 30.3tr) ---
    maps = build_luy_ke_map(args.xac_thuc, args.from_date)

    df_total.loc[df_total["Loại"] == "30.3tr", "Lũy kế TH"] = (
        df_total.loc[df_total["Loại"] == "30.3tr", "Tỉnh"].map(maps["total_303"]).fillna(0)
    )
    df_total.loc[df_total["Loại"] == "30.3tr", "Online"] = (
        df_total.loc[df_total["Loại"] == "30.3tr", "Tỉnh"].map(maps["online_myvt_303"]).fillna(0)
    )
    df_total.loc[df_total["Loại"] == "30.3tr", "Offline"] = (
        df_total.loc[df_total["Loại"] == "30.3tr", "Tỉnh"].map(maps["offline_mbccs_303"]).fillna(0)
    )

    # --- Ghi Excel ---
    df_total.to_excel(args.output, index=False)
    print(f"Da ghi file: {args.output}")

    # --- B3: Tinh cac cot phai sinh cho table t1 (chi 30.3tr), xuat data.json ---
    df303 = cast(pd.DataFrame, df_total[df_total["Loại"] == "30.3tr"].copy())

    # Tinh so ngay tu 01/05/2026 den report_dt (cho KH den ngay)
    may_1_2026 = date(2026, 5, 1)
    days_from_may_1 = (report_dt - may_1_2026).days + 1

    # T1: 30.3tr
    t1_rows = build_t1_rows(df303, days_from_may_1)
    t1_dat_kh = [r for r in t1_rows if r["pct_th"] >= 1.0][:10]
    t1_thap_nhat = list(reversed(t1_rows))[:10]

    # T3: Trend — 10 tinh kem nhat cua bang 30.3tr tu 15/4, kem all_provinces cho t4
    trend = build_trend_data(args.xac_thuc, "2026-04-15", t1_thap_nhat, all_t1_rows=t1_rows)
    print(f"  [T3] So ngay xu huong: {len(trend['labels'])} | So tinh: {len(trend['provinces'])}")

    total_card = build_total_card_summary(df_total, maps)

    data = {
        "report_date": report_date_str,
        "from_date": from_date_display,
        "days_from_start": days,
        "summary": {
            "total_card": total_card,
        },
        "t1": {
            "total_provinces": len(t1_rows),
            "all_rows": t1_rows,
            "dat_kh": t1_dat_kh,
            "thap_nhat": t1_thap_nhat,
        },
        "trend": trend,
    }

    # T2 stub for backward compatibility (20.4tr removed)
    data["t2"] = {
        "total_provinces": 0,
        "all_rows": [],
        "dat_kh": [],
        "thap_nhat": [],
    }

    with open(args.json_output, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"Da ghi file: {args.json_output}")
    print(f"Ngay bao cao: {report_date_str} | So ngay tu {from_date_display}: {days}")
    print(
        "  [CARD] Dau ky={dau_ky:,} | TH 1/4-18/4={th_1_4_18_4:,} | TH tu 19/4={th_luy_ke_tu_19_4:,} | Con phai TH={con_phai_th:,}".format(
            **total_card
        )
    )
    print(f"  [T1] Tinh dat KH den ngay (>=100%): {len(t1_dat_kh)}")


if __name__ == "__main__":
    main()