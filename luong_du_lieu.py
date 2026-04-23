from __future__ import annotations

import argparse
import json
import math
from datetime import date, datetime, timedelta

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
    return pd.to_numeric(series, errors="coerce").fillna(0)


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

    # Totals (no kenh filter)
    map_303 = (
        df_xt.groupby("province_code_home", dropna=False)["sltb_xac_thuc_final_giao_gboc"].sum().to_dict()
    )
    map_204 = (
        df_xt.groupby("province_code_home", dropna=False)["sltb_xac_thuc_final_giao_gboc_offline"].sum().to_dict()
    )

    # Online (kenh = MYVT)
    df_myvt = df_xt[df_xt["kenh"].str.upper() == "MYVT"]
    map_online_myvt_303 = df_myvt.groupby("province_code_home", dropna=False)["sltb_xac_thuc_final_giao_gboc"].sum().to_dict()
    map_online_myvt_204 = df_myvt.groupby("province_code_home", dropna=False)["sltb_xac_thuc_final_giao_gboc_offline"].sum().to_dict()

    # Offline (kenh = MBCCS)
    df_mbccs = df_xt[df_xt["kenh"].str.upper() == "MBCCS"]
    map_offline_mbccs_303 = df_mbccs.groupby("province_code_home", dropna=False)["sltb_xac_thuc_final_giao_gboc"].sum().to_dict()
    map_offline_mbccs_204 = df_mbccs.groupby("province_code_home", dropna=False)["sltb_xac_thuc_final_giao_gboc_offline"].sum().to_dict()

    return {
        "total_303": map_303,
        "total_204": map_204,
        "online_myvt_303": map_online_myvt_303,
        "online_myvt_204": map_online_myvt_204,
        "offline_mbccs_303": map_offline_mbccs_303,
        "offline_mbccs_204": map_offline_mbccs_204,
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


def build_t1_rows(df303: pd.DataFrame, df204: pd.DataFrame, days: int) -> list[dict]:
    """Tinh toan cac cot phai sinh cho bang t1 (30.3tr)."""
    kq_204 = df204.set_index("Tỉnh")["KQ đã TH"].to_dict()

    rows = []
    for _, r in df303.iterrows():
        province = r["Tỉnh"]
        dau_ky = int(r["Tổng giao"])
        kh_den_ngay = round(r["KH ngày"] * days)
        luy_ke = int(r["Lũy kế TH"])
        online = int(r["Online"])
        offline = int(r["Offline"])
        kh_con_lai = int(r["KH còn lại"])
        kq_da_th_204 = int(kq_204.get(province, 0))

        pct_th = safe_pct(luy_ke, kh_den_ngay)
        pct_online = safe_pct(online, luy_ke)
        pct_offline = safe_pct(offline, luy_ke)
        kh_thang4 = kh_con_lai - luy_ke
        pct_th_dau_ky = safe_pct(luy_ke, dau_ky)
        can_xac_thuc = dau_ky - luy_ke - kq_da_th_204

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


def build_t2_rows(df204: pd.DataFrame, days: int) -> list[dict]:
    """Tinh toan cac cot phai sinh cho bang t2 (20.4tr - kenh OFFLINE)."""
    rows = []
    for _, r in df204.iterrows():
        province = r["Tỉnh"]
        dau_ky = int(r["Tổng giao"])
        kh_giao = int(r["KH giao"])          # cot KH giao trong f_total
        kh_den_ngay = round(r["KH ngày"] * days)
        luy_ke = int(r["Lũy kế TH"])
        online = int(r["Online"])
        offline = int(r["Offline"])
        kq_da_th = int(r["KQ đã TH"])

        pct_th = safe_pct(luy_ke, kh_den_ngay)
        pct_online = safe_pct(online, luy_ke)
        pct_offline = safe_pct(offline, luy_ke)
        kh_thang4 = kh_giao - luy_ke
        pct_th_dau_ky = safe_pct(luy_ke, dau_ky)
        can_xac_thuc = dau_ky - luy_ke - kq_da_th

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

    # Xep hang theo pct_th giam dan
    rows.sort(key=lambda x: x["pct_th"], reverse=True)
    for i, row in enumerate(rows):
        row["rank"] = i + 1

    return rows


def build_trend_data(xac_thuc_path: str, trend_from_date: str, t2_worst: list[dict]) -> dict:
    """Doc du lieu xac_thuc tu trend_from_date, nhom theo ngay va tinh,
    cho 10 tinh kem nhat kenh Offline (t2_thap_nhat)."""
    provinces = [r["province"] for r in t2_worst]

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
    df = df[df["province_code_home"].isin(provinces)]

    df["sltb_xac_thuc_final_giao_gboc"] = normalize_numeric(df["sltb_xac_thuc_final_giao_gboc"])
    df["sltb_xac_thuc_final_giao_gboc_offline"] = normalize_numeric(
        df["sltb_xac_thuc_final_giao_gboc_offline"]
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

    province_list = []
    for r in t2_worst:
        p = r["province"]
        p_df = grp[grp["province_code_home"] == p].set_index("ngay")
        total_series = [int(p_df.loc[d, "total"]) if d in p_df.index else 0 for d in all_dates]
        offline_series = [int(p_df.loc[d, "offline"]) if d in p_df.index else 0 for d in all_dates]
        province_list.append({
            "code": p,
            "rank_t2": r["rank"],
            "pct_th": r["pct_th"],
            "total": total_series,
            "offline": offline_series,
        })

    return {"labels": labels, "provinces": province_list}


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
    df_total = pd.read_excel(args.f_total)
    df_total["Tỉnh"] = df_total["Tỉnh"].astype(str).str.strip()
    df_total["Loại"] = df_total["Loại"].astype(str).str.strip()

    for col in ["Lũy kế TH", "Online", "Offline"]:
        if col not in df_total.columns:
            df_total[col] = 0

    # --- B2: Tinh Luy ke TH, Online, Offline tu xac_thuc ---
    maps = build_luy_ke_map(args.xac_thuc, args.from_date)

    df_total.loc[df_total["Loại"] == "30.3tr", "Lũy kế TH"] = (
        df_total.loc[df_total["Loại"] == "30.3tr", "Tỉnh"].map(maps["total_303"]).fillna(0)
    )
    df_total.loc[df_total["Loại"] == "20.4tr", "Lũy kế TH"] = (
        df_total.loc[df_total["Loại"] == "20.4tr", "Tỉnh"].map(maps["total_204"]).fillna(0)
    )
    df_total.loc[df_total["Loại"] == "30.3tr", "Online"] = (
        df_total.loc[df_total["Loại"] == "30.3tr", "Tỉnh"].map(maps["online_myvt_303"]).fillna(0)
    )
    df_total.loc[df_total["Loại"] == "20.4tr", "Online"] = (
        df_total.loc[df_total["Loại"] == "20.4tr", "Tỉnh"].map(maps["online_myvt_204"]).fillna(0)
    )
    df_total.loc[df_total["Loại"] == "30.3tr", "Offline"] = (
        df_total.loc[df_total["Loại"] == "30.3tr", "Tỉnh"].map(maps["offline_mbccs_303"]).fillna(0)
    )
    df_total.loc[df_total["Loại"] == "20.4tr", "Offline"] = (
        df_total.loc[df_total["Loại"] == "20.4tr", "Tỉnh"].map(maps["offline_mbccs_204"]).fillna(0)
    )

    # --- Ghi Excel ---
    df_total.to_excel(args.output, index=False)
    print(f"Da ghi file: {args.output}")

    # --- B3: Tinh cac cot phai sinh cho table t1 va t2, xuat data.json ---
    df303 = df_total[df_total["Loại"] == "30.3tr"].copy()
    df204 = df_total[df_total["Loại"] == "20.4tr"].copy()

    # T1: 30.3tr
    t1_rows = build_t1_rows(df303, df204, days)
    t1_dat_kh = [r for r in t1_rows if r["pct_th"] >= 1.0][:10]
    t1_thap_nhat = list(reversed(t1_rows))[:10]

    # T2: 20.4tr — gom rank_t1 tu ket qua bang 30.3tr
    t2_rows = build_t2_rows(df204, days)
    rank_t1_map = {r["province"]: r["rank"] for r in t1_rows}
    for row in t2_rows:
        row["rank_t1"] = rank_t1_map.get(row["province"], "-")
    t2_dat_kh = [r for r in t2_rows if r["pct_th"] >= 1.0][:10]
    t2_thap_nhat = list(reversed(t2_rows))[:10]

    # T3: Trend — 10 tinh kem nhat kenh Offline tu 15/4
    trend = build_trend_data(args.xac_thuc, "2026-04-15", t2_thap_nhat)
    print(f"  [T3] So ngay xu huong: {len(trend['labels'])} | So tinh: {len(trend['provinces'])}")

    data = {
        "report_date": report_date_str,
        "from_date": from_date_display,
        "days_from_start": days,
        "t1": {
            "total_provinces": len(t1_rows),
            "dat_kh": t1_dat_kh,
            "thap_nhat": t1_thap_nhat,
        },
        "t2": {
            "total_provinces": len(t2_rows),
            "dat_kh": t2_dat_kh,
            "thap_nhat": t2_thap_nhat,
        },
        "trend": trend,
    }

    with open(args.json_output, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"Da ghi file: {args.json_output}")
    print(f"Ngay bao cao: {report_date_str} | So ngay tu {from_date_display}: {days}")
    print(f"  [T1] Tinh dat KH den ngay (>=100%): {len(t1_dat_kh)}")
    print(f"  [T2] Tinh dat KH den ngay (>=100%): {len(t2_dat_kh)}")


if __name__ == "__main__":
    main()