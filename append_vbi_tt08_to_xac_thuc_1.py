from __future__ import annotations

import pandas as pd

TARGET_FILE = "xac_thuc_theo_dinh_nghia_v_xac_thuc_luy_ke_20260421_1.txt"
VBI_FILES = [
    "VBI_TT08_20260501.txt",
    "VBI_TT08_20260502.txt",
]

TARGET_COLUMNS = [
    "ngay",
    "khunggio",
    "province_code_home",
    "kenh",
    "user_name",
    "cmnd_9so",
    "slgiaodich",
    "sltb",
    "sltb_loaitru",
    "sltb_xac_thuc_final",
    "sltb_xac_thuc_final_giao_gboc",
    "sltb_xac_thuc_final_giao_gboc_offline",
    "sltb_gboc_60tuoi",
    "sltb_gboc_ko_nfc",
    "sltb_gboc_vung_sau_vung_xa",
    "sltb_gboc_cmt",
]

VBI_TO_TARGET = {
    "KHUNGGIO": "khunggio",
    "PROVINCE_CODE_HOME": "province_code_home",
    "KENH": "kenh",
    "USER_NAME": "user_name",
    "CMND_9SO": "cmnd_9so",
    "SLGIAODICH": "slgiaodich",
    "SLTB": "sltb",
    "SLTB_LOAITRU": "sltb_loaitru",
    "SLTB_XAC_THUC": "sltb_xac_thuc_final",
    "SLTB_XAC_THUC_GBOC": "sltb_xac_thuc_final_giao_gboc",
    "SLTB_XAC_THUC_GBOC_OFFLINE": "sltb_xac_thuc_final_giao_gboc_offline",
    "SLTB_GBOC_60TUOI": "sltb_gboc_60tuoi",
    "SLTB_GBOC_KO_NFC": "sltb_gboc_ko_nfc",
    "SLTB_GBOC_VUNGSAU_VUNGXA": "sltb_gboc_vung_sau_vung_xa",
    "SLTB_GBOC_CMT": "sltb_gboc_cmt",
}


def load_vbi_as_target_rows(vbi_path: str) -> tuple[pd.DataFrame, str]:
    df = pd.read_csv(vbi_path, sep="|", dtype=str, keep_default_na=False)
    df.columns = [c.strip().upper() for c in df.columns]

    if "PRD_ID" not in df.columns:
        raise ValueError(f"{vbi_path}: missing PRD_ID column")

    dates = sorted({str(x).strip() for x in df["PRD_ID"] if str(x).strip()})
    if len(dates) != 1:
        raise ValueError(f"{vbi_path}: expected 1 date in PRD_ID, got {dates}")
    report_date = dates[0]

    # Build output frame with exact target columns.
    out = pd.DataFrame({"ngay": df["PRD_ID"].astype(str).str.strip()})
    for src, dst in VBI_TO_TARGET.items():
        out[dst] = df[src].astype(str).str.strip() if src in df.columns else ""

    # Ensure exact order and fill missing with empty string.
    for col in TARGET_COLUMNS:
        if col not in out.columns:
            out[col] = ""

    out = out[TARGET_COLUMNS].fillna("")
    return out, report_date


def main() -> None:
    target = pd.read_csv(TARGET_FILE, sep="|", dtype=str, keep_default_na=False)
    target.columns = [c.strip() for c in target.columns]

    if list(target.columns) != TARGET_COLUMNS:
        raise ValueError(
            "Target columns mismatch. Expected exact schema with 16 columns."
        )

    all_new = []
    dates_to_replace: list[str] = []

    for path in VBI_FILES:
        rows, d = load_vbi_as_target_rows(path)
        all_new.append(rows)
        dates_to_replace.append(d)
        print(f"Loaded {path}: {len(rows)} rows for date {d}")

    # Idempotent: remove existing rows of the same dates before appending.
    if dates_to_replace:
        target = target[~target["ngay"].astype(str).str.strip().isin(dates_to_replace)].copy()

    merged = pd.concat([target] + all_new, ignore_index=True)
    merged.to_csv(TARGET_FILE, sep="|", index=False)

    print(f"Replaced/added dates: {sorted(set(dates_to_replace))}")
    print(f"Final rows (without header): {len(merged)}")


if __name__ == "__main__":
    main()
