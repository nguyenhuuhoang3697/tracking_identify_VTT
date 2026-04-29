import pandas as pd
cols = ['sltb_gboc_60tuoi', 'sltb_gboc_ko_nfc', 'sltb_gboc_vung_sau_vung_xa', 'sltb_gboc_cmt', 'sltb_xac_thuc_final_giao_gboc_offline']
results = {day: {c: 0 for c in cols} for day in [20260427, 20260428]}
for chunk in pd.read_csv('xac_thuc_theo_dinh_nghia_v_xac_thuc_luy_ke_20260421.txt', sep='|', chunksize=100000):
    chunk = chunk[chunk['ngay'].isin([20260427, 20260428])]
    for day in chunk['ngay'].unique():
        for c in cols:
            results[day][c] += chunk[chunk.ngay == day][c].sum()
for day in [20260427, 20260428]:
    yeu_the_total = results[day]['sltb_gboc_60tuoi'] + results[day]['sltb_gboc_ko_nfc'] + results[day]['sltb_gboc_vung_sau_vung_xa']
    print(f'Day {day}: ' + ', '.join([f'{c}: {results[day][c]}' for c in cols]) + f', yeu_the_total: {yeu_the_total}')
