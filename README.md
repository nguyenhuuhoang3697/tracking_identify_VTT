**Dashboard Xác Thực — README**

- **Mô tả:** Bộ công cụ tạo dashboard `report.html` từ dữ liệu `f_total.xlsx` (KPI) và file xác thực pipe-delimited (`xac_thuc_...txt`). Script chính là `luong_du_lieu.py` — sinh `f_total_cap_nhat.xlsx` và `data.json` mà `report.html` sử dụng.

**Yêu cầu**
- Python 3.8+ (khuyến nghị 3.11)
- Thư viện: `pandas`, `openpyxl`

**Chuẩn bị môi trường (Bash)**
```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install pandas openpyxl
```

**Chuẩn bị môi trường (PowerShell / Windows)**
```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1   # hoặc use activate.bat
python -m pip install --upgrade pip
pip install pandas openpyxl
```

**Chạy pipeline (1 lệnh)**
- Dùng script tiện ích: `run_pipeline.sh` (bash)
```bash
./run_pipeline.sh --from-date 2026-04-15 --report-date 2026-04-22
```
- Hoặc chạy trực tiếp (Windows PowerShell):
```powershell
.\.venv\Scripts\python.exe luong_du_lieu.py --from-date 2026-04-15 --report-date 2026-04-22
```

**Phục vụ trang báo cáo (local)**
- Dùng `server.py` (nếu có):
```powershell
.\.venv\Scripts\python.exe server.py
# mở http://localhost:8000/report.html
```
- Hoặc Python built-in simple server:
```bash
python -m http.server 8000 --directory .
```

**Các file chính**
- `luong_du_lieu.py`: pipeline chính — đọc `f_total.xlsx` và file xác thực, tính toán, xuất `f_total_cap_nhat.xlsx` và `data.json`.
- `report.html`: dashboard, đọc `data.json` và hiển thị 3 panel (T1, T2, Xu hướng).
- `server.py`: server tĩnh (UTF-8) phục vụ `report.html`.
- `data.json`: dữ liệu đầu ra mà `report.html` tiêu thụ.
- `run_pipeline.sh`: script tiện ích để chạy pipeline nhanh.

**Phần Xu hướng (Trend)**
- Bắt đầu từ `2026-04-15` (có thể thay `from-date` khi chạy pipeline).
- Hiển thị 10 tỉnh kém nhất theo kênh Offline (20.4tr).
- Biểu đồ: Line = `sltb_xac_thuc_final_giao_gboc` (Tổng), Bar = `sltb_xac_thuc_final_giao_gboc_offline` (Offline).

**Ghi chú vận hành**
- Nếu có dữ liệu mới: chạy lại `run_pipeline.sh` hoặc `luong_du_lieu.py` để cập nhật `data.json`.
- Sau cập nhật, reload `report.html` trên trình duyệt để xem đồ thị mới.

Nếu muốn, tôi sẽ thêm `run_pipeline.bat` / PowerShell wrapper hoặc `requirements.txt`. Bạn muốn tiếp theo gì?