# Thông tin bài nộp

| Mục | Giá trị |
|---|---|
| Họ tên | Đỗ Thái Sơn |
| MSSV | 2A202603021 |
| Mã bài | K4-Track02-Day18 — Lakehouse Lab |
| Repo | https://github.com/tsun165/K4-Track02-Day18-DoThaiSon-2A202603021-Lakehouse-Lab |
| Đường chạy | **Lightweight** cho cả 8 notebook (NB1–NB4 **không** dùng Spark/Docker) |
| Python | 3.12.9 (`.venv`) |
| Hệ điều hành | Windows 11 Home 10.0.26200, PowerShell |
| Thư viện chính | deltalake 1.6.6 · pyiceberg 0.12.0 · duckdb 1.5.6 · polars 1.44.2 · pyarrow 25.0.1 |

## Kết quả kiểm tra

| Lệnh | Kết quả |
|---|---|
| `scripts/verify_lite.py` | 9/9 checks passed |
| `python -m pytest` | 24 passed |
| `scripts/run_all.py` | 8/8 notebooks PASS |

Notebook trong `notebooks/` được thực thi headless bằng
`jupyter nbconvert --to notebook --execute --inplace`, giữ nguyên output, rồi chép sang
`submission/notebooks/`.

## Thay đổi so với đề

- NB1: thêm cell **2b** chỉ đọc, liệt kê `_delta_log/` và in nội dung commit `00000000000000000000.json`
  để có bằng chứng transaction log theo rubric. Không đổi logic, assertion hay ngưỡng.
- NB1–NB8: thêm một cell Markdown **"Giải thích kết quả"** ở cuối mỗi notebook.

## Screenshots

Ảnh trong `screenshots/` được render từ output đã lưu trong các notebook ở `submission/notebooks/`.
Ảnh chỉ trích các cell chứa kết quả chính, không chỉnh sửa số liệu.

| Ảnh | Nội dung |
|---|---|
| `nb01_delta_log.png` | Danh sách `_delta_log/` và nội dung commit JSON v0 |
| `nb01_schema.png` | Bad write bị chặn, `tier` được thêm qua `schema_mode="merge"`, check NB1 |
| `nb02_optimize.png` | 200 → 55 file, benchmark trước/sau, speedup và pruning 55× |
| `nb03_history_restore.png` | MERGE 100K, time travel v0, RESTORE, history 5 version |
| `nb04_gold.png` | Bronze/Silver row count, bảng Gold 8 ngày × 3 model |
| `nb05_iceberg.png` | Partition spec `day(ts)`, pruning 10×, metadata ratio, field-ID, 2 partition spec |
| `nb06_maintenance_delta.png` | Job 1–5 phía Delta: compaction, clustering, vacuum, orphan, checkpoint |
| `nb06_maintenance_iceberg.png` | Iceberg expiry 20 → 3 snapshot, sweep manifest list, check NB6 |
| `nb07_vectors.png` | Amplification 200×, int8 5.8×, semantic search, recall@10, lifecycle bug, check NB7 |
| `nb08_agents.png` | Silver theo `agent_version`, pin version, MCP mô phỏng, provenance bucket, check NB8 |

## Tài liệu khác

- [REFLECTION.md](REFLECTION.md)
- [AI_USAGE.md](AI_USAGE.md)
