# Khai báo sử dụng AI

**Công cụ:** Claude Code (Anthropic), chạy trong VS Code trên máy cá nhân.

**Phạm vi hỗ trợ:**

- Cài `.venv`, cài dependencies và chuyển notebook `.py` sang `.ipynb` bằng jupytext.
- Chạy các lệnh của lab trên máy tôi: sinh dữ liệu, `verify_lite.py`, `pytest`, `run_all.py`,
  thực thi notebook bằng `nbconvert` để lưu output.
- Thêm cell chỉ đọc ở NB1 để in `_delta_log/` và nội dung commit JSON.
- Soạn nháp phần "Giải thích kết quả" trong từng notebook, `INFO.md` và `REFLECTION.md` dựa trên output thật.
- Viết script render screenshot từ output đã lưu của notebook (Chrome headless).

**Không dùng AI để:** tạo số liệu hay output giả, bỏ assertion hoặc hạ ngưỡng. Mọi số liệu trong bài
đến từ các lần chạy notebook trên máy tôi.

**Tự kiểm tra:** tôi đã đọc lại phần giải thích, đối chiếu với output và RUBRIC.md, và chịu trách nhiệm
giải thích mã nguồn cùng kết quả.
