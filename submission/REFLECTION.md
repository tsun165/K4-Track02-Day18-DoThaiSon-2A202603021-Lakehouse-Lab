# Reflection — Anti-pattern: vector index là "nguồn sự thật" thứ hai

**Anti-pattern.** Embedding được đẩy sang một vector DB riêng bằng job sync một chiều (chỉ upsert),
trong khi bảng gốc nằm trên lakehouse. Hai bên dần lệch nhau; lệnh xóa hay bị quên nhất.

**Vì sao tôi dễ gặp.** Tôi quan tâm đến RAG/agent: corpus thay đổi liên tục, có dữ liệu người dùng
và yêu cầu xóa. NB7 tái hiện lỗi này: sau khi xóa 8 doc của `user_042`, lakehouse trả 0 hit nhưng
external index vẫn trả 8 hit, tức nội dung đã xóa vẫn có thể lọt vào prompt RAG.

**Cách phòng tránh.**
1. Coi lakehouse là system-of-record; vector index chỉ là index dẫn xuất, có thể rebuild bất kỳ lúc nào.
2. Với quy mô nhỏ, giữ vector ngay trong bảng (cột embedding, query bằng SQL) để lifecycle đi theo row.
3. Nếu cần index ngoài, cho nó subscribe Change Data Feed và xử lý cả sự kiện **delete**, không chỉ upsert.
4. Kiểm tra định kỳ: tập ID trong index − tập ID trong bảng phải rỗng; kèm retention/VACUUM vì time travel vẫn giữ dữ liệu cũ.

**Sử dụng AI:** xem [AI_USAGE.md](AI_USAGE.md).
