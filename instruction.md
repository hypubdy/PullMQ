Tôi sẽ giải thích theo từng tính năng và từng lệnh Redis được sử dụng để bạn hiểu chính xác nó đang làm gì.

# 1. Thêm job vào group

Giả sử:

```text
group = tenant1:contact123
jobId = job-001
```

Lệnh:

```redis
RPUSH queue:group:tenant1:contact123 job-001
```

Ý nghĩa:

```text
queue:group:tenant1:contact123
┌─────────┐
│ job-001 │
└─────────┘
```

Nếu thêm tiếp:

```redis
RPUSH queue:group:tenant1:contact123 job-002
RPUSH queue:group:tenant1:contact123 job-003
```

Kết quả:

```text
queue:group:tenant1:contact123

job-001
job-002
job-003
```

Đây là queue FIFO của riêng group này.

---

# 2. Đưa group vào danh sách active

Lệnh:

```redis
RPUSH groups:active tenant1:contact123
```

Ý nghĩa:

```text
groups:active

tenant1:contact123
```

Scheduler sẽ nhìn vào queue này để biết group nào đang có việc cần xử lý.

Nếu có thêm:

```redis
RPUSH groups:active tenant1:contact456
RPUSH groups:active tenant2:contact111
```

Kết quả:

```text
groups:active

tenant1:contact123
tenant1:contact456
tenant2:contact111
```

---

# 3. Scheduler lấy group tiếp theo

Lệnh:

```redis
LPOP groups:active
```

Redis trả về:

```text
tenant1:contact123
```

và queue trở thành:

```text
groups:active

tenant1:contact456
tenant2:contact111
```

Ý nghĩa:

```text
Scheduler chọn group tiếp theo để cấp phát job.
```

---

# 4. Lock group

Lệnh:

```redis
SET lock:group:tenant1:contact123 worker-1 NX EX 300
```

Giải thích từng phần:

| Thành phần                    | Ý nghĩa                  |
| ----------------------------- | ------------------------ |
| SET                           | tạo key                  |
| lock:group:tenant1:contact123 | key lock                 |
| worker-1                      | worker đang giữ lock     |
| NX                            | chỉ tạo nếu chưa tồn tại |
| EX 300                        | tự hết hạn sau 300 giây  |

Nếu thành công:

```text
OK
```

Nếu thất bại:

```text
(nil)
```

Ví dụ:

Worker 1:

```redis
SET lock:group:tenant1:contact123 worker-1 NX EX 300
```

→ thành công.

Worker 2:

```redis
SET lock:group:tenant1:contact123 worker-2 NX EX 300
```

→ thất bại.

Điều này đảm bảo:

```text
Chỉ một worker xử lý contact123 tại một thời điểm.
```

---

# 5. Lấy job đầu tiên trong group

Lệnh:

```redis
LPOP queue:group:tenant1:contact123
```

Redis trả về:

```text
job-001
```

Queue còn lại:

```text
job-002
job-003
```

Ý nghĩa:

```text
Lấy job đầu tiên theo FIFO.
```

---

# 6. Đẩy sang hàng đợi thực thi

Lệnh:

```redis
RPUSH queue:ready job-001
```

Queue:

```text
queue:ready

job-001
```

Đây là queue chung cho toàn hệ thống.

---

# 7. Worker lấy job

Lệnh:

```redis
BRPOP queue:ready 0
```

Giải thích:

| Thành phần  | Ý nghĩa            |
| ----------- | ------------------ |
| BRPOP       | Blocking Right Pop |
| queue:ready | queue cần lấy      |
| 0           | chờ vô hạn         |

Nếu queue rỗng:

```text
worker ngủ
```

Nếu có job:

```text
job-001
```

worker bắt đầu xử lý.

---

# 8. Kiểm tra group còn job không

Lệnh:

```redis
LLEN queue:group:tenant1:contact123
```

Ví dụ trả về:

```text
2
```

nghĩa là:

```text
job-002
job-003
```

vẫn còn.

---

# 9. Đưa group xuống cuối hàng

Lệnh:

```redis
RPUSH groups:active tenant1:contact123
```

Queue:

```text
tenant1:contact456
tenant2:contact111
tenant1:contact123
```

Kết quả:

```text
contact123 sẽ được xử lý lại sau khi các group khác có cơ hội chạy.
```

Đây chính là round-robin scheduling.

---

# 10. Worker hoàn thành job

Lệnh:

```redis
DEL lock:group:tenant1:contact123
```

Ý nghĩa:

```text
contact123 có thể nhận job tiếp theo.
```

---

# Group Concurrency

Ví dụ:

```text
Cho phép tối đa 3 job cùng lúc trong một group.
```

## tăng số lượng worker đang chạy

```redis
INCR running:tenant1:contact123
```

Nếu:

```text
0 -> 1
1 -> 2
2 -> 3
```

vẫn cho phép chạy.

Nếu:

```text
3 -> 4
```

thì từ chối.

---

## worker hoàn thành

```redis
DECR running:tenant1:contact123
```

Ví dụ:

```text
3 -> 2
```

cho phép worker khác vào.

---

# Group Rate Limit

Ví dụ:

```text
100 request/phút
```

Lệnh:

```redis
INCR rate:tenant1
```

lần đầu:

```text
1
```

---

Thiết lập TTL:

```redis
EXPIRE rate:tenant1 60
```

sau 60 giây Redis tự xóa key.

---

Nếu:

```redis
GET rate:tenant1
```

trả về:

```text
101
```

thì:

```text
tenant đã vượt quota.
```

---

# Delayed Job

Thêm job chạy sau 5 phút:

```redis
ZADD delayed_jobs 1782813600 job-001
```

Giải thích:

| Thành phần | Ý nghĩa             |
| ---------- | ------------------- |
| ZADD       | thêm vào sorted set |
| 1782813600 | thời điểm chạy      |
| job-001    | job id              |

---

Lấy job đã đến thời gian chạy:

```redis
ZRANGEBYSCORE delayed_jobs -inf 1782813600
```

Ý nghĩa:

```text
lấy toàn bộ job có timestamp <= hiện tại.
```

---

# Retry

Worker bắt đầu xử lý:

```redis
SET processing:job-001 worker-1 EX 300
```

Nếu worker chết:

sau 300 giây:

```text
processing:job-001
```

biến mất.

Scheduler phát hiện:

```text
job đang mất worker xử lý
```

và:

```redis
RPUSH queue:ready job-001
```

để retry.

---

# Priority

Thay vì LIST:

```redis
RPUSH queue job
```

sử dụng:

```redis
ZADD queue 1 job-high
ZADD queue 10 job-low
```

Lấy job:

```redis
ZRANGE queue 0 0
```

Kết quả:

```text
job-high
```

vì score thấp hơn.

---

# Tóm tắt

| Tính năng         | Lệnh Redis chính     |
| ----------------- | -------------------- |
| FIFO queue        | RPUSH + LPOP         |
| Group scheduling  | RPUSH + LPOP         |
| Group lock        | SET NX EX            |
| Group concurrency | INCR + DECR          |
| Rate limit        | INCR + EXPIRE        |
| Delay             | ZADD + ZRANGEBYSCORE |
| Retry             | SET EX + TTL         |
| Priority          | ZADD + ZRANGE        |
| Scale worker      | BRPOP                |
| Release lock      | DEL                  |

