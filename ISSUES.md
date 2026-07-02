# PullMQ — Các issue còn tồn đọng sau đợt fix v1.0.1

> Tài liệu này ghi lại các lỗi còn sót sau hai commit fix (`08b1136`, `eef3131`),
> được phát hiện trong đợt review ngày 2026-07-02. Ba issue đầu đã được **xác nhận
> bằng thực nghiệm** trên Redis thật; các issue còn lại là crash-window xác định
> qua phân tích code (không thể tái hiện bằng test thông thường vì cần kill process
> hoặc lỗi mạng đúng thời điểm).
>
> Bối cảnh chung: các fix v1.0.1 đã xử lý đúng những race/leak mà chúng nhắm tới,
> nhưng **cùng một lớp lỗi vẫn còn ở các code path song song** mà đợt fix không
> quét tới. Các issue dưới đây xếp theo mức độ nghiêm trọng.

> **CẬP NHẬT 2026-07-02: TẤT CẢ ĐÃ ĐƯỢC SỬA.** Cả 7 issue và 4 ghi chú phụ đã
> được sửa theo đúng hướng khuyến nghị của từng mục (Issue 1: A · 2: A · 3: A ·
> 4: A · 5: A · 6: A · 7: A · P1–P4: như bảng phụ lục; riêng P3 sửa bằng
> `saveIfExists` — HSET có điều kiện qua Lua — thay vì tombstone). Ba kịch bản
> tái hiện đã được chuyển thành regression test trong `test/`; script kiểm chứng
> chạy lại xác nhận cả ba không còn tái hiện. Trong lúc sửa còn phát hiện và vá
> thêm một bug thứ 8: nhánh missing-job của `processJob()` gọi `releaseSlot()`
> hai lần (một lần trực tiếp + một lần trong `finally`), làm `activeCount` âm
> và âm thầm nâng concurrency hiệu dụng của worker.
>
> **Lưu ý triển khai:** pickup atomic dùng `LMOVE`/`BLMOVE` nên PullMQ giờ yêu
> cầu **Redis ≥ 6.2**. Phần nội dung bên dưới giữ nguyên làm hồ sơ phân tích.

> **CẬP NHẬT 2 (soak test kill worker, 2026-07-03):** soak đa tiến trình
> (`examples/06-soak-kill.ts` — 4–6 worker process, SIGKILL ngẫu nhiên mỗi 2–4s,
> ~35.000 job hỗn hợp/lượt) phát hiện thêm **4 lỗi cùng họ "claim-rồi-crash"**
> mà review tĩnh bỏ sót, tất cả đã sửa theo nguyên tắc *mọi bước chuyển
> "gỡ khỏi A + gắn vào B" phải là một script Lua duy nhất*:
>
> 1. **Cascade false-stall**: stalled-checker quét trúng khe hở pickup→lock →
>    tạo bản sao job đang chạy; hai bản chung một lock key nên `cleanup()` DEL
>    lock của nhau → cascade re-run + rò `running` counter. Sửa: (a) checker
>    chỉ thu hồi job không-lock qua **2 lần quét liên tiếp** (grace pass);
>    (b) release lock bằng **compare-and-delete theo token**; (c) dispatch gate
>    bằng **HSETNX** trên job-map — bản sao trùng id bị drop thay vì INCR đôi.
> 2. **Mất job khi kill giữa hoàn tất**: LREM `:active` và ZADD
>    completed/failed/delayed là các round-trip rời. Sửa: script
>    `pmqFinishJob` — detach + release slot + attach đích trong một lệnh, dùng
>    cho *mọi* đường ra khỏi `:active` (hoàn thành, thất bại, retry, rate-limit,
>    stalled-reclaim, abandon).
> 3. **Mất job khi kill giữa promotion**: ZREM claim rồi mới enqueue. Sửa:
>    script `pmqPromoteJob` — claim + route + defer-maxSize atomic (đóng luôn
>    nhược điểm đã ghi ở Issue 2 hướng A).
> 4. **Mất job khi kill giữa pop và dispatch** trong scheduler. Sửa: script
>    `pmqPopDispatch` — pop (zset ưu tiên rồi list) + trần concurrency + INCR +
>    job-map + RPUSH `:ready` trong một lệnh; rate-limit slot được trừ *trước*
>    khi pop nên không còn trạng thái "đã pop chưa dispatch".
>
> Kết quả sau sửa: 2 lượt soak (180s/4 worker/51 kill và 120s/6 worker/34 kill,
> tổng ~70.000 job) **pass cả 7 bất biến**: 0 job mất, drain sạch, mọi counter
> về 0, job-map rỗng, không job nào vừa completed vừa failed. Re-run do kill
> (at-least-once) ở mức 31–33 lần/lượt — đúng ngữ nghĩa. Hiệu năng group tăng
> tiếp: ~945 jobs/s (so với ~700 trước toàn bộ đợt fix, ~800 sau vòng 1).

---

## Issue 1 — `Queue.promoteJobs()` phá vỡ toàn bộ group routing và vẫn còn race double-enqueue

**Mức độ:** Cao · **Trạng thái:** Đã tái hiện được
**Vị trí:** `src/queue.ts:331-343`

### Phân tích

`Queue.promoteJobs()` là API admin để promote thủ công các delayed job đã đến hạn.
Hiện tại nó làm hai việc sai cùng lúc:

```ts
const pipe = this.client.pipeline();
for (const id of ids) {
  pipe.zrem(`${this.keyPrefix}:delayed`, id);
  pipe.rpush(`${this.keyPrefix}:ready`, id);   // ← đẩy thẳng vào :ready, bất kể job thuộc group nào
}
await pipe.exec();                              // ← không kiểm tra kết quả ZREM
```

1. **Bỏ qua routing**: group job bị đẩy thẳng vào `:ready` — lách qua FIFO của
   group, group concurrency, `maxSize`, và cả intra-group priority. Job có
   `priority` (không group) cũng bị đẩy vào `:ready` thay vì zset `:priority`.
2. **Race double-enqueue kiểu C2**: không kiểm tra kết quả `ZREM`, nên nếu
   promoter của một Worker (chạy mỗi 1 giây) claim job cùng lúc, **cả hai bên đều
   enqueue** — job chạy hai lần. Đây chính là lỗi C2 đã fix ở `promoteDelayedJobsOnce()`
   (worker.ts) nhưng fix không được áp cho path này.

Thực nghiệm xác nhận: group có `maxSize: 1` đã đầy, gọi `promoteJobs()` vẫn đẩy
job vào `:ready` — vượt cả maxSize lẫn thứ tự FIFO của group.

### Hướng sửa

**Hướng A — Trích logic promotion của Worker thành module dùng chung.**
Tách phần thân `promoteDelayedJobsOnce()` (claim bằng kết quả ZREM → load job →
route theo group/priority → check maxSize qua `LUA_GROUP_ENQUEUE`) ra một hàm
`promoteDueJobs(client, keyPrefix, ids)` trong module riêng (vd. `src/promotion.ts`),
cả `Worker` lẫn `Queue.promoteJobs()` cùng gọi.

- ✅ Một nguồn sự thật duy nhất — chính vì duplicate logic mà lỗi này tồn tại;
  sửa một lần, cả hai path cùng đúng mãi về sau.
- ✅ `promoteJobs()` được luôn cả claim semantics lẫn maxSize enforcement.
- ❌ Refactor đụng vào `worker.ts` đang chạy ổn — cần chạy lại toàn bộ test race.
- ❌ Hàm dùng chung cần nhận đủ context (prefix, tên queue, client) — chữ ký hơi cồng kềnh.

**Hướng B — Sửa tại chỗ trong `promoteJobs()`.**
Giữ nguyên Worker; trong `promoteJobs()` kiểm tra kết quả từng `ZREM`, với id nào
claim được thì load job và route đúng (copy logic từ worker).

- ✅ Diff nhỏ, khoanh vùng, không đụng Worker.
- ❌ Nhân đôi logic routing — chính là pattern đã sinh ra lỗi này; hai bản copy
  sẽ lại trôi dạt khác nhau ở lần sửa sau.
- ❌ Vẫn phải copy cả phần xử lý maxSize-đầy (defer) — dễ sót.

**Hướng C — Ủy quyền cho `Job.promote()`.**
`promoteJobs()` chỉ lấy danh sách id đến hạn rồi gọi `getJob(id)` → `job.promote()`
từng job (sau khi `Job.promote()` được sửa atomic — xem Issue 2).

- ✅ Tái sử dụng API public sẵn có, code `promoteJobs()` còn ~5 dòng.
- ✅ Sửa Issue 2 xong là Issue 1 "tự lành" phần race.
- ❌ N+1 round-trip (HGETALL mỗi job) — chậm với batch lớn (hàng nghìn delayed job).
- ❌ Phụ thuộc thứ tự sửa: phải xong Issue 2 trước, và `Job.promote()` hiện
  **không check maxSize** — phải bổ sung thêm, không thì chỉ đổi lỗi này lấy lỗi khác.

**Khuyến nghị:** Hướng A — lớp lỗi này tồn tại vì logic promotion bị rải ở 3 nơi
(`Worker`, `Queue.promoteJobs`, `Job.promote`); gom về một chỗ là cách duy nhất
triệt để.

---

## Issue 2 — `Job.promote()` double-enqueue khi gọi đồng thời

**Mức độ:** Trung bình · **Trạng thái:** Đã tái hiện được (job xuất hiện 2 lần trong `:ready`)
**Vị trí:** `src/job.ts:148-161`

### Phân tích

```ts
async promote(): Promise<void> {
  const score = await this.client.zscore(..., this.id);   // (1) kiểm tra
  if (score === null) return;
  await this.client.zrem(..., this.id);                    // (2) xóa — KHÔNG check kết quả
  // (3) enqueue vào group list / priority zset / :ready
}
```

Hai lời gọi đồng thời (hai process admin, hoặc admin + dashboard) cùng qua bước (1)
trước khi bên kia kịp (2). `ZREM` của bên đến sau trả về 0 nhưng không ai kiểm tra,
nên **cả hai cùng enqueue** → job chạy hai lần. Đúng mẫu lỗi C2 đã fix ở Worker
nhưng sót path này. Ngoài ra `promote()` cũng **không check group maxSize** khi đưa
job vào group list (cùng họ với Issue 1).

### Hướng sửa

**Hướng A — Gate trên kết quả ZREM (mẫu claim của Worker).**

```ts
const removed = await this.client.zrem(..., this.id);
if (removed === 0) return;   // bên khác đã claim
```

- ✅ Sửa 2 dòng, đồng nhất với pattern claim đã dùng ở `promoteDelayedJobsOnce()`.
- ✅ Không đổi hành vi với caller đơn lẻ.
- ❌ Vẫn còn cửa sổ crash giữa ZREM và enqueue → job mất nếu process chết đúng lúc
  (ngang bằng với Worker hiện tại, không tệ hơn — nhưng không triệt để hơn).
- ❌ Chưa giải quyết chuyện thiếu check maxSize.

**Hướng B — Script Lua atomic: ZREM + route trong một lệnh.**
Viết `LUA_PROMOTE`: nhận keys (`:delayed`, group list, group priority zset,
`:priority`, `:ready`) và args (jobId, groupId, groupPriority, priority, maxSize);
script ZREM, nếu trả 0 thì dừng, nếu 1 thì check maxSize và push đích đúng.

- ✅ Đóng **cả hai** lỗ: double-enqueue và crash-window mất job.
- ✅ Script này tái dùng được cho `promoteDelayedJobsOnce()` của Worker và
  `Queue.promoteJobs()` — sửa cả họ lỗi promotion trong một mũi tên (kể cả Issue 1).
- ❌ Logic routing nằm trong Lua khó đọc/khó test hơn TypeScript; opts của job
  phải truyền qua ARGV (JS đọc hash trước rồi truyền vào) — vẫn cần một lần đọc
  hash ngoài script.
- ❌ Nhiều key động (group list theo groupId) — phải tính key ở JS, script dài.

**Hướng C — Khóa phân tán quanh promote (`SET NX` theo jobId).**

- ✅ Giữ toàn bộ logic ở JS, dễ đọc.
- ❌ Thêm 2 round-trip mỗi lần promote; phải xử lý lock expiry.
- ❌ Không đóng crash-window; quá nặng so với vấn đề chỉ cần một ZREM có điều kiện.

**Khuyến nghị:** Hướng A ngay lập tức (2 dòng, rủi ro ~0), nâng cấp lên Hướng B
khi làm Issue 1 theo hướng gom module — hai việc dùng chung một script.

---

## Issue 3 — Add bị từ chối vì `maxSize` để lại job hash mồ côi

**Mức độ:** Thấp (rò bộ nhớ tích lũy) · **Trạng thái:** Đã tái hiện được
**Vị trí:** `src/queue.ts:61-62`

### Phân tích

```ts
await job.save();        // (1) HSET job:{id} — hash đã nằm trong Redis
await this.enqueue(job); // (2) Lua check maxSize → throw GroupMaxSizeExceededError
```

Khi (2) throw, hash tạo ở (1) **không được dọn** — không nằm trong list/zset nào,
không TTL, không gì tham chiếu. Hệ thống dùng `maxSize` làm backpressure (từ chối
thường xuyên là hành vi chủ đích) sẽ rò một hash mỗi lần từ chối, tích lũy vô hạn.
`obliterate()` là thứ duy nhất dọn được.

### Hướng sửa

**Hướng A — Dọn dẹp trong nhánh lỗi (compensating delete).**

```ts
try { await this.enqueue(job); }
catch (err) {
  if (err instanceof GroupMaxSizeExceededError) await this.client.del(jobKey);
  throw err;
}
```

- ✅ Diff nhỏ nhất, không đổi thứ tự thao tác, không ảnh hưởng path thành công.
- ✅ Chi phí (1 DEL) chỉ trả trên path bị từ chối.
- ❌ Vẫn còn cửa sổ crash tí hon giữa save và del (rò 1 hash nếu process chết đúng lúc) —
  nhỏ hơn hiện tại rất nhiều nhưng không phải zero.

**Hướng B — Đảo thứ tự: admission trước, save sau.**
Gọi `enqueue()` (check maxSize + push id) trước, `job.save()` sau.

- ✅ Không bao giờ tạo hash cho job bị từ chối; không thêm round-trip.
- ❌ **Nguy hiểm**: mở ra cửa sổ id-nằm-trong-group-list-mà-hash-chưa-tồn-tại.
  Scheduler nhanh tay dispatch được id đó → worker load hash ra null →
  `releaseGroupSlotForMissingJob()` **xóa job khỏi hệ thống** dù add() thành công.
  Đổi lỗi rò bộ nhớ lấy lỗi mất job — tệ hơn.
- ❌ Muốn an toàn phải thêm cơ chế retry-load ở worker → phức tạp lan rộng.

**Hướng C — Gộp save + admission vào một script Lua.**
Script nhận field-value của hash + thông số group: check maxSize, nếu OK thì
HSET hash và RPUSH/ZADD trong cùng một lệnh atomic; nếu đầy trả 0, không ghi gì.

- ✅ Triệt để tuyệt đối: hash và entry trong list sống chết cùng nhau, không còn
  bất kỳ cửa sổ nào (kể cả crash). Còn giảm 1 round-trip mỗi add.
- ✅ Mở rộng script này thêm SADD/RPUSH groups là đóng luôn Issue 6.
- ❌ Thay đổi lớn nhất: serialize hash qua ARGV, script phình to, mọi biến thể
  add (delay/group/priority) phải đi qua script → cần viết lại `enqueue()` và
  test kỹ từng nhánh.

**Khuyến nghị:** Hướng A trước mắt (an toàn, đủ tốt cho tần suất lỗi thực tế),
Hướng C nếu quyết định làm "atomic add" tổng thể cùng Issue 6.

---

## Issue 4 — Crash giữa LPOP `:ready` và RPUSH `:active`: mất job vĩnh viễn + leak group slot

**Mức độ:** Cao (hậu quả nặng, xác suất thấp) · **Trạng thái:** Phân tích code (cần kill process đúng lúc để tái hiện)
**Vị trí:** `src/worker.ts:170` (lpop) / `worker.ts:181-183` (blpop) → `worker.ts:283` (rpush `:active` trong `lockAndActivate`)

### Phân tích

Pickup hiện tại là **hai bước không atomic qua nhiều round-trip**:

```
(1) LPOP :ready → jobId            ← job rời khỏi :ready
    ... processJob() → Job.fromId (HGETALL) ...
(2) SET processing:{id} + RPUSH :active
```

Process chết giữa (1) và (2): jobId không còn trong `:ready`, chưa vào `:active`,
không có processing lock. **Stalled-checker chỉ quét `:active`** nên không bao giờ
thấy nó. Job hash còn nguyên trong Redis nhưng không cấu trúc nào tham chiếu → job
mất vĩnh viễn. Nếu là group job: `running:{groupId}` đã INCR lúc dispatch và entry
`group:job-map` còn đó, nhưng không code path nào gọi `releaseGroupSlotForMissingJob`
cho một id "không nằm đâu cả" → **leak slot đúng kiểu C1**, group kẹt khi đủ số lần.

Đây là phần còn thiếu của các fix v1.0.1: Lua `LUA_DISPATCH_JOB` đã đóng crash-window
phía *dispatch* (scheduler → :ready), nhưng phía *pickup* (:ready → :active) vẫn hở.

### Hướng sửa

**Hướng A — Thay LPOP/BLPOP bằng LMOVE/BLMOVE (atomic move `:ready` → `:active`).**

```
LMOVE :ready :active LEFT RIGHT      (blocking: BLMOVE ... timeout)
```

Job không bao giờ ở trạng thái "không nằm đâu cả": crash sau move thì job nằm trong
`:active` không lock → stalled-checker hiện có sẽ tự re-enqueue nó (cơ chế đã có,
đã test).

- ✅ Đóng window hoàn toàn bằng 1 lệnh Redis; đây là pattern chuẩn của chính
  Bull/BullMQ (RPOPLPUSH/LMOVE). Giữ nguyên FIFO.
- ✅ Không thêm round-trip (thay lệnh, không thêm lệnh); tận dụng nguyên vẹn
  stalled-checker làm lưới an toàn.
- ❌ Yêu cầu Redis ≥ 6.2 cho BLMOVE (hoặc dùng BRPOPLPUSH đã deprecated cho bản cũ).
- ❌ Mở ra khả năng **false-stall hiếm**: job vừa move vào `:active` nhưng chưa kịp
  SET lock trong vài ms, đúng lúc stalled-checker của worker *khác* quét → bị coi là
  stalled, re-enqueue (chạy lặp 1 lần, `stalledCounter` +1). Vô hại về mất mát nhưng
  cần nâng `maxStalledCount` mặc định hoặc set lock TRƯỚC khi move để triệt tiêu.

**Hướng B — Claim list riêng theo worker + heartbeat.**
`BLMOVE :ready :claimed:{workerId}`; worker duy trì key heartbeat có TTL; một
reclaimer định kỳ quét `:claimed:{workerId}` của các worker hết heartbeat và trả
job về `:ready`.

- ✅ Chẩn đoán rõ ràng (biết chính xác worker nào giữ job nào); không false-stall.
- ✅ Nền tảng tốt nếu sau này cần "worker observability" / graceful takeover.
- ❌ Phức tạp hơn hẳn: worker registry, heartbeat, reclaim loop, dọn list của worker
  chết — nhiều trạng thái mới đồng nghĩa nhiều bug mới tiềm năng.
- ❌ Thêm round-trip cho heartbeat; `getJobCounts('active')` phải cộng dồn nhiều list.

**Hướng C — Reconciler định kỳ trên `group:job-map`.**
Job group nào có entry trong `group:job-map` nhưng không nằm trong `:ready`, không
trong `:active`, và không có processing lock trong N giây → coi là mồ côi: trả slot
và re-enqueue vào group list.

- ✅ Không đụng hot path pickup; cùng cơ chế quét này chữa luôn Issue 5 và 6.
- ✅ Là "lưới an toàn tầng hai" đáng có kể cả khi đã làm Hướng A.
- ❌ **Chỉ cứu được group job** — job thường không có record nào ngoài hash, muốn
  quét phải SCAN toàn bộ `job:*` (đắt, O(tổng số job)).
- ❌ Độ trễ phát hiện bằng chu kỳ quét; logic "không nằm đâu cả" cần đọc nhiều key
  mỗi entry (LPOS đắt trên list dài).

**Khuyến nghị:** Hướng A là bắt buộc (đây là lỗi thiết kế pickup, phải sửa tại gốc),
kèm Hướng C như lưới an toàn cho group job nếu muốn phòng thủ theo chiều sâu.

---

## Issue 5 — Group rơi khỏi rotation vĩnh viễn khi lỗi giữa LPOP `groups:active` và push trả lại

**Mức độ:** Cao (hậu quả ngang các leak đã fix) · **Trạng thái:** Phân tích code
**Vị trí:** `src/worker.ts:514-519` (`scheduleGroupBatch`) và các nhánh rpush-trả-lại trong `scheduleOneGroup`

### Phân tích

```ts
const groupId = await this.client.lpop(activeKey);   // group RỜI khỏi rotation
if (!groupId) break;
if (await this.scheduleOneGroup(groupId)) ...        // mọi nhánh trong đây phải tự push trả lại
```

`scheduleOneGroup()` có ~6 nhánh thoát, mỗi nhánh tự chịu trách nhiệm `RPUSH` trả
group về `groups:active`. Nếu một lệnh Redis ở giữa ném lỗi (mạng chớp, failover,
timeout) — hoặc process chết — group đã bị LPOP nhưng chưa được push lại. Khi đó:

- `groups:set` **vẫn chứa** groupId → mọi `queue.add()` sau này cho group đó có
  `SADD` trả 0 → **không bao giờ** push lại vào `groups:active`.
- `checkStalledJobs()` re-enqueue cũng đi qua đúng cổng SADD đó → cũng không cứu được.

→ Group đứng im vĩnh viễn dù job cứ chất thêm vào list. Cách cứu duy nhất hiện tại
là gọi tay `resumeGroup()` (tình cờ có nhánh rpush không qua cổng SADD). Cùng độ
nghiêm trọng với các leak đã fix ở v1.0.1, chỉ khác vector kích hoạt (lỗi I/O thay
vì thứ tự thao tác).

### Hướng sửa

**Hướng A — Xoay vòng không phá hủy: `LMOVE groups:active groups:active LEFT RIGHT`.**
Thay LPOP bằng LMOVE tự xoay (đưa đầu list xuống cuối trong 1 lệnh atomic). Group
**không bao giờ rời** rotation trong lúc được xử lý; chỉ khi scheduler xác nhận
group cạn job mới `LREM` nó ra (kèm SREM groups:set như hiện tại).

- ✅ Loại bỏ window theo thiết kế — không còn trạng thái "đã pop, chưa push lại";
  crash hay lỗi mạng ở bất kỳ điểm nào cũng không làm mất group.
- ✅ Xóa được toàn bộ ~6 chỗ rpush-trả-lại rải rác trong `scheduleOneGroup` — code
  gọn đi đáng kể; đồng thời hết luôn lỗi duplicate-entry khi rate-limit (ghi chú P2).
- ❌ Cần xử lý điểm dừng của một pass cẩn thận (đã có `limit = min(len, 64)` — đi
  đúng số bước là quay đủ một vòng, không lặp vô hạn).
- ❌ LREM O(N) khi gỡ group cạn — không đáng kể vì list ngắn (số group active).

**Hướng B — Reconciler định kỳ `groups:set` ↔ `groups:active`.**
Mỗi X giây: với từng member của `groups:set` có group list/zset không rỗng mà không
xuất hiện trong `groups:active` → RPUSH trả lại.

- ✅ Không đụng hot path; một reconciler dùng chung chữa được Issue 5, 6 và phần
  group của Issue 4.
- ❌ Tự chữa chứ không ngăn chặn — group vẫn đứng im tới chu kỳ quét kế tiếp.
- ❌ SMEMBERS + kiểm tra từng group tốn kém khi số group lớn; race với scheduler
  đang chạy có thể tạo duplicate entry (vô hại nhưng phải chấp nhận).

**Hướng C — try/finally quanh `scheduleOneGroup` trong batch loop.**
Bọc lời gọi: nếu throw thì `finally` RPUSH groupId trả lại.

- ✅ Diff vài dòng, chữa đúng vector "lỗi Redis giữa chừng" (vector dễ xảy ra nhất).
- ❌ **Không** chữa được process crash (kill -9, OOM) — window vẫn còn.
- ❌ Phải cẩn thận không push duplicate khi `scheduleOneGroup` đã tự push trước khi
  throw ở nhánh sau đó — cần cờ theo dõi "đã push chưa" luồn qua các nhánh, dễ sai.

**Khuyến nghị:** Hướng A — nó vừa triệt để vừa *đơn giản hóa* code hiện tại thay vì
chồng thêm cơ chế. Hướng B đáng làm bổ sung nếu triển khai reconciler chung.

---

## Issue 6 — `Queue.add()` group: crash giữa Lua-enqueue và `SADD groups:set` làm group không được lên lịch

**Mức độ:** Trung bình · **Trạng thái:** Phân tích code
**Vị trí:** `src/queue.ts:104-107` (và các chỗ tương tự: `worker.ts` `onFailed`/`onRateLimited`/`checkStalledJobs`/`promoteDelayedJobsOnce`)

### Phân tích

```ts
const ok = await this.client.eval(LUA_GROUP_ENQUEUE, ...);  // (1) job vào group list
const added = await this.client.sadd(`...groups:set`, groupId); // (2)
if (added === 1) await this.client.rpush(`...groups:active`, groupId); // (3)
```

Crash/lỗi giữa (1) và (3): job nằm trong group list nhưng group không có trong
rotation → không được lên lịch. Khác Issue 5 ở chỗ đây tự lành *một phần*: lần
`add()` kế tiếp cho **đúng group đó** sẽ SADD trả 1 và push lại. Nhưng với group
kiểu "mỗi khách hàng một group, thưa thớt" thì lần add kế tiếp có thể không bao giờ
đến — job đầu kẹt vô hạn. Bốn call site tương tự trong `worker.ts` có cùng window.

### Hướng sửa

**Hướng A — Gộp SADD + RPUSH vào chính `LUA_GROUP_ENQUEUE`.**
Script nhận thêm 2 key (`groups:set`, `groups:active`): sau khi push job thành công,
`SADD` và nếu trả 1 thì `RPUSH` — tất cả trong một lệnh atomic.

- ✅ Triệt để cho cả 5 call site (add, onFailed, onRateLimited, stalled re-enqueue,
  delayed promotion) — sửa một script, mọi nơi cùng hưởng.
- ✅ Còn **giảm 2 round-trip** mỗi lần enqueue group job — cải thiện hiệu năng thật.
- ❌ Phải cập nhật chữ ký gọi script ở cả 5 chỗ; script thêm trách nhiệm (vẫn ngắn,
  ~10 dòng Lua).

**Hướng B — Đảo thứ tự: đăng ký rotation trước, enqueue job sau.**
SADD/RPUSH trước rồi mới chạy Lua enqueue. Nếu crash sau bước 1: group nằm trong
rotation với list rỗng → scheduler pop ra, thấy rỗng, tự SREM dọn (nhánh dọn này
đã có sẵn trong `scheduleOneGroup`). Window đổi chiều thành **vô hại**.

- ✅ Không cần sửa Lua, không thêm round-trip; tận dụng nhánh tự dọn sẵn có.
- ❌ Lập luận đúng đắn tinh vi (dựa vào hành vi dọn rác của scheduler) — dễ bị phá
  vỡ âm thầm khi ai đó sửa nhánh dọn; bắt buộc phải có comment + test giữ chỗ.
- ❌ Add bị từ chối maxSize vẫn để lại group rỗng trong rotation một nhịp (vô hại
  nhưng gây nhiễu metric/debug).

**Hướng C — Reconciler định kỳ (chung với Issue 5 Hướng B).**

- ✅ Một cơ chế chữa nhiều issue; không đụng hot path.
- ❌ Chỉ tự chữa theo chu kỳ, không ngăn chặn; thêm timer + chi phí quét.

**Khuyến nghị:** Hướng A — đây là hướng hiếm hoi vừa đúng hơn vừa nhanh hơn, và
nó chuẩn hóa cả 5 call site đang lặp lại 3 lệnh giống hệt nhau.

---

## Issue 7 — `LUA_DISPATCH_JOB` không tự kiểm tra trần concurrency

**Mức độ:** Thấp (chỉ sai khi group lock hết hạn giữa pass) · **Trạng thái:** Phân tích code
**Vị trí:** `src/scripts.ts:26-36`, dùng tại `src/worker.ts:618-626`

### Phân tích

Script dispatch `INCR running:{groupId}` **vô điều kiện**. Trần `maxGroupConcurrency`
chỉ được kiểm tra ở JS *trước* vòng lặp fill-slot, dưới sự bảo vệ của group lock
(`SET NX EX 30`). Nếu một pass bị treo quá 30 giây (GC pause dài, mạng nghẽn), lock
hết hạn, worker khác vào và cả hai cùng dispatch → vượt trần concurrency của group.
Xác suất rất thấp (pass bình thường vài ms) nhưng invariant "không bao giờ vượt
`maxGroupConcurrency`" hiện phụ thuộc vào một giả định thời gian, không phải vào cấu
trúc dữ liệu.

### Hướng sửa

**Hướng A — Đưa ceiling check vào script.**
Truyền `max` qua ARGV: script đọc `running`, nếu `>= max` trả 0 (JS LPUSH job trả
về đầu group list — hoặc để luôn việc trả-lại trong Lua); ngược lại mới INCR.

- ✅ Invariant được bảo đảm bằng atomicity của Redis, không còn phụ thuộc lock TTL;
  diff nhỏ (~4 dòng Lua + xử lý mã trả về).
- ✅ Group lock từ vai trò "bảo đảm đúng đắn" hạ xuống "tối ưu tránh tranh chấp" —
  đúng vai trò của nó.
- ❌ Phải resolve `max` (local override vs default) trước mỗi lần gọi — hiện đã có
  sẵn giá trị này trong `scheduleOneGroup`, nên thực tế không tốn thêm gì.

**Hướng B — Watchdog gia hạn lock trong pass.**
Timer gia hạn `EX` của lock chừng nào pass còn chạy (kiểu lock renewal của job).

- ✅ Không đụng Lua.
- ❌ Thêm timer + round-trip mỗi chu kỳ gia hạn cho mỗi group đang xử lý; và vẫn
  không tuyệt đối (check-rồi-expire ngay sau vẫn khả thi về lý thuyết).
- ❌ Chữa triệu chứng (lock hết hạn) thay vì gốc (invariant không nằm trong Redis).

**Hướng C — Gộp toàn bộ fill-slot vào một script Lua lớn.**
Pop từ priority zset/group list + check trần + dispatch, lặp trong Lua đến khi hết
slot hoặc hết job — một round-trip cho cả pass của một group.

- ✅ Atomic trọn vẹn, không cần group lock cho phần dispatch; giảm mạnh round-trip
  (hiện ~10-13 RTT/group/pass) — thắng lớn về hiệu năng khi nhiều group.
- ❌ Script lớn nhất trong ba hướng: gánh cả rate-limit window (INCR + PEXPIRE +
  logic pushback) — khó test, khó debug; block Redis lâu hơn theo batch (phải cap).
- ❌ Rủi ro regression cao nhất — về bản chất là viết lại trái tim của scheduler.

**Khuyến nghị:** Hướng A — chi phí gần bằng 0 và chuyển invariant về đúng nơi nó
thuộc về. Hướng C chỉ cân nhắc khi hiệu năng scheduler thành vấn đề thực tế.

---

## Phụ lục — Các ghi chú nhỏ (không cần 3 phương án, sửa thẳng khi tiện)

| # | Vấn đề | Vị trí | Gợi ý |
|---|--------|--------|-------|
| P1 | `eval` gửi nguyên văn script mỗi lần gọi thay vì dùng `defineCommand`/EVALSHA của ioredis | mọi chỗ gọi `client.eval(LUA_*)` | Khai báo `defineCommand` một lần trong `createClient()` — tiết kiệm băng thông, không đổi ngữ nghĩa |
| P2 | Duplicate entry trong `groups:active` khi dính rate-limit giữa chừng (push ở nhánh limit + push ở đuôi hàm) — group bị check trùng mỗi vòng tới khi cạn job | `worker.ts:607` + `worker.ts:642` | Tự biến mất nếu làm Issue 5 Hướng A; hoặc thêm cờ `requeued` để chỉ push một lần |
| P3 | `remove()` job đang chạy: worker `job.save()` lúc xong việc **hồi sinh hash đã xóa**, job "đã remove" vẫn hiện trong completed/failed | `queue.ts:285-321` + `worker.ts:234-236` | Cần cơ chế tombstone (vd. SET `removed:{id}` TTL ngắn, worker check trước khi save) hoặc tài liệu hóa rõ là remove không cancel job đang chạy |
| P4 | QueueEvents: nếu chính *error listener* throw thì vẫn giết được vòng đọc (throw thoát khỏi catch ngoài) | `queue-events.ts:83-90` | Bọc `this.emit('error', ...)` trong try/catch rỗng ở cả hai chỗ |

---

*Ba issue 1–3 có script tái hiện tại thời điểm review (xem lịch sử phiên làm việc);
khi sửa nên chuyển các kịch bản đó thành regression test trong `test/`.*
