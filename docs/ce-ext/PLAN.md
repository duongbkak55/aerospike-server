# Kế hoạch bổ sung Authentication, Tombstone (durable delete) và XDR cho Aerospike CE

Base code: `aerospike-server` CE **8.1.2.5** (commit `36bd2da`).

Mục tiêu: có các tính năng đang là Enterprise-only trên bản build CE: **xác thực/phân quyền**,
**tombstone / durable delete** (kèm tomb raider) và **XDR** (replicate sang cluster khác).
Phải giữ tương thích wire protocol để client chính thức (Java/Go/C/Python), `asadm`, `aql`
và `asinfo` dùng được mà không phải sửa.

---

## 0. Quyết định đã chốt (13/10/2026)

| # | Câu hỏi | Quyết định | Ảnh hưởng tới kế hoạch |
|---|---|---|---|
| 1 | Phạm vi Auth | **Chỉ user nội bộ** (internal). | Bỏ LDAP, PKI và Kerberos khỏi lộ trình. |
| 2 | Mô hình XDR | **Active-active.** | Bin-convergence (giải xung đột theo bin bằng LUT và `src-id`) trở thành phần **bắt buộc**, không còn tuỳ chọn. **Cả hai cluster đều phải chạy bản mở rộng.** Xem mục 3.4. |
| 3 | TLS | **Không làm.** | Cách ly bằng hạ tầng mạng (mạng riêng, VPN hoặc WireGuard giữa các DC) là điều kiện bắt buộc khi triển khai. Xem mục 6. |
| 4 | Pháp lý (AGPL, EULA) | **Tạm chưa đánh giá. Owner quyết định trước.** | Trong lúc chờ, vẫn giữ nguyên tắc clean-room ở mục 2 để không tự đóng mất lựa chọn nào. Chưa phát hành binary ra ngoài. |

---

## 1. Kết quả khảo sát base code

### 1.1 Cách server tách CE / EE

Core chung cho cả hai bản. Logic Enterprise nằm sau các **hook function** có cùng chữ ký,
bản CE cài đặt chúng bằng stub trong các file `*_ce.c`. `as/src/Makefile` chỉ đưa các file
`*_ce.c` vào build khi `USE_EE != 1`; bản EE lấy file thật từ một repo khác (`$(EEREPO)`).

```
ifneq ($(USE_EE),1)
  BASE_SOURCES += security_ce.c xdr_ce.c record_ce.c ...
endif
```

Nghĩa là **phần lớn chỗ cắm (hook) đã có sẵn trong core**. Việc chính là cài đặt lại các
hook đó và thêm module mới; không phải viết lại transaction path.

| Tính năng | File stub CE | Hook đã được core gọi |
|---|---|---|
| Auth | `as/src/base/security_ce.c` (239 dòng) | `as_security_check_auth` (thr_tsvc.c:135, batch.c:901, thr_info.c:972), `as_security_check_data_op` (thr_tsvc.c:189/254/264), `as_security_check_info_cmd` (thr_info.c:1102), `as_security_transact` (service.c:1081, `PROTO_TYPE_SECURITY`), `as_security_filter_create/destroy` (per-connection, `fd_h->security_filter`) |
| Tombstone | `as/src/base/record_ce.c`, `as/src/transaction/rw_utils_ce.c`, `delete_ce.c`, `as/src/storage/drv_ssd_ce.c`, `drv_mem_ce.c`, `drv_pmem_ce.c`, `flat_ce.c` | `as_record_is_live` (query, sindex populate, drv), `validate_delete_durability` (write.c:1306, delete.c:216, udf.c:1168), `transition_delete_metadata`, `write_delete_record`, `delete_all_bins`, `drop_local`, `as_record_drop_stats`, `as_record_transition_stats`, `ssd_cold_start_adjust_cenotaph/drop_cenotaphs` |
| XDR | `as/src/base/xdr_ce.c` (162 dòng) | `as_xdr_submit` + `as_xdr_get_submit_info` (write.c:926, record.c:242/349, read_touch.c:493), `as_xdr_ship_check` (write.c:766 – hot key), `as_xdr_read` (thr_tsvc.c:121, `PROTO_TYPE_INTERNAL_XDR`), `as_xdr_init_poll/io_event/timer_event` (service.c:744–776 – async I/O đã gắn vào event loop của service thread), `set_xdr_write` (write.c:1274) |

### 1.2 Những thứ đã có sẵn, dùng lại được

- **Cấu trúc dữ liệu:**
  - `as_index` đã có các bit `tombstone`, `cenotaph`, `xdr_write`, `xdr_tombstone`,
    `xdr_nsup_tombstone`, `xdr_bin_cemetery`.
  - `as_namespace` đã có `n_tombstones`, `n_durable_tombstones`, `n_xdr_tombstones`,
    `tomb_raider_eligible_age`, `tomb_raider_period`, `n_tomb_raider_unmark_threads`,
    `storage_tomb_raider_sleep`, `xdr_bin_tombstone_ttl_ms`.
  - Thống kê ticker/info cho tombstone đã được in sẵn (ticker.c:432, thr_info.c:4114).
- **Config parser** (`cfg.c`) đã parse toàn bộ context `security {}` và `xdr { dc {...} }`,
  cùng các tham số tomb-raider. Hiện tại parser chỉ gọi `cfg_enterprise_only()` để crash.
  Có khoảng 70 chỗ như vậy; YAML schema nằm ở `as/opt/schema/aerospike_config_schema.json`.
- **Cấu hình XDR** đã có đủ struct `as_xdr_dc_cfg` / `as_xdr_dc_ns_cfg` trong `xdr.h`:
  auth-mode, bin-policy, ship-only-specified-sets, write-policy, hot-key-ms, delay, max-throughput…
- **SMD** (System Metadata, đồng bộ toàn cluster và có persist) đã có sẵn module ID
  `AS_SMD_MODULE_SECURITY` và `AS_SMD_MODULE_XDR`. Có thể dùng để lưu user/role và bộ lọc XDR.
- **Phía đích của XDR đã chạy được trên CE:**
  - Write có cờ `AS_MSG_INFO1_XDR` được chấp nhận.
  - `reject-xdr-writes` có hiệu lực.
  - Write mang field `AS_MSG_FIELD_TYPE_LUT` đi qua `forbid_resolve` / `resolve_bin`.
  - Kết luận: **cluster đích có thể vẫn là CE gốc**. Chỉ cluster nguồn cần bản đã mở rộng.
- `modules/common` có `tls_ce.c`, `vault_ce.c`, `secrets_ce.c`, `fips_ce.c`, nên **TLS cũng
  đang là stub**. Hệ quả này quan trọng cho bảo mật của Auth và XDR (xem mục 6).

### 1.3 Những thứ CE hoàn toàn không có

- Lưu trữ user/role, xử lý login và session token, kiểm tra privilege, audit log.
- Đường ghi tombstone: hiện delete là *drop* luôn index entry.
  - `drop_master`: `as_index_delete`.
  - `write_delete_record`: `as_index_delete`.
  - Cold start: bỏ qua record không có bin.
- Tomb raider: thread dọn tombstone, gồm cả bước quét device để tìm bản cũ.
- Toàn bộ XDR phía nguồn: queue, recovery, reader, shipper, cluster discovery phía đích,
  thống kê.

---

## 2. Nguyên tắc triển khai

1. **Clean-room.** Chỉ dùng code CE (AGPL), tài liệu công khai của Aerospike và client
   open-source (Apache-2.0) làm tham chiếu protocol. Không dùng hay dịch ngược binary EE
   (EULA cấm). Không dùng nhãn "Enterprise" hay thương hiệu Aerospike cho bản build.
2. **Tách file, hạn chế sửa core** để rebase lên mỗi bản CE mới cho dễ:
   - Code mới đặt trong `as/src/ext/` (`security_ext.c`, `tomb_ext.c`, `xdr_*.c`…).
   - Makefile thêm cờ `USE_EXT=1`. Khi bật cờ này thì dùng `*_ext.c` thay cho `*_ce.c`
     tương ứng.
   - Core chỉ sửa ở những chỗ bắt buộc (gỡ `cfg_enterprise_only`, các `cf_warning(...
     enterprise-only)` trong `cfg_info.c`), và mỗi chỗ sửa đặt sau `#ifdef AS_EXT`.
3. **Tương thích protocol.** Mọi message security, info command và config phải đúng tên
   và định dạng của bản EE, để tool và client hiện có chạy được.
4. **Thứ tự phụ thuộc:** Hạ tầng → Auth → Tombstone → XDR.
   - XDR cần Auth để login vào cluster đích (`auth-mode internal`).
   - XDR cần tombstone để ship được delete một cách tin cậy, kể cả khi đang recovery.
5. **AGPL-3.0:** nếu cung cấp dịch vụ cho người dùng qua mạng thì phải công bố mã nguồn
   bản sửa đổi (§13). Cần pháp chế xác nhận trước khi đưa vào production.

---

## 3. Thiết kế chi tiết

### 3.1 Authentication & Authorization (RBAC)

**Wire protocol.** Dùng định dạng lấy từ client open-source: header `as_sec_msg` 16 byte
(đã khai báo trong `security.h`), mỗi field có dạng `[len:4][id:1][data]`.

- **Command:**
  - `AUTHENTICATE=0`, `LOGIN=20`
  - `CREATE_USER=1`, `DROP_USER=2`, `SET_PASSWORD=3`, `CHANGE_PASSWORD=4`
  - `GRANT_ROLES=5`, `REVOKE_ROLES=6`, `QUERY_USERS=9`
  - `CREATE_ROLE=10`, `DROP_ROLE=11`, `GRANT_PRIVILEGES=12`, `REVOKE_PRIVILEGES=13`
  - `SET_WHITELIST=14`, `SET_QUOTAS=15`, `QUERY_ROLES=16`
- **Field:**
  - `USER=0`, `PASSWORD=1`, `OLD_PASSWORD=2`, `CREDENTIAL=3`, `CLEAR_PASSWORD=4`
  - `SESSION_TOKEN=5`, `SESSION_TTL=6`
  - `ROLES=10`, `ROLE=11`, `PRIVILEGES=12`, `WHITELIST=13`
  - `READ_QUOTA=14`, `WRITE_QUOTA=15`
- Client gửi `CREDENTIAL` = bcrypt(password, salt cố định của client). Như vậy server không
  cần bcrypt, nhưng **không được lưu nguyên credential**. Server lưu
  `PBKDF2-SHA256(credential, salt ngẫu nhiên)`, dùng OpenSSL vốn đã được link sẵn.

**Thành phần:**

| Thành phần | Thiết kế |
|---|---|
| Kho user/role | SMD module `SECURITY`. Key `U:<user>` lưu `{hash, salt, roles[]}`. Key `R:<role>` lưu `{privs[], whitelist[], r_quota, w_quota}`. SMD tự replicate và persist. Mỗi node giữ cache trong RAM (hash map), rebuild trong callback accept của SMD. |
| Role định sẵn | `read`, `read-write`, `read-write-udf`, `write`, `truncate`, `sys-admin`, `user-admin`, `data-admin`, `udf-admin`, `sindex-admin`, `masking-admin`, `read-masked`, `write-masked`. Map vào bitmask `as_sec_perm` có sẵn. Scope privilege: global, `ns`, hoặc `ns.set`. |
| Bootstrap | Lần đầu start, nếu SMD rỗng thì tạo user `admin` với role `user-admin` + `sys-admin`. Mật khẩu lấy từ `default-password-file` nếu có, nếu không thì là `admin` và in cảnh báo. |
| Session | Token = `HMAC-SHA256(cluster_secret, user ‖ expiry ‖ cred_version)`. `cluster_secret` sinh ngẫu nhiên và phân phối qua SMD, nhờ vậy token hợp lệ trên mọi node mà không cần lưu state. `session-ttl` mặc định 24h. Khi user đổi mật khẩu thì `cred_version` tăng, mọi token cũ mất hiệu lực. |
| Per-connection | `as_security_filter_create` cấp `sec_conn_ctx {user, roles_gen, perms cache}` và gắn vào `fd_h->security_filter`. Khi `roles_gen` đổi (nhận event từ SMD) thì làm mới cache (`as_security_should_refresh` / `as_security_refresh`). |
| Kiểm tra quyền | `check_auth`: connection chưa login thì trả `AS_SEC_ERR_NOT_AUTHENTICATED`. `check_data_op(tr, ns, perm)`: tra theo `ns_ix` và `set_id`. `check_info_cmd`: dùng bảng perm sẵn có trong `thr_info.c`. |
| Allowlist / Quota | Giai đoạn 1b. Allowlist kiểm tra lúc login theo IP. Quota dùng token bucket theo user (`as_security_check_rps` / `done_rps` dành cho query/scan). |
| Audit log | `as_security_log` / `as_audit_log` ghi vào log context `AS_SECURITY` (đã có) theo cờ `report-*` trong `security { log {...} }`. Syslog để sau. |
| Info/config | `as_security_get_config`, `as_security_set_config` (`enable-quotas`, `privilege-refresh-period`, `session-ttl`, `report-*`). |
| Ngoài phạm vi (đã chốt) | LDAP, PKI, Kerberos. Chỉ hỗ trợ user nội bộ. |

**Lưu ý:**
- **Fabric và heartbeat giữa các node không được xác thực**, giống bản EE khi không bật TLS.
  Mạng nội bộ của cluster phải được cách ly.
- Transaction nội bộ (proxy, batch sub, query sub-tx, XDR read) đi qua `from` khác
  `PROTO`. Phải đảm bảo chúng không bị chặn nhầm và cũng không trở thành đường vòng để né
  kiểm tra quyền.
- Tool kiểm tra `edition` (`aerospike_build_type`) để bật hoặc tắt chức năng. Cần kiểm tra
  `asadm` có bật lệnh `manage acl` với chuỗi edition hiện tại hay không. Nếu không thì xem
  lại cách báo edition hoặc feature list (`features_ce.c`).

### 3.2 Tombstone / Durable delete / Tomb raider

**Vấn đề hiện tại (zombie record).** Delete thường chỉ xoá index entry. Bản ghi cũ vẫn còn
trên SSD cho tới khi bị defrag. Vì vậy:

- Sau cold start, record bị đọc lại từ device và "sống lại".
- Khi migrate, node đang giữ bản cũ đẩy record cũ ngược trở lại.

**Thiết kế:**

1. **Đường ghi tombstone** khi `durable_delete` được bật, hoặc khi namespace cấu hình
   luôn dùng durable delete:
   - `validate_delete_durability`: trả `AS_OK` thay vì `AS_ERR_ENTERPRISE_ONLY`.
   - `drop_master`, `delete_master`, và nhánh `is_delete` trong `write.c` / `udf.c`
     không xoá index nữa. Thay vào đó:
     - Đặt `r->tombstone = 1`, đặt `n_bins = 0`.
     - Tăng generation, set LUT.
     - Ghi một **flat record không bin** xuống storage. Driver đã có nhánh
       `as_record_is_binless` ở drv_ssd.c:1313 và drv_mem.c:542.
     - Gỡ record khỏi sindex và set-index.
   - `write_delete_record`, `delete_all_bins`, `transition_delete_metadata`: phân biệt
     giữa *drop* (khi không durable) và *tombstone*.
   - `as_record_is_live()` trả `!r->tombstone`. `as_record_is_binless()` cài đặt thật.
   - Thống kê: `as_record_transition_stats` và `as_record_drop_stats` cập nhật
     `n_objects`, `n_tombstones`, `n_durable_tombstones`, `p->n_tombstones`, và
     `set->n_tombstones`.
2. **Replication và migration:**
   - Replica write gửi record 0 bin kèm cờ tombstone, không gửi lệnh *drop*. Phía replica
     xử lý trong `as_record_replace`.
   - Migration gửi tombstone như record thường. Conflict resolution theo
     generation/LUT, nên tombstone mới hơn sẽ thắng bản live cũ hơn.
   - Duplicate resolution: tombstone là một "version" hợp lệ.
3. **Read path:** kiểm tra mọi `reduce` callback.
   - Read, batch, query, scan, sindex, UDF, nsup, truncate, MRT phải coi tombstone là
     không tồn tại.
   - Read trả `NOT_FOUND`.
   - Ghi đè lên tombstone được tính là create.
   - Nhiều chỗ đã có `as_record_is_live`. Cần rà hết các chỗ còn thiếu.
4. **Cold start** (`drv_ssd.c`, `drv_mem.c`, `drv_pmem.c`):
   - Record không bin trên device được nạp thành index entry có `tombstone = 1`, thay vì
     bị bỏ qua.
   - Cài đặt thật `ssd_cold_start_adjust_cenotaph` và `ssd_cold_start_drop_cenotaphs`.
5. **Tomb raider:** một thread cho mỗi namespace, chạy theo `tomb-raider-period`
   (mặc định 1 ngày):
   1. **Mark.** Chỉ chạy khi không có migration đang diễn ra. Duyệt index tree. Tombstone
      nào có `LUT < now − tomb-raider-eligible-age` thì set `cenotaph = 1`.
   2. **Unmark** (chỉ cho storage persist). Quét lần lượt các write block trên device, có
      sleep `tomb-raider-sleep` giữa các lần đọc. Với mỗi record trên device mà digest
      đang là tombstone đã mark và **không phải** version hiện tại (LUT cũ hơn):
      bỏ mark (`cenotaph = 0`), vì bản cũ vẫn còn và có thể "sống lại" sau cold start.
      Phần đếm `n_tomb_raider_reads` cho ticker đã có sẵn.
   3. **Remove.** Xoá khỏi index những tombstone vẫn còn `cenotaph = 1`. Block của chúng
      sẽ được defrag thu hồi.
   - Với namespace chỉ dùng memory và không persist thì bỏ qua bước 2.
6. **Config:** gỡ `cfg_enterprise_only` cho các tham số sau, đặt default giống bản EE:
   - `tomb-raider-eligible-age` (1 ngày), `tomb-raider-period` (1 ngày)
   - `tomb-raider-unmark-threads`, `tomb-raider-sleep`, `serialize-tomb-raider`

### 3.3 XDR (Cross Datacenter Replication)

**Mô hình:** shipper chạy trong process (in-process), **active-active**, mỗi cluster vừa là
nguồn vừa là đích của cluster kia. Phần ship ở mục này dùng chung cho cả hai chiều. Phần giải
xung đột nằm ở mục 3.4.

```
write/delete trên master ─► as_xdr_submit(digest, lut, set_id, flags)
                               │  (filter set, hot-key dedup)
                               ▼
               per-(DC,ns) Transaction Queue  ──overflow/restart──►  Recovery (reduce partition,
                               │                                      ship LUT > LST)
                               ▼
               internal tx PROTO_TYPE_INTERNAL_XDR ─► as_xdr_read: đọc record dưới lock,
                               │                     dựng as_msg (UPDATE/REPLACE hoặc durable DELETE,
                               │                     INFO1_XDR, field LUT, set, key nếu có)
                               ▼
               Shipper async (poll của service thread: as_xdr_io_event/timer_event)
                               │  connection pool tới từng node đích, login nếu auth-mode=internal,
                               │  partition map đích qua info "partition-generation"/"replicas"
                               ▼
               ACK ─► tiến LST (last-ship-time) theo partition; lỗi ─► retry/backoff; DC down ─► dừng LST
```

| Thành phần | Thiết kế |
|---|---|
| Config/khởi tạo | Cài đặt thật `as_xdr_startup_create_dc`, `_dc_ns_cfg`, `_add_seed`. `as_xdr_link_tls` giữ là no-op vì không làm TLS. Gỡ `cfg_enterprise_only` cho `CASE_XDR_*`. `src-id` **bắt buộc khác nhau** giữa các cluster. |
| Submit | Ring buffer MPSC cho từng cặp (DC, ns), giới hạn bởi `transaction-queue-limit`. Lọc theo `ship-only-specified-sets`, `ignored_sets` và cờ `xdr_write` (chống vòng lặp khi `forward = false`). Áp `delay-ms`. Áp `hot-key-ms`: hash digest→lut, gộp nhiều write liên tiếp trên cùng một key. |
| Hot key | `as_xdr_ship_check` trả `XDR_SHIP_NEAR`/`FAR` khi một key đang được ship, để tránh ship lại liên tục. |
| LST & recovery | Mỗi partition có một LST, là mốc LUT mà mọi thay đổi trước nó đều đã được ship. LST được persist định kỳ qua SMD module `XDR` (hoặc file trong thư mục smd). Khi queue tràn, node restart, partition đổi master, hoặc DC vừa được thêm, chạy `as_index_reduce` trên partition và ship mọi record có `LUT > LST`. Số thread recovery do `n_recovery_threads` quy định. |
| Master-only | Chỉ master của partition mới ship. Khi master thay đổi, master mới chạy recovery từ LST đã replicate. LST nên được gửi kèm replica write hoặc qua SMD theo chu kỳ. Chấp nhận ship trùng lặp (idempotent) thay vì để mất dữ liệu. |
| Reader | Cài đặt `as_xdr_read`: tạo `as_msg` bằng các helper sẵn có trong `proto.c`. Với active-active thì dùng `bin-policy only-changed` và `ship-bin-luts true`. Mỗi op mang LUT của bin (cờ `has_lut` trong `as_msg_op`, đã có sẵn), còn message mang field `AS_MSG_FIELD_TYPE_LUT`. Bin đã bị xoá được ship dưới dạng op delete kèm LUT. Với tombstone của record thì gửi delete kèm cờ durable. |
| Delete | Durable delete ship qua tombstone. Delete thường (không durable) sinh `xdr_tombstone` tạm để kịp ship rồi xoá. Đây là lý do cần làm tombstone trước XDR. Delete do nsup (expire/evict) chỉ ship khi bật `ship-nsup-deletes`. |
| Shipper | I/O non-blocking chạy trên poll của service thread, vì core đã gọi sẵn `as_xdr_init_poll`, `io_event` và `timer_event`. Có connection pool, giới hạn `max-throughput`, timeout và retry. Tự viết client tối giản: login, partition map, write, delete. **Không** nhúng aerospike-client-c in-process, vì tên symbol `as_record`, `as_bin`… trùng với server. |
| Auth tới đích | `auth-mode internal`, `auth-user`, `auth-password-file`, dùng chính protocol ở mục 3.1. Không có TLS, kênh XDR đi qua VPN giữa các DC. |
| Phía đích | Phải là bản mở rộng, vì cần giải xung đột theo bin (mục 3.4). CE gốc chỉ dùng được cho mô hình active-passive. Tài khoản XDR ở đích cần quyền `write`; nếu dùng `reject-non-xdr-writes` thì cấu hình theo namespace. |
| Thống kê / info | `as_xdr_get_stats`: `lag`, `in_queue`, `in_progress`, `success`, `abandoned`, `not_found`, `filtered_out`, `retry_conn_reset`, `retry_dest`, `recoveries`, `recoveries_pending`, `throughput`, `latency_ms`. Thêm `as_xdr_dc_state`, `as_xdr_get_config` / `set_config` (thêm/bớt DC và namespace lúc runtime) và `as_xdr_ticker`. |
| Giai đoạn sau | Filter bằng expression (`as_xdr_set_filter` lưu SMD), compression (cần cài đặt `flat_ce.c` / zstd), connector (HTTP/Kafka), topology nhiều hơn 2 DC (mesh với `forward`). |

Không chọn phương án sidecar (process ngoài dùng client chính thức). Lý do: client không
ghi được LUT và `src-id` theo bin, nên không làm được active-active.

### 3.4 Active-active: giải xung đột theo bin (bin convergence)

**Vấn đề.** Khi hai DC cùng ghi một key:
- Ship ở mức record thì bản đến sau đè toàn bộ bản kia, nên mất update và hai bên không
  hội tụ.
- Ship lại write nhận từ XDR thì sinh vòng lặp.

**Khung có sẵn trong code** (đã kiểm tra):
- `as_bin` có `lut` (40 bit) và `src_id`. Có cờ `xdr_write` theo bin.
- `as_msg_op` có `has_lut`, kèm `as_msg_op_get_lut()`.
- `write.c` gọi `forbid_resolve`, `resolve_bin`, `write_resolved_bin`, `delete_bin` với
  `msg_lut`.
- `storage_rd` có `bin_luts`.
- Định dạng flat có hook `flatten_bin_src_id`, `unpack_bin_src_id` và cờ `xdr_bin_cemetery`.
- Namespace có `conflict-resolve-writes`, `xdr-bin-tombstone-ttl`, `xdr-tomb-raider-period`
  và `n_xdr_tomb_raider_threads`.
- Bin tombstone có `as_bin_is_tombstone` và `as_bin_set_tombstone` (hiện là stub trong
  `bin_ce.c`).

**Quy tắc hội tụ** (giống nhau trên mọi cluster nên kết quả là tất định):
- So sánh cặp `(bin_lut, src_id)`: LUT lớn hơn thắng; nếu LUT bằng nhau thì `src_id` lớn
  hơn thắng.
- Write cục bộ (không qua XDR) dùng `lut = now`, `src_id = 0`, có kiểm tra để LUT không
  bao giờ lùi.
- Write nhận qua XDR: với từng op, so `op_lut` (hoặc `msg_lut` nếu op không có LUT) với LUT
  của bin hiện tại.
  - Op thua thì bỏ qua (`n_won--`).
  - Thua toàn bộ thì trả `AS_ERR_LOST_CONFLICT`. Phía ship coi đây là thành công.
- Xoá bin cũng phải ghi lại LUT, nếu không bản cũ ship đến sau sẽ hồi sinh bin đó. Vì vậy
  xoá bin tạo **bin tombstone** (bin không có giá trị, chỉ giữ LUT và `src_id`).
  - Record chỉ còn bin tombstone gọi là **bin cemetery** (`xdr_bin_cemetery`).
  - **XDR tomb raider** dọn bin tombstone sau `xdr-bin-tombstone-ttl`.
- Xoá cả record (durable delete) tạo tombstone có LUT (mục 3.2) và đi qua cùng quy tắc.
  Write cũ hơn tombstone sẽ thua.

**Việc cần cài đặt:**

| Hook / file | Nội dung |
|---|---|
| `rw_utils_ext.c` | `prepare_bin_metadata` bật `rd->bin_luts` khi `conflict-resolve-writes` hoặc `xdr_ships_bin_luts`. Cài đặt `forbid_resolve`, `resolve_bin`, `write_resolved_bin` (ghi LUT và `src_id` vào bin), `delete_bin` / `udf_delete_bin` (tạo bin tombstone thay vì xoá hẳn), `touch_bin_metadata`, `set_xdr_write` (đánh dấu bin và record do XDR ghi để không ship ngược), `transition_delete_metadata` (bin cemetery). Thêm `udf_resolve_bin`. |
| `bin_ext.c` | Thêm particle/state cho bin tombstone. Cài đặt `as_bin_is_tombstone`, `as_bin_set_tombstone`, `as_bin_empty_if_all_tombstones`. Read path (`as_bin_is_live`) phải ẩn bin tombstone với client. |
| `flat_ext.c` | Lưu và đọc LUT, `src_id`, cờ `xdr_write` của bin (`flatten_bin_*` / `unpack_bin_*`), và extra flags `xdr_tombstone`, `xdr_nsup_tombstone`, `xdr_bin_cemetery`. **Định dạng trên disk phải khớp các trường đã khai báo trong `flat.h`.** |
| `record_ext.c` | Đếm thống kê `n_xdr_bin_cemeteries` và `n_xdr_tombstones`. Thêm tiêu chí resolve record ở chế độ LUT khi bin LUT bằng nhau. |
| `xdr_ext/*` | Reader ship chỉ những bin đã đổi kể từ LST: bin có `lut > LST` và không phải do XDR ghi khi `forward = false`. Gửi kèm LUT của từng bin. |
| XDR tomb raider | Thread cho mỗi namespace, chạy theo `xdr-tomb-raider-period`. Xoá bin tombstone và bin cemetery có tuổi lớn hơn `xdr-bin-tombstone-ttl`, nhưng chỉ khi mọi DC đã ship qua mốc đó (LST của mọi DC lớn hơn LUT của chúng). |
| Migration / replica | Bin LUT, `src_id` và bin tombstone đi kèm trong flat record nên tự được replicate. Cần kiểm tra lại `as_record_replace`, vì ở chế độ bin-convergence thì merge theo bin chứ không replace cả record. |

**Ràng buộc vận hành:**
- **Đồng bộ giờ (NTP/chrony)** giữa các DC là bắt buộc, vì LUT là đồng hồ ms. Lệch giờ làm
  sai thứ tự giữa các write gần nhau.
- Đặt ngưỡng cảnh báo lệch giờ. Có thể dùng `as_record_handle_clock_skew` và
  `clock_skew_stop_writes_sec`.
- Ngữ nghĩa là **last-writer-wins theo từng bin.** Các thao tác đọc-sửa-ghi như increment,
  CDT list/map hay UDF chạy đồng thời ở hai DC **không cộng dồn**: một bên sẽ thắng. Ứng dụng
  cần biết điều này. Hoặc định tuyến một key về một DC chính. Ghi rõ trong tài liệu sử dụng.
- Index lớn hơn vì có bin tombstone và cemetery. Mỗi bin tốn thêm 6 byte metadata (LUT và
  `src_id`) trên storage.

---

## 4. Lộ trình (1–2 dev C senior, ước lượng)

| Giai đoạn | Nội dung | Thời gian | Tiêu chí hoàn thành |
|---|---|---|---|
| **P0 – Hạ tầng** | Cờ build `USE_EXT`, thư mục `as/src/ext`, gỡ gating config và schema (sau `AS_EXT`), CI build trên Docker, harness test cluster 3 node (docker compose), smoke test bằng Java client | 1–2 tuần | Build ra 2 biến thể. Config `security {}` và `xdr {}` parse được, chưa có chức năng. |
| **P1a – Auth core** | Protocol security, SMD store, login/session, RBAC với role định sẵn, check data/info, user `admin` mặc định, audit log cơ bản | 4–5 tuần | `asadm manage acl`, `aql`, Java client đăng nhập được. Truy cập không đủ quyền bị từ chối. Chạy được bộ test quyền theo ns/set. |
| **P1b – Auth mở rộng** | Allowlist, quota, `set-config` động, refresh privilege, đo overhead | 2 tuần | Overhead ≤ 3% TPS (đo bằng kvbench) |
| **P2a – Tombstone/durable delete** | Đường ghi tombstone (master, replica, migrate, cold start), read path, thống kê | 3–4 tuần | Kịch bản zombie: write → durable delete → cold restart / node rời rồi vào lại cluster → record **không** sống lại. |
| **P2b – Tomb raider** | Ba bước mark / unmark (quét device) / remove, cấu hình tham số, throttle | 2–3 tuần | Tombstone được thu hồi sau eligible-age và không bao giờ xoá nhầm khi còn bản cũ trên disk. |
| **P3a – XDR ship** | Config DC, submit queue, reader, shipper async, partition map đích, auth tới đích, stats cơ bản. Kiểm thử trước một chiều với `bin-policy all`. | 5–6 tuần | 2 cluster: lag p99 < 1s ở tải chuẩn. Ship đúng insert/update/durable delete. |
| **P3b – Bin convergence** | Bin LUT, `src_id`, bin tombstone (mục 3.4), `resolve_bin`, `set_xdr_write` chống ship ngược, `bin-policy only-changed` + `ship-bin-luts`, lưu metadata bin trên disk, XDR tomb raider | 5–6 tuần | 2 cluster cùng ghi một tập key ở cả hai phía → sau khi hội tụ, hai bên **giống hệt nhau từng bin** (checker so sánh toàn bộ), không có vòng lặp ship. |
| **P3c – XDR bền vững** | LST + recovery (queue tràn, restart, đổi master, DC down/up), hot key, throttle, thêm/bớt DC động, `ship-nsup-deletes` | 4–5 tuần | Chaos test (kill node, cắt mạng giữa hai DC khi cả hai đang ghi) → hội tụ, không mất write. |
| **P3d – Tuỳ chọn** | Expression filter, compression, topology nhiều hơn 2 DC | Theo nhu cầu | — |

Tổng cho phạm vi cốt lõi (P0 → P3c): khoảng **6–7 tháng** với 1 dev, hoặc **4–5 tháng**
với 2 dev. Active-active thêm khoảng 5–6 tuần so với kế hoạch active-passive ban đầu; bỏ
TLS và LDAP bớt được khoảng 3–5 tuần. Sau P0, nhánh Auth và nhánh Tombstone có thể làm song
song. P3b phụ thuộc P2a vì dùng chung cơ chế tombstone.

---

## 5. Kiểm thử & vận hành

- **Unit:**
  - Parser của security message.
  - Hash/HMAC token.
  - Logic resolve tombstone theo gen/LUT.
  - Ring queue của XDR.
- **Integration (docker compose):**
  - Cluster 3 node RF=2 cho Auth và Tombstone.
  - Hai cluster cho XDR active-active, cả hai đều là bản mở rộng, `src-id` khác nhau. Có thể
    giả lập lệch giờ bằng `libfaketime`.
  - Chạy bằng Java client của **kvbench**: đã dùng client 8.1.5, chỉ cần thêm cấu hình
    `user`/`password` vào driver Aerospike.
- **Chaos/consistency:** kill -9 node, cắt mạng (`tc`/iptables), restart cold start.
  Dùng một checker kiểu Jepsen rút gọn để xác nhận:
  - không có record "sống lại" sau delete;
  - mọi write đã ACK ở nguồn đều có mặt ở đích sau khi hội tụ.
- **Hiệu năng:** dùng kvbench, so sánh baseline CE với bản mở rộng ở 3 trạng thái:
  bật auth, bật durable delete, bật XDR.
  Đo TPS, p99 và chi phí bộ nhớ (index lớn lên do tombstone).
- **Duy trì theo upstream:**
  - Mỗi bản CE mới thì rebase nhánh `ext`.
  - Script kiểm tra chữ ký các hook trong `*_ce.c` không đổi, chạy diff header
    `xdr.h`, `security.h`, `rw_utils.h` để phát hiện hook mới hoặc đổi chữ ký.

---

## 6. Rủi ro

1. **Không có TLS (đã chốt).** Credential (bcrypt hash, tương đương mật khẩu), session
   token và dữ liệu XDR đều đi dạng plaintext. Điều kiện triển khai bắt buộc:
   - Port service (3000) chỉ mở cho mạng ứng dụng.
   - Fabric và heartbeat nằm trên mạng nội bộ riêng.
   - XDR giữa hai DC đi qua VPN hoặc WireGuard.
   - Ghi rõ các điều kiện này trong runbook.
2. **Tương thích tool.** `asadm` và client có thể dựa vào `edition` hay `features` để bật
   chức năng security. Cần kiểm tra ngay từ P0.
3. **Bộ nhớ index tăng do tombstone:** mỗi tombstone tốn 64B index. Tomb raider phải chạy
   ổn định, cần cảnh báo khi `tombstones/objects` cao.
4. **Đúng đắn của XDR khi đổi master hoặc migrate:** đây là phần khó nhất. Chấp nhận ship
   trùng lặp (at-least-once). Nhờ quy tắc LUT theo bin, ship trùng lặp hay sai thứ tự đều
   không làm sai kết quả. Cần thời gian chaos test đủ dài.
5. **Active-active và đồng hồ:** hội tụ dựa vào LUT, nên lệch giờ giữa các DC làm sai thứ tự
   giữa các write gần nhau. Phép cộng dồn (increment, CDT) chạy đồng thời ở hai phía không
   được merge. Cần thống nhất với đội ứng dụng (mục 3.4).
6. **Pháp lý:** AGPL-3.0 và EULA của EE. Owner sẽ đánh giá sau. Trong lúc chờ: làm
   clean-room, không dùng thương hiệu, chưa phát hành binary ra ngoài tổ chức.
7. **Phạm vi strong-consistency (SC):** bản CE không có SC. Kế hoạch này chỉ nhắm namespace
   AP.

## 7. Bước tiếp theo đề xuất

1. ~~Chốt phạm vi~~: đã chốt ở mục 0.
2. Làm P0: tạo khung `USE_EXT`, gỡ gating config, dựng harness docker 2 cluster và thêm
   cấu hình auth cho kvbench.
3. Bắt đầu P1a (Auth) song song với P2a (Tombstone).
