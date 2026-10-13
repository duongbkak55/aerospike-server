# XDR (P3a) – bản build `USE_EXT=1`

Kế hoạch tổng xem `PLAN.md`. Đây là giai đoạn **P3a**: ship dữ liệu giữa các cluster, chạy
được active-active. Phần hội tụ theo bin (P3b) và phục hồi bền vững (P3c) chưa có, xem
[Hạn chế hiện tại](#hạn-chế-hiện-tại).

## Cấu hình

Cú pháp giống bản EE. Ví dụ active-active giữa hai cluster:

```
# Cluster A
xdr {
	dc DC_B {
		node-address-port 10.0.2.1 3000
		node-address-port 10.0.2.2 3000    # nhiều seed: thử lần lượt
		auth-mode internal                 # bỏ qua nếu cluster B không bật security
		auth-user xdr
		auth-password-file /etc/aerospike/xdr.pw
		namespace test {
		}
	}
}

# Cluster B: cấu hình ngược lại, trỏ về A.
```

User trên cluster đích cần quyền ghi (ví dụ role `read-write`) trên namespace được ship.
File mật khẩu chứa mật khẩu dạng rõ, nên đặt quyền `0600`.

| Config | Hỗ trợ |
|---|---|
| `dc` → `node-address-port` | Có (một hoặc nhiều seed) |
| `dc` → `auth-mode` | `none`, `internal` |
| `dc` → `auth-user`, `auth-password-file` | Có |
| `namespace` → `remote-namespace` | Có |
| `namespace` → `forward` | Có (ship tiếp cả các write nhận qua XDR) |
| `namespace` → `ship-only-specified-sets`, `ship-set`, `ignore-set` | Có |
| `namespace` → `transaction-queue-limit` | Có (mặc định 16K, chia đều cho 4 worker) |
| `tls-name`, `connector`, `auth-mode external/pki` | **Không**: server từ chối khởi động |
| `bin-policy` khác `all`, `ship-bin-luts` | **Không** (P3b): server từ chối khởi động |
| `compression`, `ship-bin`, `ignore-bin` | **Không**: server từ chối khởi động |
| Các option khác (`delay-ms`, `max-throughput`, `hot-key-ms`, `ship-nsup-deletes`…) | Được nhận nhưng chưa có tác dụng |
| Đổi config DC lúc đang chạy (`set-config:context=xdr`) | Chưa hỗ trợ |

## Hành vi

- **Cái gì được ship:** mọi write trên master của partition (client write, UDF, operate),
  durable delete và delete thường. Replica không ship.
- **Ship bản mới nhất:** worker đọc record hiện tại tại thời điểm ship và gửi toàn bộ bin,
  thay thế record ở đích (`create-or-replace`). Nhiều write liên tiếp vào một key có thể
  được gộp thành một lần ship.
- **Thứ tự:** mỗi DC có 4 worker. Một key luôn đi qua cùng một worker (chọn theo digest),
  nên các thay đổi của một key được ship theo đúng thứ tự.
- **Delete:**
  - Durable delete (tombstone) được ship thành durable delete, nên đích cũng giữ
    tombstone.
  - Delete thường được ship thành delete thường, với điều kiện record vẫn chưa được ghi
    lại tính đến lúc ship.
  - Record hết hạn (expire) và record bị evict thì không ship.
- **Chống vòng lặp (active-active):** record nhận qua XDR được đánh dấu `xdr-write`, và
  dấu này được lưu cả trên storage. Write có dấu này không được ship tiếp, trừ khi bật
  `forward`. Với active-active giữa hai DC thì **không bật `forward`**.
- **Đích nhận write:** worker kết nối tới một seed bất kỳ của cluster đích. Seed sẽ proxy
  write sang node master của record (cluster đích chưa cần partition map phía nguồn).
- **Lỗi và retry:**
  - Mất kết nối, timeout, đích quá tải hoặc đang đổi cluster: retry mãi, mỗi lần cách
    500 ms, đổi seed khi không kết nối được.
  - Đích từ chối hẳn (ví dụ thiếu quyền, record quá lớn): bỏ record đó, tăng `abandoned` và
    ghi log cảnh báo.
  - Queue đầy: bỏ write mới, tăng `abandoned`.

## Thống kê

Lệnh `asinfo -v 'get-stats:context=xdr;dc=DC_B'` trả về:

| Stat | Ý nghĩa |
|---|---|
| `in_queue` | Số write đang chờ ship |
| `success` | Số lần ship thành công |
| `abandoned` | Số write bị bỏ (queue đầy hoặc đích từ chối hẳn) |
| `not_found` | Record đã biến mất trước khi ship, nên không ship |
| `filtered_out` | Số write không ship vì có dấu `xdr-write` hoặc set bị lọc |
| `retry_conn_reset` | Số lần retry do lỗi kết nối |
| `retry_dest` | Số lần retry do đích trả lỗi tạm thời |
| `lag` | Tuổi (ms) của write ship gần nhất, tính từ lúc ghi ở nguồn |
| `latency_us` | Thời gian round-trip trung bình của một lần ship |

`get-config:context=xdr` liệt kê các DC. `get-config:context=xdr;dc=DC_B` trả seed,
namespace và chế độ auth. Ticker in mỗi DC một dòng mỗi 10 giây.

## Cài đặt

| File ext | Thay cho | Nội dung |
|---|---|---|
| `xdr_ext.c` | `base/xdr_ce.c` | Đăng ký DC, kiểm tra config, queue và worker, đọc record, dựng `as_msg` XDR, kết nối và login tới đích, retry, stats/info/ticker. |
| `flat_ext.c` | `storage/flat_ce.c` | Lưu bit `xdr-write` của record xuống storage, và đọc lại bit này từ bản ghi nhận qua replica/migrate. |
| `rw_utils_ext.c` | (đã có) | `set_xdr_write`: đánh dấu record ghi bởi XDR. `unwind_index_metadata` khôi phục bit khi write lỗi. |
| `record_ext.c` | (đã có) | `replace_index_metadata` lấy `xdr-write` từ bản ghi replica/migrate. |
| `drv_ssd_ext.c`, `drv_mem_ext.c` | (đã có) | Cold start nạp lại `xdr-write`. |
| `delete_ext.c` | (đã có) | Delete thường trên master thì submit sang XDR. |

Core **không phải sửa**. Phía đích dùng đường ghi XDR sẵn có của core (cờ `INFO1_XDR`,
field LUT).

**Lưu ý nâng cấp:** record nhận qua XDR được lưu với bit `xdr-write`. Bản CE gốc không
đọc được các record này (nó báo lỗi `bad xdr-write` khi đọc). Vì vậy không thể chạy bản CE
gốc trên dữ liệu đã nhận qua XDR. Bản EE thì dùng đúng định dạng này.

## Hạn chế hiện tại

- **Xung đột khi hai DC cùng ghi một key:** nếu cùng một key được ghi ở cả hai DC trong
  khoảng thời gian nhỏ hơn độ trễ ship, mỗi bên có thể giữ bản của bên kia. Hai DC lệch
  nhau cho tới lần ghi kế tiếp vào key đó.
  - P3a an toàn khi mỗi key chỉ được ghi ở một DC tại một thời điểm, ví dụ chia key theo
    vùng hoặc theo người dùng.
  - P3b sẽ giải quyết bằng LUT theo bin (bin convergence, `conflict-resolve-writes`).
- **Không bền qua sự cố:**
  - Queue nằm trong RAM. Write còn trong queue sẽ mất khi node nguồn restart.
  - Master đổi do migrate trước khi kịp ship cũng làm mất write.
  - Write bị bỏ khi queue đầy.
  - Chưa có cơ chế recovery theo last-ship-time, kể cả cho dữ liệu có sẵn trước khi
    thêm DC. Phần này thuộc P3c.
- Mỗi worker gửi một request rồi chờ kết quả, mỗi DC có 4 worker. Throughput ship vì vậy
  bị giới hạn bởi độ trễ mạng. Pipelining nằm trong P3c.
- Chưa có TLS và nén. Giữa các DC nên dùng VPN hoặc kênh riêng.

## Test

```sh
tools/ext-lab/p3a_xdr_test.sh <asd-ext> <kvbench.jar>
```

26 kiểm tra trên hai cluster cục bộ, mỗi cluster 2 node, ship qua lại cho nhau:
- **Active-active:**
  - Write, update, durable delete, delete thường và ghi đè lên tombstone đều được ship.
  - Ship được theo cả hai chiều.
  - Không có vòng lặp (generation không tăng thêm, `filtered_out` khớp số write).
- **Đích tạm ngừng:** write được giữ trong queue rồi ship hết khi đích chạy lại.
- **File storage:** record nhận qua XDR vẫn còn sau cold restart. Write cục bộ lên các
  record này thành công và được ship ngược lại.
- **Đích bật security:** ship với `auth-mode internal`.
