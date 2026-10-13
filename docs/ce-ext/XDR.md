# XDR (P3a, P3b, P3c) – bản build `USE_EXT=1`

Kế hoạch tổng xem `PLAN.md`. Tài liệu này gồm ba giai đoạn:
- **P3a:** ship dữ liệu giữa các cluster, chạy được active-active.
- **P3b:** hội tụ theo bin (bin convergence). Hai DC cùng ghi một key vẫn cho ra cùng một
  kết quả ở cả hai bên. Xem mục [Hội tụ theo bin (P3b)](#hội-tụ-theo-bin-p3b).
- **P3c:** phục hồi bền vững. Write không bị mất khi queue đầy, khi node restart, khi đổi
  master, hay khi DC đích ngừng lâu. Xem mục [Phục hồi (P3c)](#phục-hồi-p3c).

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
| `namespace` → `ship-bin-luts` | Có (P3b), xem mục hội tụ theo bin |
| `namespace` → `bin-policy` | `all`, `only-changed` (hiện ship mọi bin, kể cả với `only-changed`). Giá trị khác: server từ chối khởi động |
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
  - Queue đầy: write mới không vào queue mà sẽ được phục hồi từ storage (P3c).
- **Key nóng:** một key đang chờ trong queue thì chỉ giữ một item. Write tiếp theo vào key
  đó không thêm item mới, vì item đang chờ sẽ đọc và ship bản mới nhất (stat `hot_keys`).

## Hội tụ theo bin (P3b)

**Khi nào cần:** khi cả hai DC có thể ghi cùng một key. Không bật thì bản ship đến sau đè
cả record, nên hai DC có thể lệch nhau.

**Cấu hình** (bật ở **mọi** cluster tham gia):

```
namespace test {
	conflict-resolve-writes true
	...
}

xdr {
	src-id 1              # duy nhất cho mỗi cluster, từ 1 đến 255
	dc DC_B {
		node-address-port 10.0.2.1 3000
		namespace test {
			ship-bin-luts true
		}
	}
}
```

Server từ chối khởi động nếu:
- Namespace có `conflict-resolve-writes` mà DC không bật `ship-bin-luts`, hoặc ngược lại.
- Bật `ship-bin-luts` mà chưa đặt `src-id`.

**Quy tắc:** mỗi bin là một thanh ghi "ghi sau thắng" (last-writer-wins).
- Mỗi bin lưu LUT (thời điểm ghi) và `src-id` (cluster đã ghi).
- Khi nhận write qua XDR, server so từng bin với bản đang có. LUT lớn hơn thì thắng. LUT
  bằng nhau thì `src-id` lớn hơn thắng.
- Bin thua bị bỏ qua. Nếu mọi bin đều thua thì đích trả `LOST_CONFLICT`, phía ship coi là
  thành công.
- Write ship qua XDR là **cập nhật**, không thay thế record. Bin chỉ có ở đích vẫn được
  giữ.

**Xoá:**
- Xoá một bin để lại **bin tombstone** mang LUT của lần xoá. Bản ship cũ hơn đến sau sẽ
  không làm bin sống lại.
- Xoá cả record (kể cả delete thường, vì delete thường được tự đổi thành durable delete)
  biến mọi bin đang có thành bin tombstone. Record không còn bin sống gọi là **bin
  cemetery**: đọc trả `not found`, được đếm vào stat `tombstones` và `xdr_bin_cemeteries`.
- Ghi bin mới lên một bin cemetery làm record sống lại. Các bin đã xoá vẫn ở trạng thái đã
  xoá.
- **Xoá chỉ áp dụng cho dữ liệu mà DC đó đã thấy.** Ví dụ DC A xoá một record mà A chưa
  nhận được, trong khi DC B vừa tạo record đó: A trả `not found` và không để lại gì. Record
  của B được giữ ở cả hai bên.

**Dọn dẹp:**
- Bin tombstone cũ hơn `xdr-bin-tombstone-ttl` (mặc định 1 ngày) bị bỏ ở lần ghi kế tiếp
  vào record đó.
- Bin cemetery được tomb raider xoá như tombstone thường (`tomb-raider-eligible-age`, xem
  `TOMBSTONE.md`).
- `xdr-tomb-raider-period` và `xdr-tomb-raider-threads` được nhận để tương thích config
  nhưng không có tác dụng.
- **Cả hai TTL phải lớn hơn độ trễ ship tối đa,** kể cả lúc DC bị cắt mạng. Nếu không, bản
  cũ đến sau khi tombstone đã bị dọn có thể làm dữ liệu sống lại.

**Giờ hệ thống:**
- LUT là đồng hồ ms của từng node, nên **các DC phải đồng bộ giờ** (NTP/chrony).
- Nếu bản ghi nhận qua XDR có LUT "ở tương lai" do giờ lệch, LUT của record được đẩy lên
  theo. Write cục bộ sau đó vẫn luôn mới hơn mọi bin mà nó ghi đè (LUT tăng ít nhất 1 ms).
- Khi giờ hai DC lệch nhau, hai write gần nhau có thể được xếp sai thứ tự. Hai bên vẫn hội
  tụ về cùng một kết quả.

**Ngữ nghĩa cần biết:**
- Increment, thao tác CDT (list/map) và UDF chạy đồng thời ở hai DC **không cộng dồn**:
  giá trị của một bên sẽ thắng nguyên bin.
- Không cho phép write kiểu replace (`REPLACE`, `CREATE_OR_REPLACE`) trên namespace bật
  `conflict-resolve-writes`: server trả lỗi parameter.
- Không nên bật `default-read-touch-ttl-pct` cùng `conflict-resolve-writes`.

**Chi phí:**
- Mỗi bin tốn thêm tối đa 6 byte trên storage (LUT và `src-id`).
- Bin tombstone và bin cemetery chiếm chỗ cho tới khi được dọn.
- Mỗi lần ship gửi mọi bin kèm LUT, kể cả bin tombstone. Đặt `bin-policy only-changed`
  để chỉ ship bin đổi sau last-ship-time (P3c). Không áp dụng khi bật `forward`, và lần
  ship do phục hồi luôn gửi mọi bin.
- `conflict-resolve-writes` chỉ đặt được trong file config, chưa đổi được lúc đang chạy.

## Phục hồi (P3c)

**Last-ship-time (LST):** mỗi DC có một LST. Mọi phiên bản record có LUT ≤ LST đã được
ship.
- LST đi theo item cũ nhất còn trong queue hoặc đang ship, trừ một khoảng an toàn 5 giây.
- Khi đang có phục hồi chờ chạy hoặc đang chạy, LST bị giữ lại ở mốc bắt đầu phục hồi.
- Mỗi node công bố LST của mình qua SMD (module `XDR`, key `<dc>|<node-id>`), 5 giây một
  lần. SMD được chia sẻ trong cluster và lưu xuống đĩa, nên LST còn sau restart. Xem bằng
  `asinfo -v 'smd-show:module=XDR'`.

**Phục hồi:** quét index các partition mà node đang làm master và ship lại mọi record có
LUT > mốc (kể cả tombstone). Phục hồi xảy ra khi:

| Tình huống | Mốc phục hồi |
|---|---|
| Queue đầy, write bị bỏ | LST hiện tại |
| DC đích ngừng quá lâu (item chờ hơn khoảng 66 phút): xả hết queue (`flushing`) | LST lúc xả |
| Node trở thành master của partition (node khác chết, thêm/bớt node) | LST nhỏ nhất mà các node của cluster trước đó đã công bố |
| Node restart | LST của chính node trước khi restart, giữ cho tới khi phục hồi được xếp lịch |

Thêm vài điểm:
- Partition chỉ được quét khi node là master cuối cùng **và** đã migrate xong, vì record
  đến qua migrate không được submit vào XDR.
- Phục hồi chừa một nửa queue cho write mới, và chờ có chỗ trống chứ không bỏ item.
- Trong lúc migrate, node đang tạm làm master (acting master) cũng ship write mà nó nhận.
- LST của node đã rời cluster quá 10 phút bị principal xoá khỏi SMD.

**Hệ quả cần biết:**
- Phục hồi có thể ship lại record đã ship. Việc này vô hại: đích ghi lại cùng nội dung,
  hoặc trả `LOST_CONFLICT` khi bật hội tụ theo bin. Ví dụ, khi cluster vừa hình thành, node
  nhận partition sẽ quét lại các write mới nhất.
- Node gia nhập lại hoặc restart sẽ quét lại các partition nó làm master, với mốc là LST
  cũ nhất của cluster. Sau sự cố dài, việc quét này có thể tốn tài nguyên.
- **Delete thường (không durable) không phục hồi được**, vì record đã biến khỏi index. Cần
  dùng durable delete nếu delete phải tới được đích. Với `conflict-resolve-writes`, mọi
  delete đều đã tự thành durable.
- Thêm DC mới không ship dữ liệu có sẵn (chưa có "rewind").

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
| `hot_keys` | Số write gộp vào item đang chờ của cùng key |
| `queue_overflows` | Số write không vào được queue vì đầy (sẽ được phục hồi) |
| `recoveries`, `recovered` | Số lượt phục hồi, số record đã xếp hàng do phục hồi |
| `flushes` | Số lần xả queue vì DC đích ngừng quá lâu |
| `lst`, `lst_lag` | Last-ship-time (ms) và độ trễ của nó so với hiện tại |
| `recovering`, `recovery_in_queue` | Đang có phục hồi chờ hoặc chạy; số item phục hồi chưa ship xong |
| `flushing` | Đang xả queue |

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
| `bin_ext.c` | `base/bin_ce.c` | P3b: trạng thái bin tombstone, dọn bin tombstone quá hạn, nhận diện bin cemetery. |
| `rw_utils_ext.c` | (đã có) | P3b: `resolve_bin` so LUT và `src-id` theo bin, `write_resolved_bin` / `delete_bin` / `delete_all_bins` ghi metadata và tạo bin tombstone, cấm replace, đổi delete thường thành durable delete. |
| `record_ext.c` | (đã có) | P3b: LUT của record tăng đơn điệu, nhận diện bin cemetery ở replica, set index và stat. |
| `flat_ext.c`, `drv_common_ext.c`, `tomb_raider_ext.c` | (đã có) | P3b: lưu `src-id` và cờ `xdr-write` của bin, cờ bin cemetery; nạp lại khi cold start; tomb raider dọn bin cemetery. |
| `xdr_ext.c` | (đã có) | P3b: với `ship-bin-luts`, ship mọi bin (bin tombstone thành op xoá bin) kèm LUT và `src-id` (byte cao của LUT), không replace. P3c: gộp key nóng, queue đầy thì chuyển sang phục hồi, `bin-policy only-changed`, master tạm thời cũng ship. |
| `xdr_recovery_ext.c` | (file mới) | P3c: theo dõi LST, công bố qua SMD, phát hiện partition mới làm master, thread phục hồi, xả queue khi DC ngừng lâu. |

Core **không phải sửa**. Phía đích dùng đường ghi XDR sẵn có của core (cờ `INFO1_XDR`,
field LUT).

**Lưu ý nâng cấp:** record nhận qua XDR được lưu với bit `xdr-write`. Bản CE gốc không
đọc được các record này (nó báo lỗi `bad xdr-write` khi đọc). Vì vậy không thể chạy bản CE
gốc trên dữ liệu đã nhận qua XDR. Bản EE thì dùng đúng định dạng này.

## Hạn chế hiện tại

- **Xung đột khi hai DC cùng ghi một key:** chỉ an toàn khi bật hội tụ theo bin (P3b).
  Không bật thì nếu một key được ghi ở cả hai DC trong khoảng thời gian nhỏ hơn độ trễ
  ship, mỗi bên có thể giữ bản của bên kia.
- **Phục hồi:** xem các hệ quả ở mục [Phục hồi (P3c)](#phục-hồi-p3c). Delete thường không
  phục hồi được. Chưa có rewind (ship dữ liệu cũ cho DC mới).
- Mỗi worker gửi một request rồi chờ kết quả, mỗi DC có 4 worker. Throughput ship vì vậy
  bị giới hạn bởi độ trễ mạng giữa các DC. Chưa có pipelining.
- `max-throughput`, `delay-ms`, `hot-key-ms`, `ship-nsup-deletes`, `period-ms`,
  `max-recoveries-interleaved` được nhận nhưng chưa có tác dụng. Đổi config DC lúc đang
  chạy cũng chưa hỗ trợ.
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

### Hội tụ theo bin (P3b)

```sh
tools/ext-lab/p3b_converge_test.sh <asd-ext> <kvbench.jar> memory   # 16 kiểm tra
tools/ext-lab/p3b_converge_test.sh <asd-ext> <kvbench.jar> file     # 19 kiểm tra, có cold restart
```

- Hai cluster ship cho nhau qua `tcpproxy.py`. Tắt proxy để cắt mạng giữa hai DC, cả hai
  phía vẫn nhận write và giữ việc ship trong queue. Bật lại proxy để nối mạng.
- **Merge khi không cắt mạng:** bin ghi ở mỗi bên được giữ; xoá bin và xoá record được
  ship.
- **Xung đột khi cắt mạng:**
  - Cùng một bin: write sau thắng, theo cả hai chiều.
  - Khác bin: hai bên được merge.
  - Xoá bin sau write thì xoá thắng. Write sau xoá bin thì write thắng.
  - Xoá record sau write thì xoá thắng. Write sau xoá record thì record sống lại chỉ với
    bin mới.
- **Ghi ngẫu nhiên:** put, xoá bin, xoá record và increment.
  - Lần 1: ghi ở cả hai phía khi đang cắt mạng.
  - Lần 2: bốn writer đồng thời trên cả bốn node khi không cắt mạng.
  - Sau khi ship xong, toàn bộ dữ liệu hai bên trùng khớp (so sánh hash).
- **File storage:** sau cold restart cả hai cluster, dữ liệu không đổi, hai bên vẫn trùng
  khớp, và xung đột mới vẫn được resolve đúng (dùng metadata bin nạp lại từ đĩa).

### Phục hồi (P3c)

```sh
tools/ext-lab/p3c_recovery_test.sh <asd-ext> <kvbench.jar>   # 13 kiểm tra
BIN_POLICY=only-changed tools/ext-lab/p3b_converge_test.sh <asd-ext> <kvbench.jar> file
```

Cluster nguồn có 2 node, dùng file storage, ship qua proxy. Tắt proxy để mô phỏng mất kết
nối giữa hai DC.
- **Key nóng:** ghi một key nhiều lần khi đang mất kết nối. Các write được gộp lại, và sau
  khi nối lại thì đích nhận đúng giá trị cuối.
- **Queue tràn:** ghi 6000 record khi mất kết nối, với queue 1024. Sau khi nối lại, phục
  hồi ship đủ 6000 record.
- **Restart toàn cluster nguồn:** ghi khi mất kết nối, rồi restart cả hai node (queue mất
  theo). Sau khi nối lại, mọi write đều tới đích.
- **Một node nguồn chết** (`kill -9`) khi còn write chưa ship. Node còn lại trở thành
  master và phục hồi từ LST mà node chết đã công bố, nên mọi write đều tới đích. Node chết
  vào lại cluster và việc ship tiếp tục bình thường.
- **Hội tụ theo bin với `bin-policy only-changed`:** chạy lại toàn bộ test P3b.

