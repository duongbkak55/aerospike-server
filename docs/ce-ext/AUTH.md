# Authentication & RBAC (P1a) – bản build `USE_EXT=1`

Phần cài đặt nằm trong `as/src/ext/security_ext.c`. Kế hoạch tổng xem `PLAN.md`.

## Bật tính năng

```
security {
	# session-ttl 1d                     # mặc định 86400 giây
	# default-password-file /etc/aerospike/admin.pw
	log {
		report-authentication true    # log mỗi lần login
		report-user-admin true        # log các lệnh quản trị user/role
		report-violation true         # log các lần bị từ chối quyền
		# report-sys-admin true       # log các lệnh set-config, log-set...
	}
}
```

Cấu hình YAML (`--experimental`) dùng `security: {}` với các khoá tương ứng.

- Khi bật, **mọi** kết nối client và info phải đăng nhập. Kết nối chưa đăng nhập nhận lỗi
  `80 not authenticated`.
- Khi không có context `security`, bản ext hoạt động như CE. Client có cấu hình
  user/password vẫn kết nối được, vì server trả `52 security not configured` và client
  coi mã này là "không cần đăng nhập".
- **Chưa hỗ trợ:** LDAP, PKI, quota (`enable-quotas`), allowlist. Config có `ldap {}` hoặc
  `enable-quotas true` sẽ bị từ chối khi khởi động. Quota và allowlist dự kiến làm ở P1b.

## User mặc định

- Khi cluster chưa có user nào, server tạo sẵn user `admin`, mật khẩu `admin`, role
  `user-admin`. Nếu cấu hình `default-password-file` thì lấy mật khẩu từ dòng đầu tiên của
  file đó.
- **Phải đổi mật khẩu ngay sau khi cài.** User mặc định được lưu trong SMD với timestamp 0,
  nên bất kỳ thay đổi thật nào (đổi mật khẩu, xoá) cũng thắng. Mật khẩu mặc định không quay
  lại sau khi restart, kể cả khi thêm node mới vào cluster.
- `admin` chỉ có quyền `user-admin`, không có `sys-admin` hay quyền đọc/ghi dữ liệu. Muốn
  dùng các quyền đó thì tạo user riêng.

## Role định sẵn và quyền tương ứng

| Role | Mã | Quyền (`as_sec_perm`) | Phạm vi |
|---|---|---|---|
| `user-admin` | 0 | quản trị user và role | toàn cục |
| `sys-admin` | 1 | `set-config`, `log-set`, điều khiển service, XDR filter, `query-abort`, eviction, record info | toàn cục |
| `data-admin` | 2 | quản trị UDF, sindex, `truncate`, `query-abort`, eviction | toàn cục |
| `udf-admin` | 3 | quản trị UDF | toàn cục |
| `sindex-admin` | 4 | quản trị sindex | toàn cục |
| `read` | 10 | đọc, query/scan | toàn cục, namespace, hoặc namespace.set |
| `read-write` | 11 | `read` + ghi, xoá, background ops query | như trên |
| `read-write-udf` | 12 | `read-write` + gọi UDF, UDF query | như trên |
| `write` | 13 | ghi, xoá (không đọc) | như trên |
| `truncate` | 14 | `truncate` | như trên |

- Role tự định nghĩa (`create role`) gồm nhiều privilege, mỗi privilege có thể giới hạn theo
  namespace hoặc namespace + set. Privilege quản trị (mã < 10) chỉ có phạm vi toàn cục.
- Với thao tác dữ liệu, privilege giới hạn theo set chỉ áp dụng khi request có set khớp.
  Key không có set thì chỉ dùng được privilege toàn cục hoặc privilege theo namespace.
- Với lệnh info có tham số `namespace=` (và `set=`), ví dụ `truncate`, server xét cả
  privilege theo namespace và set. Các lệnh info khác chỉ xét privilege toàn cục.
- Batch: từng key được kiểm tra riêng. Key không đủ quyền nhận lỗi `81`, các key khác vẫn
  trả về bình thường.
- User được tự đổi mật khẩu của mình (phải nhập mật khẩu cũ) và tự xem thông tin của mình.
  Mọi lệnh quản trị khác cần `user-admin`.

## Công cụ

- Client chính thức (Java, C, Go, Python…): chỉ cần đặt user/password trong
  `ClientPolicy`, không phải sửa gì khác.
- kvbench: đặt `db.aerospike.auth.user` / `db.aerospike.auth.password` trong file YAML.
- `tools/ext-lab/asclient.py`: client tối giản để quản trị khi chưa có `asadm`.

  ```sh
  asclient.py -U admin -P admin create-user alice 'pw' read-write
  asclient.py -U admin -P admin query-users
  ```
- `asadm` và `aql`: chưa kiểm tra (xem PLAN.md mục 6, rủi ro tương thích công cụ).

## Thiết kế

- **Kho dữ liệu:** user và role user tự định nghĩa nằm trong SMD module `security`, được
  đồng bộ toàn cluster và lưu ra file `<work-directory>/smd/security.smd`.
  - Key `U|<user>` có value `v1|<salt>|<hash>|<role>,...`.
  - Key `R|<role>` có value `v1|<code>:<ns>:<set>,...`.
- **Lưu mật khẩu:** client gửi `credential` là bcrypt của mật khẩu với salt cố định (giống
  client chính thức). Server lưu `PBKDF2-HMAC-SHA256(credential, salt ngẫu nhiên 16 byte,
  10.000 vòng)`. Server không lưu credential gốc.
- **Session token:** gồm `version | expiry | HMAC-SHA256`, trong đó HMAC lấy hash của user
  làm khoá.
  - Mọi node đều kiểm tra được token mà không cần lưu trạng thái.
  - Đổi mật khẩu hoặc xoá user thì mọi token cũ mất hiệu lực, và các kết nối đang mở bị
    huỷ xác thực ở request kế tiếp.
- **Đổi quyền:** mỗi lần SMD thay đổi, server tăng một số thế hệ (generation) toàn cục. Mỗi
  kết nối tính lại quyền khi thấy số này đổi, nên grant/revoke có hiệu lực ngay trên kết
  nối đang mở.
- **Quyền file:** file SMD (mọi module) được ghi với quyền `0600`, vì `security.smd` chứa
  hash.

## Hạn chế đã biết

- **Không có TLS:** credential (tương đương mật khẩu) và token đi dạng rõ trên mạng. Bắt
  buộc phải cách ly mạng (xem PLAN.md mục 0).
- Ai đọc được `security.smd` có thể giả mạo token của user tương ứng. Phải bảo vệ
  work-directory, chỉ user chạy `asd` được đọc.
- Mỗi lần login tốn PBKDF2 (vài ms CPU) trên service thread. Kết nối mới dùng token, chỉ
  tốn một phép HMAC. Chưa giới hạn tần suất login sai (dự kiến P1b).
- `report-data-op` theo namespace, set, role hay user chưa hỗ trợ lọc theo phạm vi: chỉ cần
  cấu hình một mục là bật log data-op cho toàn bộ.
- Các node trong cluster vẫn trao đổi với nhau (fabric, heartbeat) mà không xác thực.

## Overhead (kvbench)

Đo ngày 13/10/2026 trong môi trường lab:
- 1 node, namespace lưu trong memory, 100k key.
- Workload B1 (50% READ, 50% OPERATE_ABM), async, concurrency 64, warmup 10 giây, đo 45
  giây.
- Client và server chạy chung một máy 4 vCPU.
- 2 vòng, các biến thể chạy xen kẽ nhau.

| Biến thể | TPS vòng 1 / vòng 2 | p99 trung bình |
|---|---|---|
| CE gốc | 51.311 / 51.640 | 5,03 ms |
| ext, không bật `security` | 52.495 / 52.425 | 4,87 ms |
| ext, có `security`, user `read-write` | 52.701 / 52.742 | 4,99 ms |

Không đo được overhead: mọi chênh lệch đều nằm trong mức nhiễu khoảng ±2% của môi trường
này. Điều này phù hợp với thiết kế: mỗi request chỉ tốn một lần lock mutex của kết nối và
duyệt danh sách privilege đã tính sẵn. Cần đo lại trên phần cứng thật, nhiều node, client
tách riêng khỏi server.

## Test

```sh
tools/ext-lab/p1a_auth_test.py <asd-ext>                     # protocol, RBAC, restart
tools/ext-lab/p1a_data_test.sh <asd-ext> <kvbench.jar>       # đọc/ghi/scan/batch với Java client
```
