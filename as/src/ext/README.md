# as/src/ext

Phần cài đặt các tính năng mở rộng cho bản build Community: Auth, tombstone và XDR
active-active. Kế hoạch tổng thể xem tại `docs/ce-ext/PLAN.md`.

## Build

```sh
make                 # bản CE gốc, giữ nguyên như upstream
make USE_EXT=1       # bản CE dùng code trong as/src/ext
```

Hai biến thể dùng chung thư mục object. Khi chuyển qua lại giữa hai biến thể thì chỉ các
file trong `ext/` và file stub tương ứng bị build lại.

## Tính năng đã có

| File | Thay cho | Tính năng | Tài liệu |
|---|---|---|---|
| `cfg_ext.c` | `base/cfg_ce.c` | Mở cổng chặn config theo từng tính năng | mục bên dưới |
| `security_ext.c` | `base/security_ce.c` | Auth với user nội bộ, RBAC | `docs/ce-ext/AUTH.md` |
| `record_ext.c`, `rw_utils_ext.c`, `delete_ext.c`, `drv_common_ext.c`, `drv_ssd_ext.c`, `drv_mem_ext.c` | các bản `*_ce.c` tương ứng | Durable delete và tombstone | `docs/ce-ext/TOMBSTONE.md` |
| `tomb_raider_ext.c` | (file mới) | Tomb raider dọn tombstone | `docs/ce-ext/TOMBSTONE.md` |
| `xdr_ext.c`, `flat_ext.c` | `base/xdr_ce.c`, `storage/flat_ce.c` | XDR ship active-active | `docs/ce-ext/XDR.md` |
| `bin_ext.c` | `base/bin_ce.c` | Bin tombstone, hội tụ theo bin (P3b) | `docs/ce-ext/XDR.md` |
| `xdr_recovery_ext.c` | (file mới) | XDR last-ship-time và phục hồi (P3c) | `docs/ce-ext/XDR.md` |

## Cách tách code

- Mỗi file `*_ext.c` thay thế đúng một file stub `*_ce.c`. Danh sách cặp thay thế khai báo
  trong `Makefile.ext`:
  - `EXT_REPLACED_SOURCES`: file stub bị loại khỏi build.
  - `EXT_SOURCES`: file ext được đưa vào thay.
- File ext cài đặt **đúng chữ ký hook** đã khai báo trong header core. Không sửa core,
  trừ khi thật sự cần một hook mới. Khi thêm hook mới thì bản CE phải giữ nguyên hành vi
  cũ (xem ví dụ `as_config_allows_feature` trong `base/cfg_ce.c`).

## Bật config item enterprise-only

Bảng `EXT_FEATURES` trong `cfg_ext.c` liệt kê các config item mà bản ext sẽ cung cấp.

- Item có `ready = false`: config vẫn bị từ chối khi khởi động, với thông báo
  `'<item>' is not yet implemented (<phase>)`. Mục đích là để config không bật nhầm một
  stub không làm gì.
- Khi tính năng của một item đã cài đặt xong thì đổi `ready` thành `true`. Từ lúc đó cả
  config dạng `.conf` lẫn YAML đều chấp nhận item này.
- Lúc khởi động, server in log `ext build - ready config items: ...` liệt kê các item đã
  bật.

## Test

Thư mục `tools/ext-lab/` chứa công cụ test, chạy được mà không cần Docker:

- `cluster.sh`: chạy cluster gồm nhiều `asd` cục bộ trên `127.0.0.1`.
  - Mỗi node dùng một nhóm port riêng.
  - Có thể chạy 2 cluster song song (`-c 0`, `-c 1`) để test XDR.
- `asinfo.py`: client info protocol tối giản.
- `asclient.py`: client cho security protocol (login, quản trị user/role, info khi đã
  đăng nhập).
- `p0_smoke.sh <asd-ce> <asd-ext>`: kiểm tra cổng chặn config và việc lập cluster.
- `p1a_auth_test.py <asd-ext>`: kiểm tra protocol security, RBAC và việc dữ liệu còn
  nguyên sau restart.
- `p1a_data_test.sh <asd-ext> <client.jar>`: kiểm tra RBAC khi đọc, ghi, scan và batch
  bằng Java client chính thức.
- `p2a_tombstone_test.sh <asd-ext> <client.jar>`: kiểm tra durable delete, cold restart,
  node vào lại cluster, replica và bộ đếm theo set.
- `p2b_tomb_raider_test.sh <asd-ext> <client.jar>`: kiểm tra tomb raider với memory
  storage và với file storage (ép defrag ghi đè bản cũ bằng churn).
- `p3a_xdr_test.sh <asd-ext> <client.jar>`: kiểm tra XDR active-active giữa hai cluster
  cục bộ: ship write và delete, chống vòng lặp, đích tạm ngừng, file storage, đích bật
  security.
- `p3b_converge_test.sh <asd-ext> <client.jar> [memory|file]`: kiểm tra hội tụ theo bin. Hai
  DC ship qua `tcpproxy.py`; tắt proxy để cắt mạng, ghi xung đột ở cả hai phía, bật lại
  proxy rồi so sánh dữ liệu hai bên.
- `p3c_recovery_test.sh <asd-ext> <client.jar>`: kiểm tra phục hồi XDR khi queue tràn, khi
  restart toàn cluster nguồn, và khi một node nguồn chết.
- `cluster.sh` có thêm các tuỳ chọn:
  - `-s file`: lưu namespace ra file, để test cold start.
  - `-F <size>`: kích thước file.
  - `-N ns.conf`: chèn config vào trong namespace.
  - `start-node` / `stop-node`: bật/tắt từng node.

Cả hai biến thể đều ghi binary ra cùng một đường dẫn `target/.../bin/asd`, nên phải copy
mỗi binary ra chỗ khác sau khi build, rồi mới chạy `p0_smoke.sh`.

```sh
make && cp target/Linux-x86_64/bin/asd /tmp/asd-ce
make USE_EXT=1 && cp target/Linux-x86_64/bin/asd /tmp/asd-ext
tools/ext-lab/p0_smoke.sh /tmp/asd-ce /tmp/asd-ext
```

## Build telemetry và tên sản phẩm

- **Telemetry:** bản upstream của `build/gen_version` gửi một request về
  `build.aerospike.com` mỗi lần build. Request mang theo version, edition, arch, OS và SHA
  commit. Nhánh này đã đổi telemetry thành **opt-in**: chỉ gửi khi đặt
  `AEROSPIKE_TELEMETRY` khác `FALSE`.
- **Tên sản phẩm:** `make AS_BUILD_TYPE="Ten San Pham"` đổi chuỗi mà server báo ra trong
  info `edition`, `build` và `version`, trong log và trong `asd --version`. Mặc định giữ
  nguyên `Aerospike Community Edition`. Chỉ cho phép chữ, số, dấu cách và các ký tự
  `. _ -`. **Lưu ý:** `asadm` và một số client có thể dựa vào chuỗi này, nên cần test lại
  công cụ trước khi đổi.
