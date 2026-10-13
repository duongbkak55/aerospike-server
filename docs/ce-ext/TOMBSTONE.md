# Durable delete & tombstone (P2a) – bản build `USE_EXT=1`

Kế hoạch tổng xem `PLAN.md`. Phần dọn tombstone (tomb raider) là P2b, **chưa làm**.

## Vấn đề

Với bản CE, lệnh delete chỉ xoá record khỏi index; bản ghi cũ vẫn còn trên đĩa. Hậu quả:

- **Cold restart:** server đọc lại đĩa và record đã xoá **sống lại**.
- **Node cũ vào lại cluster:** node từng vắng mặt lúc delete vẫn giữ bản cũ, rồi migrate bản
  đó trở lại. Record cũng sống lại.

Hai trường hợp trên đã được tái hiện bằng `p2a_tombstone_test.sh`. Với delete thường,
20/20 record sống lại sau cold restart và 50/50 record sống lại sau khi node cũ vào lại.

## Cách dùng

Client gửi delete với cờ durable. Đây là API sẵn có của mọi client chính thức:

- Java: `WritePolicy.durableDelete = true`, `BatchDeletePolicy.durableDelete = true`.
- Các client khác: tuỳ chọn `durable_delete`.
- kvbench: `db.aerospike.write.durableDelete: true`.

**Không cần cấu hình gì phía server.** Delete không có cờ durable vẫn chạy như CE.

## Hành vi

- Durable delete để lại một **tombstone**: index entry vẫn giữ, được đánh dấu
  `tombstone = 1`, generation tăng, có LUT mới, `void-time = 0` (không hết hạn), và có một
  bản ghi không bin trên storage.
- Đọc, batch, scan, query và UDF đều coi tombstone là không tồn tại. Read trả `not found`,
  durable delete lần nữa cũng trả `not found`.
- Ghi đè lên tombstone làm record sống lại; generation tiếp nối từ tombstone.
- **Replica và migrate:** tombstone được gửi như một record không bin. Conflict resolution
  theo generation/LUT nên tombstone mới hơn thắng bản cũ hơn.
- **Cold start:** bản ghi không bin trên đĩa được nạp lại thành tombstone. Nó thắng các bản
  live cũ hơn nằm trên đĩa.
- **Thống kê:**
  - Namespace: `tombstones`, `master_tombstones`, `prole_tombstones`.
  - Set (`sets/<ns>/<set>`): `objects` không tính tombstone; tombstone đếm riêng ở
    `tombstones`.
- **Truncate** xoá cả tombstone.

## Cài đặt

| File ext | Thay cho | Nội dung |
|---|---|---|
| `rw_utils_ext.c` | `transaction/rw_utils_ce.c` | `convert_to_write` / `convert_batched_to_write`: đổi durable delete thành write có op `DELETE_ALL` và TTL không hết hạn. `transition_delete_metadata` đặt cờ tombstone. `write_delete_record` giữ tombstone lại trong index. `unwind_index_metadata` khôi phục cờ khi write thất bại. |
| `record_ext.c` | `base/record_ce.c` | `as_record_is_live` / `is_binless` theo cờ tombstone. Bộ đếm namespace, set và partition (`ext_record_count_transition`). Set index chỉ chứa record còn sống. `replace_index_metadata`: record nhận từ node khác mà không có bin thì là tombstone. |
| `delete_ext.c` | `transaction/delete_ce.c` | Delete thường gặp tombstone thì trả `not found` và giữ nguyên tombstone. |
| `drv_common_ext.c`, `drv_ssd_ext.c`, `drv_mem_ext.c` | bản `*_ce.c` tương ứng | Ghi tombstone xuống storage (CE bỏ qua mọi write 0 bin). Cold start nạp lại tombstone và chuyển trạng thái live ↔ tombstone, kể cả set index và bộ đếm. |

Core **không phải sửa**: các hook EE và các chỗ kiểm tra `as_record_is_live` đã có sẵn.

## Hạn chế hiện tại

- **Chưa có tomb raider (P2b):** tombstone tồn tại mãi. Mỗi tombstone tốn 64B index, cộng
  một bản ghi nhỏ trên storage. Cần theo dõi stat `tombstones` cho tới khi có P2b. Cách giải
  phóng hiện có là `truncate`.
- Write xoá bin cuối cùng mà không durable vẫn để lại tombstone trên replica: master gửi
  pickle 0 bin, giống CE. Tombstone này vô hại vì không hiển thị, và sẽ do P2b dọn.
- Durable delete đi theo đường write, nên được đếm vào thống kê write (`client_write_*`),
  không vào `client_delete_*`. Durable delete cũng bị chặn khi namespace ở trạng thái
  stop-writes.
- Durable delete tốn một lần ghi storage (bản ghi tombstone nhỏ), delete thường thì không.
  Chưa đo overhead bằng kvbench.

## Test

```sh
tools/ext-lab/p2a_tombstone_test.sh <asd-ext> <kvbench.jar>
```

28 kiểm tra:
- 1 node lưu file, cold restart, batch.
- 2 node RF2: tombstone trên master và replica, node cũ vào lại cluster, đọc từ replica khi
  mất node master.
- Memory storage.
- Bộ đếm theo set và truncate.
