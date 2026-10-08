# Version 1 data operation bytes

Captured from the derived bincode encoding at commit `0434aa5`, before the
versioned serialization wrapper was introduced. Encoding uses bincode 1.3,
little endian fixed integers. These bytes are pinned by
`version_one_data_operations_keep_their_canonical_bytes` in `src/types.rs`.

| Operation | Bytes (decimal) |
| --- | --- |
| `Put { key: "k", value: "v", if_version: None }` | `0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 107, 1, 0, 0, 0, 0, 0, 0, 0, 118, 0` |
| `Delete { key: "k", if_version: Some(Number(7)) }` | `1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 107, 1, 0, 0, 0, 0, 7, 0, 0, 0, 0, 0, 0, 0` |

This fixture covers operation and predicate tags. It is not a complete durable
format fixture: meta commands, idempotency records, Raft logs, snapshots, and
whole databases still need archived examples before the release gate.
