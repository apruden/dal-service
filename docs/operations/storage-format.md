# Storage format preflight

Every data directory carries `DAL_FORMAT.json` with a
`storage_format_version`. The current binary supports storage version `2`,
durable record version `2`, application snapshot version `2`, and search engine
revision `2`. New markers record each requirement separately. All four fields
are required; incomplete markers from earlier development builds are rejected.
The application snapshot stream identifies version 2 with the `DALSNP02`
magic. The wire envelope uses `PROTOCOL_VERSION` independently of these disk
versions. The coordinated offline backup package uses format version `1`.

A new empty data directory receives the manifest before RocksDB opens. The
manifest and its directory entry are synced before startup proceeds. Startup
refuses to open a non-empty directory without the marker, a malformed marker,
or a version this binary does not support. Unknown manifest fields also fail
preflight. It does not rewrite an unknown
marker or infer compatibility from RocksDB files.
This binary accepts only the exact listed format requirements. When a format
changes during development, update the current marker and start with fresh data
directories. Earlier development directories are disposable; there is no
migration or backward-compatibility requirement.
The current data operation, version predicate, meta command, group, node-state,
serving-state, hash algorithm, key-presence, mutation-result, search-path, and
search field-kind encodings are pinned in dedicated serialization types.
Original bytes are archived in
[`format-v1-data-op.md`](evidence/format-v1-data-op.md) and the
[`format-v1` fixtures](evidence/format-v1/README.md). All current encodings and
fixtures can be revised together during development. The fixtures guard current
serialization and recovery behavior; they do not establish support for older
development binaries.

If startup reports an unclassified directory or an unsupported version, use a
fresh directory for the current development build when the old contents are
disposable. Preserve a directory with an unexpectedly damaged marker for
investigation. Do not edit `DAL_FORMAT.json` to make a mismatched directory
appear valid. Only empty directories and the current format are supported.
