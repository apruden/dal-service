# Storage format preflight

Every data directory carries `DAL_FORMAT.json` with a
`storage_format_version`. The current binary supports version `1`.

A new empty data directory receives the manifest before RocksDB opens. The
manifest and its directory entry are synced before startup proceeds. Startup
refuses to open a non-empty directory without the marker, a malformed marker,
or a version this binary does not support. It does not rewrite an unknown
marker or infer compatibility from RocksDB files.

There is no legacy import or migration command yet. If startup reports an
unclassified directory or an unsupported version, stop the node and preserve
the directory. Do not delete `DAL_FORMAT.json`, copy it from another node, or
edit the version by hand. Use a release-provided migration procedure once one
is available; until then, only empty directories and format version `1` are
supported.
