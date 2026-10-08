# Storage format preflight

Every data directory carries `DAL_FORMAT.json` with a
`storage_format_version`. The current binary supports storage version `1`,
durable record version `1`, application snapshot version `1`, and search engine
revision `2`. New markers record each requirement separately. A version-1
marker containing only `storage_format_version` has the historical defaults
`records=1`, `snapshots=1`, and `search=2`. These defaults are fixed to the
initial format and do not change when a future binary adds a format.
The application snapshot stream identifies version 1 with the `DALSNP01`
magic. The wire envelope uses `PROTOCOL_VERSION` independently of these disk
versions. Backup format versioning begins when a backup artifact exists.

A new empty data directory receives the manifest before RocksDB opens. The
manifest and its directory entry are synced before startup proceeds. Startup
refuses to open a non-empty directory without the marker, a malformed marker,
or a version this binary does not support. Unknown manifest fields also fail
preflight. It does not rewrite an unknown
marker or infer compatibility from RocksDB files.
This binary accepts only the exact listed format requirements. Binaries from
before the independent fields were added ignore unknown JSON fields while the
storage version remains 1. A future incompatible format must therefore bump
`storage_format_version` as well as its specific requirement, so those older
binaries reject the marker before opening storage. A future search-engine
change needs a staged rebuild and marker publication procedure, even though
the index is derived from authoritative records.

There is no legacy import or migration command yet. If startup reports an
unclassified directory or an unsupported version, stop the node and preserve
the directory. Do not delete `DAL_FORMAT.json`, copy it from another node, or
edit the version by hand. Use a release-provided migration procedure once one
is available; until then, only empty directories and format version `1` are
supported.
