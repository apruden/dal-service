# Version 1 meta command fixtures

The four `meta-*.bin` files were captured from the derived `MetaCommand`
bincode encoding at commit `d7ea563`, before its versioned remote serialization
type was added. They cover command tags 0, 3, 4, and 12 with nested cluster,
directory-state, group, and search-name fields. The
`version_one_meta_commands_replay_from_archived_bytes` regression compares new
encodings with these files and decodes each archived command.

The group, node, serving, hash, presence, and mutation files were captured
from derived bincode encodings at commit `154d2d4`, before their versioned
remote serialization types were added. The
`version_one_nested_records_replay_from_archived_bytes` regression pins every
variant of those six enums.

These are initial format fixtures. The complete production compatibility gate
also needs every meta command variant, retained Raft logs, identity and
idempotency records, snapshots, and a complete database captured from the release
candidate.
