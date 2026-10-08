# Repository guidance

## Preproduction storage policy

This project has not been deployed to production. Treat stored data and wire
formats from earlier development revisions as disposable.

- Do not implement or plan migrations from earlier development revisions.
- Do not design for backward compatibility with earlier development binaries,
  storage directories, snapshots, or wire formats. Incidental legacy decoding is
  not a supported contract and need not be removed solely for this policy.
- When a format changes, update the current format and any fixtures or examples
  to match. A fresh data directory is an acceptable requirement.
- Keep correctness, durability, and recovery guarantees for data written by the
  current version. Do not relax those guarantees because old development data
  is disposable.
- If a plan or older document calls for migration or backward compatibility,
  follow this policy for current implementation work.
