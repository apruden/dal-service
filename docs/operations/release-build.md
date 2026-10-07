# Release build evidence

The GitHub Actions workflow in `.github/workflows/ci.yml` checks the declared
Rust 1.88 MSRV and the pinned Rust 1.98.1 release toolchain on Ubuntu 24.04.
Both test jobs install the native dependencies and set
`DAL_REQUIRE_CURVE_TESTS=1`, turning the CURVE-dependent process tests into
required tests instead of silent skips.

The release job audits `Cargo.lock`, builds `dal` with `--locked --release`
without the `failpoints` feature, and uploads the Linux binary with its
SHA-256 digest, Cargo metadata, CycloneDX SBOM, native package versions, and
runtime shared-library inventory. Review the dependency audit and evidence
artifact for each release; a successful CI build does not replace independent
host, restore, capacity, and pilot qualification.
