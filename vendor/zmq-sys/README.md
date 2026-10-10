# zmq-sys 0.12.0 local build override

This directory copies the `zmq-sys` 0.12.0 Rust FFI sources (MIT or Apache-2.0)
and changes only its build script to link the installed libzmq through
`system-deps`. The upstream build calls `with_libsodium(None)`, which produces
a libzmq without CURVE in this project. DAL requires CURVE for authenticated
transport and checks `zmq_has("curve")` at startup. Build hosts need libzmq
4.3 or newer development files discoverable through pkg-config because these
bindings expose the 4.3 API. That library must also provide CURVE support for
DAL to run. CI installs `libzmq3-dev` and `libsodium-dev` before building. The
license texts are copied from the `zmq` 0.10.0 crate in the same `rust-zmq`
source project; the `zmq-sys` 0.12.0 crate archive contains no license files.
