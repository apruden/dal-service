# zmq-sys 0.12.0 local build override

This directory copies the `zmq-sys` 0.12.0 Rust FFI sources (MIT or Apache-2.0)
and changes only its build script to link the installed libzmq through
`system-deps`. The upstream build calls `with_libsodium(None)`, which produces
a libzmq without CURVE in this project. DAL requires CURVE for authenticated
transport and checks `zmq_has("curve")` at startup. The system libzmq must
provide CURVE support.
