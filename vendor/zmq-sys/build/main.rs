fn main() {
    // The upstream vendored build disables CURVE when it passes no libsodium
    // location. Use the system library selected by pkg-config; DAL checks CURVE
    // support at startup before opening its authenticated transport.
    system_deps::Config::new()
        .probe()
        .expect("libzmq development files are required (with CURVE support)");
}
