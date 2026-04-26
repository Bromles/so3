# ────────────────────────────────────────────
# Stage 1 – builder
# ────────────────────────────────────────────
FROM rust:1-slim-bookworm AS builder

# protobuf-compiler is required by tonic-prost-build (crates/core/build.rs)
RUN apt-get update \
    && apt-get install -y --no-install-recommends protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Copy manifests, build scripts, and proto definitions first.
# These layers are invalidated only when dependencies or the proto contract changes,
# not on every source edit.
COPY Cargo.toml Cargo.lock ./
COPY crates/core/Cargo.toml   crates/core/Cargo.toml
COPY crates/core/build.rs     crates/core/build.rs
COPY crates/core/proto/       crates/core/proto/
COPY crates/so3/Cargo.toml              crates/so3/Cargo.toml
COPY crates/so3-maelstrom/Cargo.toml    crates/so3-maelstrom/Cargo.toml

# Compile all external dependencies with placeholder sources so Docker can
# cache them independently of application source changes.
# The dummy build is expected to fail at link time (our crates are empty);
# the important thing is that all transitive dependencies get compiled and
# cached in target/ before the real source is copied in.
RUN mkdir -p crates/core/src crates/so3/src crates/so3-maelstrom/src \
    && touch crates/core/src/lib.rs \
    && printf 'fn main() {}\n' > crates/so3/src/main.rs \
    && printf 'fn main() {}\n' > crates/so3-maelstrom/src/main.rs \
    && cargo build --release --bin so3 || true

# Copy real application source (invalidates this layer and the one below on
# source changes, but leaves the dependency layer above intact).
COPY crates/ crates/

RUN cargo build --release --bin so3

# ────────────────────────────────────────────
# Stage 2 – runtime
# ────────────────────────────────────────────
FROM debian:bookworm-slim AS runtime

# The binary is already stripped (profile.release: strip = true).
# glibc is the only runtime requirement; sqlite and tls (rustls) are bundled.
COPY --from=builder /build/target/release/so3 /usr/local/bin/so3

# SO3_OBJECT_ADDR – S3-compatible HTTP API (default 127.0.0.1:3000)
# SO3_RPC_ADDR    – internal gRPC consensus transport (default 127.0.0.1:4000)
EXPOSE 3000 4000

ENTRYPOINT ["/usr/local/bin/so3"]
