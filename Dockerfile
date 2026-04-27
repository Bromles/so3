# ────────────────────────────────────────────
# Stage 1 – builder
#
# rust:alpine uses musl libc by default, so the resulting binary is
# fully statically linked — no runtime libc dependency.
# ────────────────────────────────────────────
FROM rust:1-alpine AS builder

# build-base  – gcc + musl-dev + make, required to compile bundled C code
#               (libsqlite3-sys, ring, etc.)
# protobuf    – protoc binary required by tonic-prost-build (crates/core/build.rs)
RUN apk add --no-cache build-base protobuf

WORKDIR /build

# ── dependency caching layer ─────────────────────────────────────────────────
# Copy manifests, build scripts, and proto definitions.
# These layers are only invalidated when dependencies or the proto contract change.
COPY Cargo.toml Cargo.lock ./
COPY crates/core/Cargo.toml   crates/core/Cargo.toml
COPY crates/core/build.rs     crates/core/build.rs
COPY crates/core/proto/       crates/core/proto/
COPY crates/so3/Cargo.toml              crates/so3/Cargo.toml
COPY crates/so3-maelstrom/Cargo.toml    crates/so3-maelstrom/Cargo.toml

# Compile all external dependencies using placeholder sources.
# The build is expected to fail at link time (empty lib.rs); what matters is
# that every transitive dependency is compiled and cached in target/ before the
# real source lands in the next layer.
RUN mkdir -p crates/core/src crates/so3/src crates/so3-maelstrom/src \
    && touch crates/core/src/lib.rs \
    && printf 'fn main() {}\n' > crates/so3/src/main.rs \
    && printf 'fn main() {}\n' > crates/so3-maelstrom/src/main.rs \
    && cargo build --release --bin so3 || true

# ── application build ─────────────────────────────────────────────────────────
# Copying real source invalidates only this layer and the one below.
# All cached dependency artifacts above remain intact.
COPY crates/ crates/
RUN cargo build --release --bin so3

# ────────────────────────────────────────────
# Stage 2 – runtime
#
# FROM scratch: zero base OS — image size equals the stripped binary.
# Works because the binary is fully static (musl, bundled sqlite, rustls).
# Docker still mounts /etc/resolv.conf and /etc/hosts at runtime, so DNS
# resolution and networking work without any extra files.
# ────────────────────────────────────────────
FROM scratch AS runtime

# The binary is already stripped (profile.release: strip = true).
COPY --from=builder /build/target/release/so3 /so3

# SO3_OBJECT_ADDR – S3-compatible HTTP API (default 127.0.0.1:3000)
# SO3_RPC_ADDR    – internal gRPC consensus transport (default 127.0.0.1:4000)
EXPOSE 3000 4000

ENTRYPOINT ["/so3"]
