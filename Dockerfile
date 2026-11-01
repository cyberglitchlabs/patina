# syntax=docker/dockerfile:1

# ---------------------------------------------------------------------------
# Build stage. The base image is pinned by tag AND digest: the tag says what
# it is for humans, the digest makes the build reproducible (a tag can be
# re-pushed, a digest cannot). Renovate bumps both together.
# rust 1.99.1 is the toolchain; "slim" still includes gcc, which `ring` and
# the bundled SQLite need to compile their C parts.
# ---------------------------------------------------------------------------
FROM rust:1.99.1-slim-trixie@sha256:b8e062b1c3a80183df83d9ba187cadd2b5ab84bc78d453dbfeb2fccd5f982247 AS build

WORKDIR /src

# Layer-caching trick: first build ONLY the dependencies using a dummy main,
# so this slow layer is reused until Cargo.toml / Cargo.lock change. Editing
# our own source later only rebuilds the small final step below.
COPY Cargo.toml Cargo.lock build.rs ./
RUN mkdir src && echo 'fn main() {}' > src/main.rs && : > src/lib.rs \
    && cargo build --release --locked

# Now the real source. `touch` makes cargo see the files as newer than the
# dummy ones from the previous layer (COPY keeps old timestamps).
COPY src ./src
COPY migrations ./migrations
RUN find src -name '*.rs' -exec touch {} + \
    && cargo build --release --locked \
    && cp target/release/patina /patina

# The runtime image has no shell, so the writable data directory is created
# here and copied across with the right owner (65532 = the "nonroot" user).
RUN mkdir /data-skel

# ---------------------------------------------------------------------------
# Runtime stage: distroless "cc" has glibc, libgcc and CA certificates and
# nothing else (no shell, no package manager), which is what a dynamically
# linked Rust binary needs. The :nonroot tag runs as uid 65532 by default.
# debian13 (trixie) matches the build stage's glibc.
# ---------------------------------------------------------------------------
FROM gcr.io/distroless/cc-debian13:nonroot@sha256:08476c244888bfb425bae1ae4dd7c17f093e972aab8426883d97f3c6f280f7c1

# Filled in by CI (docker/build-push-action build-args); sensible defaults for
# local builds.
ARG VERSION=dev
ARG REVISION=unknown
ARG SOURCE=https://github.com/cyberglitchlabs/patina
LABEL org.opencontainers.image.title="patina" \
      org.opencontainers.image.description="Guide and EPG aggregator that serves XMLTV feeds to Jellyfin" \
      org.opencontainers.image.source="${SOURCE}" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${REVISION}" \
      org.opencontainers.image.licenses="MIT OR Apache-2.0"

COPY --from=build /patina /usr/local/bin/patina
# /data is the ONLY writable path, so the container works with
# `--read-only` / readOnlyRootFilesystem.
COPY --from=build --chown=65532:65532 /data-skel /data

ENV PATINA_DATABASE_URL="sqlite:///data/patina.db?mode=rwc" \
    PATINA_LISTEN="0.0.0.0:8080"
WORKDIR /data
VOLUME ["/data"]
EXPOSE 8080
USER 65532:65532

# No curl in this image, so the binary checks itself (exec form: no shell).
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD ["/usr/local/bin/patina", "healthcheck"]

ENTRYPOINT ["/usr/local/bin/patina"]
