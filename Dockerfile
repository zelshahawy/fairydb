FROM rust:1.82 AS builder
WORKDIR /usr/src/fairydb

# 1) Copy everything and build all binaries
COPY . .
RUN cargo build --release --bins

# 2) Create the slim runtime image
FROM ubuntu:22.04
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/src/fairydb/target/release/server     /usr/local/bin/server
COPY --from=builder /usr/src/fairydb/target/release/cli-crusty /usr/local/bin/cli-crusty

