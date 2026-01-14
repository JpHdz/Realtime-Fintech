FROM rust:slim-bookworm as builder


RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    cmake \
    g++ \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /usr/src/crypto-engine


COPY Cargo.toml Cargo.lock ./
COPY crates ./crates

COPY .sqlx ./.sqlx

ENV SQLX_OFFLINE=true

RUN cargo build --release

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /usr/src/crypto-engine/target/release/ingestor .
COPY --from=builder /usr/src/crypto-engine/target/release/processor .
COPY --from=builder /usr/src/crypto-engine/target/release/gateway .

