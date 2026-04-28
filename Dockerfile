FROM rust:1.88-slim AS builder

RUN apt-get update && apt-get install -y pkg-config libssl-dev && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY crates/ crates/
COPY src/ src/
RUN cargo build --release

FROM gcr.io/distroless/cc-debian12

COPY --from=builder /app/target/release/cdc-rs /

EXPOSE 8080

ENTRYPOINT ["/cdc-rs"]
