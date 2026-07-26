FROM rust:1.88-slim-bookworm AS builder

WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY demo/queue-demo/Cargo.toml demo/queue-demo/Cargo.lock ./demo/queue-demo/
COPY demo/queue-demo/src ./demo/queue-demo/src
RUN cargo build \
    --locked \
    --release \
    --features web \
    --manifest-path demo/queue-demo/Cargo.toml

FROM debian:bookworm-slim
COPY --from=builder \
    /src/demo/queue-demo/target/release/kneefinder-queue-demo \
    /usr/local/bin/kneefinder-queue-demo

WORKDIR /demo
USER 65532:65532
EXPOSE 8080 9000
ENTRYPOINT ["kneefinder-queue-demo"]
