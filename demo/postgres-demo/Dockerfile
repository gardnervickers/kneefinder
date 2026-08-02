FROM rust:1.88-slim-bookworm AS builder

WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY demo/postgres-demo/Cargo.toml demo/postgres-demo/Cargo.lock ./demo/postgres-demo/
COPY demo/postgres-demo/src ./demo/postgres-demo/src
RUN cargo build \
    --locked \
    --release \
    --features web \
    --manifest-path demo/postgres-demo/Cargo.toml

FROM debian:bookworm-slim
COPY --from=builder \
    /src/demo/postgres-demo/target/release/kneefinder-postgres-demo \
    /usr/local/bin/kneefinder-postgres-demo

RUN mkdir -p /demo/results && chown -R 65532:65532 /demo
WORKDIR /demo
USER 65532:65532
EXPOSE 8080 9000
VOLUME ["/demo/results"]
ENTRYPOINT ["kneefinder-postgres-demo"]
