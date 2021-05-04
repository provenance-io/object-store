FROM rust:1.51 as builder

RUN rustup component add rustfmt

WORKDIR /usr/src/object-store
COPY . .
RUN cargo install --path .

FROM debian:buster-slim

LABEL org.opencontainers.image.source=https://github.com/provenance-io/object-store

EXPOSE 8080

COPY --from=builder /usr/local/cargo/bin/object-store /usr/local/bin/object-store

CMD ["object-store"]
