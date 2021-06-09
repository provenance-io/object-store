FROM rust:1.51 as builder

RUN rustup component add rustfmt

WORKDIR /usr/src/object-store
COPY . .
RUN cargo install --path .

RUN GRPC_HEALTH_PROBE_VERSION=v0.4.2 && \
    wget -qO/bin/grpc_health_probe https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/${GRPC_HEALTH_PROBE_VERSION}/grpc_health_probe-linux-amd64

FROM debian:buster-slim

LABEL org.opencontainers.image.source=https://github.com/provenance-io/object-store

EXPOSE 8080

COPY --from=builder /usr/local/cargo/bin/object-store /usr/local/bin/object-store

COPY --from=builder /bin/grpc_health_probe /bin/grpc_health_probe
RUN chmod +x /bin/grpc_health_probe

CMD ["object-store"]
