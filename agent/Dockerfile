FROM rust:1.97.1-slim-bookworm AS build
WORKDIR /src
COPY Cargo.toml ./
COPY Cargo.lock ./
COPY src ./src
RUN cargo build --release --locked

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates wget \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system logstream \
    && useradd --system --gid logstream --home-dir /app --no-create-home logstream
WORKDIR /app
COPY --from=build --chown=logstream:logstream /src/target/release/logstream-agent /app/logstream-agent
COPY --chown=logstream:logstream agent.docker.yaml /app/agent.yaml
EXPOSE 9090
USER logstream
CMD ["/app/logstream-agent", "--config", "/app/agent.yaml"]
