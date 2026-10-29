# cfsolve websolve — Railway/Render deploy
# Build Linux release (cac patch Windows trong repo deu cfg-gated, khong anh huong).
FROM rust:1.98-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends \
        cmake clang libclang-dev git build-essential pkg-config ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY . .
RUN cargo build --release --bin websolve

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=build /app/target/release/websolve /app/websolve
COPY --from=build /app/profiles /app/profiles
ENV PROFILE=/app/profiles/safari-26-ios.json
EXPOSE 8080
CMD ["/app/websolve"]
