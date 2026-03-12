FROM rust:1.87-alpine AS builder
RUN apk add --no-cache musl-dev
WORKDIR /build
COPY stophammer-parser ./stophammer-parser
COPY stophammer-crawler ./stophammer-crawler
RUN cd stophammer-crawler && cargo build --release

FROM alpine:3.20
RUN addgroup -S crawler && adduser -S -G crawler crawler
WORKDIR /app
COPY --from=builder /build/stophammer-crawler/target/release/stophammer-crawler /app/stophammer-crawler
USER crawler
ENTRYPOINT ["/app/stophammer-crawler"]
