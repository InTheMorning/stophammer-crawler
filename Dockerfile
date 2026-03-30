FROM rust:alpine AS builder
RUN apk add --no-cache musl-dev
WORKDIR /build
COPY stophammer-parser ./stophammer-parser
COPY stophammer-crawler ./stophammer-crawler
RUN cd stophammer-crawler && cargo build --release

FROM alpine:3.20 AS stophammer-crawler
RUN apk add --no-cache ca-certificates \
 && addgroup -S stophammer-crawler \
 && adduser -S -G stophammer-crawler stophammer-crawler \
 && mkdir -p /data \
 && chown stophammer-crawler:stophammer-crawler /data
WORKDIR /data
COPY --from=builder /build/stophammer-crawler/target/release/stophammer-crawler /usr/local/bin/stophammer-crawler
USER stophammer-crawler
CMD ["stophammer-crawler", "gossip"]
