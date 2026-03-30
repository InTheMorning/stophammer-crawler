FROM rust:alpine AS builder

ARG STOPHAMMER_PARSER_REPO=https://github.com/inthemorning/stophammer-parser.git
ARG STOPHAMMER_PARSER_REF=main

RUN apk add --no-cache \
    build-base \
    cmake \
    git \
    linux-headers \
    musl-dev \
    perl \
    pkgconf
WORKDIR /build

RUN git clone --depth 1 --branch "${STOPHAMMER_PARSER_REF}" \
    "${STOPHAMMER_PARSER_REPO}" \
    ./stophammer-parser

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
