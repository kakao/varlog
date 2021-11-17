ARG GO_VERSION=1.17.2
ARG ALPINE_VERSION=3.14
ARG GRPC_HEALTH_PROBE_VERSION=v0.4.5

FROM mdock.daumkakao.io/golang:${GO_VERSION}-alpine AS builder
RUN addgroup -S -g 1000 varlog \
    && adduser -H -S -D -h /varlog -G varlog -u 999 -s /bin/sh varlog \
    && mkdir /varlog \
    && chown -R varlog:varlog /varlog
ARG GRPC_HEALTH_PROBE_VERSION
USER root
RUN apk add --update --no-cache alpine-sdk python3 bash \
    && ln -sf python3 /usr/bin/python \
    && python3 -m ensurepip \
    && pip3 install --no-cache --upgrade pip setuptools \
    && wget -qO/bin/grpc_health_probe https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/${GRPC_HEALTH_PROBE_VERSION}/grpc_health_probe-linux-amd64 \
    && chmod +x /bin/grpc_health_probe \
    && go install github.com/go-delve/delve/cmd/dlv@latest

WORKDIR /varlog
USER varlog
