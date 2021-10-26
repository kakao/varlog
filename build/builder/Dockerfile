ARG GO_VERSION=1.17.2
ARG GRPC_HEALTH_PROBE_VERSION=v0.4.5
ARG DEBIAN_FRONTEND=noninteractive

FROM mdock.daumkakao.io/golang:${GO_VERSION} AS builder
RUN groupadd -r varlog && useradd -r -g varlog varlog --home-dir=/varlog
ARG DEBIAN_FRONTEND
ARG GRPC_HEALTH_PROBE_VERSION
USER root
RUN set -eux \
    && apt-get -y update \
    && apt-get install -y --no-install-recommends \
        unzip \
        build-essential \
        vim \
    && rm -rf /var/lib/apt/lists/* \
    && wget -qO/bin/grpc_health_probe https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/${GRPC_HEALTH_PROBE_VERSION}/grpc_health_probe-linux-amd64 \
    && chmod +x /bin/grpc_health_probe \
    && go get github.com/go-delve/delve/cmd/dlv

WORKDIR /varlog
COPY . .
RUN chown -R varlog:varlog /varlog
USER varlog
RUN set -eux \
    && CGO_ENABLED=0 make build GOFLAGS=""
