ARG GRPC_HEALTH_PROBE_VERSION=v0.4.11
ARG DEBIAN_FRONTEND=noninteractive

ARG VARLOGMR_RPC_PORT=9092
ARG VARLOGMR_RAFT_PORT=10000
ARG VARLOGADM_RPC_PORT=9093
ARG VARLOGSN_RPC_PORT=9091


FROM busybox:1.36.1 AS tools
ARG GRPC_HEALTH_PROBE_VERSION
RUN wget -qO/bin/grpc_health_probe https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/${GRPC_HEALTH_PROBE_VERSION}/grpc_health_probe-linux-amd64 \
    && chmod +x /bin/grpc_health_probe


FROM golang:1.21 AS builder-cgo
ARG DEBIAN_FRONTEND
WORKDIR /varlog
COPY . .
RUN make build GCFLAGS=""


FROM golang:1.21 AS builder-noncgo
ARG DEBIAN_FRONTEND
WORKDIR /varlog
COPY . .
RUN CGO_ENABLED=0 make build GCFLAGS=""


FROM python:3.12 AS base-cgo
ARG DEBIAN_FRONTEND
RUN groupadd -r varlog && useradd -r -g varlog varlog --home-dir=/varlog \
    && mkdir /varlog \
    && chown -R varlog:varlog /varlog
WORKDIR /varlog
USER varlog
ENV PATH=/varlog/bin:$PATH


FROM python:3.12-alpine AS base-noncgo
RUN addgroup -S -g 1000 varlog \
    && adduser -H -S -D -h /varlog -G varlog -u 999 -s /bin/sh varlog \
    && mkdir /varlog \
    && chown -R varlog:varlog /varlog
WORKDIR /varlog
USER varlog
ENV PATH=/varlog/bin:$PATH


FROM scratch AS varlogbenchmark
LABEL org.opencontainers.image.source=https://github.com/kakao/varlog
WORKDIR /varlog
COPY --from=builder-noncgo /varlog/bin/benchmark /varlog/bin/benchmark
CMD ["/varlog/bin/benchmark"]


FROM scratch AS varlogctl
LABEL org.opencontainers.image.source=https://github.com/kakao/varlog
WORKDIR /varlog
COPY --from=builder-noncgo /varlog/bin/varlogctl /varlog/bin/varlogctl
CMD ["/varlog/bin/varlogctl"]


FROM scratch AS varlogcli
LABEL org.opencontainers.image.source=https://github.com/kakao/varlog
WORKDIR /varlog
COPY --from=builder-noncgo /varlog/bin/varlogcli /varlog/bin/varlogcli
CMD ["/varlog/bin/varlogcli"]


FROM base-noncgo AS varlogmr
LABEL org.opencontainers.image.source=https://github.com/kakao/varlog
EXPOSE ${VARLOGMR_RPC_PORT}
EXPOSE ${VARLOGMR_RAFT_PORT}
USER varlog
WORKDIR /varlog
COPY --from=tools /bin/grpc_health_probe /varlog/tools/grpc_health_probe
COPY --from=builder-noncgo /varlog/bin/start_varlogmr.py /varlog/bin/start_varlogmr.py
COPY --from=builder-noncgo /varlog/bin/varlogmr /varlog/bin/varlogmr
COPY --from=builder-noncgo /varlog/bin/mrtool /varlog/bin/mrtool
COPY --from=builder-noncgo /varlog/bin/varlogctl /varlog/bin/varlogctl
COPY --from=builder-noncgo /varlog/pylib /varlog/pylib


FROM base-noncgo AS varlogadm
LABEL org.opencontainers.image.source=https://github.com/kakao/varlog
EXPOSE ${VARLOGADM_RPC_PORT}
USER varlog
WORKDIR /varlog
COPY --from=tools /bin/grpc_health_probe /varlog/tools/grpc_health_probe
COPY --from=builder-noncgo /varlog/bin/varlogadm /varlog/bin/varlogadm
COPY --from=builder-noncgo /varlog/pylib /varlog/pylib


FROM base-cgo AS varlogsn
LABEL org.opencontainers.image.source=https://github.com/kakao/varlog
EXPOSE ${VARLOGSN_RPC_PORT}
USER varlog
WORKDIR /varlog
COPY --from=tools /bin/grpc_health_probe /varlog/tools/grpc_health_probe
COPY --from=builder-cgo /varlog/bin/start_varlogsn.py /varlog/bin/start_varlogsn.py
COPY --from=builder-cgo /varlog/bin/varlogsn /varlog/bin/varlogsn
COPY --from=builder-cgo /varlog/bin/varlogctl /varlog/bin/varlogctl
COPY --from=builder-cgo /varlog/pylib /varlog/pylib
