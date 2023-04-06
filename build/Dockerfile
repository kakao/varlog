ARG GO_VERSION=1.20
ARG PYTHON_VERSION=3.9
ARG BUSYBOX_VERSION=stable
ARG GRPC_HEALTH_PROBE_VERSION=v0.4.11
ARG DEBIAN_FRONTEND=noninteractive

ARG VARLOGMR_RPC_PORT=9092
ARG VARLOGMR_RAFT_PORT=10000
ARG VARLOGADM_RPC_PORT=9093
ARG VARLOGSN_RPC_PORT=9091


FROM busybox:${BUSYBOX_VERSION} AS tools
ARG GRPC_HEALTH_PROBE_VERSION
RUN wget -qO/bin/grpc_health_probe https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/${GRPC_HEALTH_PROBE_VERSION}/grpc_health_probe-linux-amd64 \
    && chmod +x /bin/grpc_health_probe


FROM golang:${GO_VERSION} AS builder-debug
ARG DEBIAN_FRONTEND
WORKDIR /varlog
COPY . .
RUN make build GCFLAGS="-gcflags=all='-N -l' -race"


FROM golang:${GO_VERSION} AS builder-cgo
ARG DEBIAN_FRONTEND
WORKDIR /varlog
COPY . .
RUN make build GCFLAGS=""


FROM golang:${GO_VERSION} AS builder-noncgo
ARG DEBIAN_FRONTEND
WORKDIR /varlog
COPY . .
RUN CGO_ENABLED=0 make build GCFLAGS=""


FROM python:${PYTHON_VERSION}-slim AS base-cgo
ARG DEBIAN_FRONTEND
RUN groupadd -r varlog && useradd -r -g varlog varlog --home-dir=/varlog \
    && mkdir /varlog \
    && chown -R varlog:varlog /varlog
WORKDIR /varlog
USER varlog
ENV PATH=/varlog/bin:$PATH


FROM python:${PYTHON_VERSION}-alpine AS base-noncgo
RUN addgroup -S -g 1000 varlog \
    && adduser -H -S -D -h /varlog -G varlog -u 999 -s /bin/sh varlog \
    && mkdir /varlog \
    && chown -R varlog:varlog /varlog
WORKDIR /varlog
USER varlog
ENV PATH=/varlog/bin:$PATH


FROM scratch AS varlogbenchmark-noncgo
LABEL org.opencontainers.image.source=https://github.com/kakao/varlog
WORKDIR /varlog
COPY --from=builder-noncgo /varlog/bin/benchmark /varlog/bin/benchmark
CMD ["/varlog/bin/benchmark"]


FROM busybox:${BUSYBOX_VERSION}-glibc AS varlogctl-debug
LABEL org.opencontainers.image.source=https://github.com/kakao/varlog
WORKDIR /varlog
COPY --from=builder-debug /varlog/bin/varlogctl /varlog/bin/varlogctl
CMD ["/varlog/bin/varlogctl"]


FROM scratch AS varlogctl-noncgo
LABEL org.opencontainers.image.source=https://github.com/kakao/varlog
WORKDIR /varlog
COPY --from=builder-noncgo /varlog/bin/varlogctl /varlog/bin/varlogctl
CMD ["/varlog/bin/varlogctl"]


FROM busybox:${BUSYBOX_VERSION}-glibc AS varlogcli-debug
LABEL org.opencontainers.image.source=https://github.com/kakao/varlog
WORKDIR /varlog
COPY --from=builder-debug /varlog/bin/varlogcli /varlog/bin/varlogcli
CMD ["/varlog/bin/varlogcli"]


FROM scratch AS varlogcli-noncgo
LABEL org.opencontainers.image.source=https://github.com/kakao/varlog
WORKDIR /varlog
COPY --from=builder-noncgo /varlog/bin/varlogcli /varlog/bin/varlogcli
CMD ["/varlog/bin/varlogcli"]


FROM base-cgo AS varlogmr-debug
LABEL org.opencontainers.image.source=https://github.com/kakao/varlog
EXPOSE ${VARLOGMR_RPC_PORT}
EXPOSE ${VARLOGMR_RAFT_PORT}
USER varlog
WORKDIR /varlog
COPY --from=tools /bin/grpc_health_probe /varlog/tools/grpc_health_probe
COPY --from=builder-debug /varlog/bin/start_varlogmr.py /varlog/bin/start_varlogmr.py
COPY --from=builder-debug /varlog/bin/vmr /varlog/bin/vmr
COPY --from=builder-debug /varlog/bin/mrtool /varlog/bin/mrtool
COPY --from=builder-debug /varlog/bin/varlogctl /varlog/bin/varlogctl
COPY --from=builder-debug /varlog/pylib /varlog/pylib


FROM base-cgo AS varlogmr-cgo
LABEL org.opencontainers.image.source=https://github.com/kakao/varlog
EXPOSE ${VARLOGMR_RPC_PORT}
EXPOSE ${VARLOGMR_RAFT_PORT}
USER varlog
WORKDIR /varlog
COPY --from=tools /bin/grpc_health_probe /varlog/tools/grpc_health_probe
COPY --from=builder-cgo /varlog/bin/start_varlogmr.py /varlog/bin/start_varlogmr.py
COPY --from=builder-cgo /varlog/bin/vmr /varlog/bin/vmr
COPY --from=builder-cgo /varlog/bin/mrtool /varlog/bin/mrtool
COPY --from=builder-cgo /varlog/bin/varlogctl /varlog/bin/varlogctl
COPY --from=builder-cgo /varlog/pylib /varlog/pylib


FROM base-noncgo AS varlogmr-noncgo
LABEL org.opencontainers.image.source=https://github.com/kakao/varlog
EXPOSE ${VARLOGMR_RPC_PORT}
EXPOSE ${VARLOGMR_RAFT_PORT}
USER varlog
WORKDIR /varlog
COPY --from=tools /bin/grpc_health_probe /varlog/tools/grpc_health_probe
COPY --from=builder-noncgo /varlog/bin/start_varlogmr.py /varlog/bin/start_varlogmr.py
COPY --from=builder-noncgo /varlog/bin/vmr /varlog/bin/vmr
COPY --from=builder-noncgo /varlog/bin/mrtool /varlog/bin/mrtool
COPY --from=builder-noncgo /varlog/bin/varlogctl /varlog/bin/varlogctl
COPY --from=builder-noncgo /varlog/pylib /varlog/pylib


FROM base-cgo AS varlogadm-debug
LABEL org.opencontainers.image.source=https://github.com/kakao/varlog
EXPOSE ${VARLOGADM_RPC_PORT}
USER varlog
WORKDIR /varlog
COPY --from=tools /bin/grpc_health_probe /varlog/tools/grpc_health_probe
COPY --from=builder-debug /varlog/bin/varlogadm /varlog/bin/varlogadm
COPY --from=builder-debug /varlog/pylib /varlog/pylib


FROM base-cgo AS varlogadm-cgo
LABEL org.opencontainers.image.source=https://github.com/kakao/varlog
EXPOSE ${VARLOGADM_RPC_PORT}
USER varlog
WORKDIR /varlog
COPY --from=tools /bin/grpc_health_probe /varlog/tools/grpc_health_probe
COPY --from=builder-cgo /varlog/bin/varlogadm /varlog/bin/varlogadm
COPY --from=builder-cgo /varlog/pylib /varlog/pylib


FROM base-noncgo AS varlogadm-noncgo
LABEL org.opencontainers.image.source=https://github.com/kakao/varlog
EXPOSE ${VARLOGADM_RPC_PORT}
USER varlog
WORKDIR /varlog
COPY --from=tools /bin/grpc_health_probe /varlog/tools/grpc_health_probe
COPY --from=builder-noncgo /varlog/bin/varlogadm /varlog/bin/varlogadm
COPY --from=builder-noncgo /varlog/pylib /varlog/pylib


FROM base-cgo AS varlogsn-debug
LABEL org.opencontainers.image.source=https://github.com/kakao/varlog
EXPOSE ${VARLOGSN_RPC_PORT}
USER varlog
WORKDIR /varlog
COPY --from=tools /bin/grpc_health_probe /varlog/tools/grpc_health_probe
COPY --from=builder-debug /varlog/bin/start_varlogsn.py /varlog/bin/start_varlogsn.py
COPY --from=builder-debug /varlog/bin/varlogsn /varlog/bin/varlogsn
COPY --from=builder-debug /varlog/bin/varlogctl /varlog/bin/varlogctl
COPY --from=builder-debug /varlog/pylib /varlog/pylib


FROM base-cgo AS varlogsn-cgo
LABEL org.opencontainers.image.source=https://github.com/kakao/varlog
EXPOSE ${VARLOGSN_RPC_PORT}
USER varlog
WORKDIR /varlog
COPY --from=tools /bin/grpc_health_probe /varlog/tools/grpc_health_probe
COPY --from=builder-cgo /varlog/bin/start_varlogsn.py /varlog/bin/start_varlogsn.py
COPY --from=builder-cgo /varlog/bin/varlogsn /varlog/bin/varlogsn
COPY --from=builder-cgo /varlog/bin/varlogctl /varlog/bin/varlogctl
COPY --from=builder-cgo /varlog/pylib /varlog/pylib


FROM base-noncgo AS varlogsn-noncgo
LABEL org.opencontainers.image.source=https://github.com/kakao/varlog
EXPOSE ${VARLOGSN_RPC_PORT}
USER varlog
WORKDIR /varlog
COPY --from=tools /bin/grpc_health_probe /varlog/tools/grpc_health_probe
COPY --from=builder-noncgo /varlog/bin/start_varlogsn.py /varlog/bin/start_varlogsn.py
COPY --from=builder-noncgo /varlog/bin/varlogsn /varlog/bin/varlogsn
COPY --from=builder-noncgo /varlog/bin/varlogctl /varlog/bin/varlogctl
COPY --from=builder-noncgo /varlog/pylib /varlog/pylib
