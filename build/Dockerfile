ARG GO_VERSION=1.17.2
ARG PYTHON_VERSION=3.9
ARG ALPINE_VERSION=3.14
ARG GRPC_HEALTH_PROBE_VERSION=v0.4.5
ARG DEBIAN_FRONTEND=noninteractive

ARG MS_RPC_PORT=9093
ARG SN_RPC_PORT=9091
ARG MR_RPC_PORT=9092
ARG MR_RAFT_PORT=10000
ARG RPC_TEST_SERVER_PORT=9997

FROM mdock.daumkakao.io/golang:${GO_VERSION} AS builder
RUN groupadd -r varlog && useradd -r -g varlog varlog --home-dir=/varlog
ARG DEBIAN_FRONTEND
ARG PROTO_VERSION
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


FROM mdock.daumkakao.io/python:${PYTHON_VERSION}-alpine${ALPINE_VERSION} AS release-base
RUN addgroup -S -g 1000 varlog \
    && adduser -H -S -D -h /varlog -G varlog -u 999 -s /bin/sh varlog \
    && mkdir /varlog \
    && chown -R varlog:varlog /varlog


FROM release-base AS release-varlog
ENV PATH=/varlog/bin:$PATH
WORKDIR /varlog
COPY --from=builder /varlog/bin /varlog/bin
COPY --from=builder /varlog/pylib /varlog/pylib
COPY --from=builder /varlog/docker/docker-entrypoint.sh /varlog/docker-entrypoint.sh
COPY --from=builder /go/bin/dlv /varlog/tools/dlv
COPY --from=builder /bin/grpc_health_probe /varlog/tools/grpc_health_probe
RUN chown -R varlog:varlog /varlog
USER varlog


FROM release-base AS varlog-vmc
ENV PATH=/varlog/bin:$PATH
WORKDIR /varlog
RUN chown -R varlog:varlog /varlog
USER varlog
CMD ["/varlog/bin/vmc"]


FROM release-base AS varlog-vms
ARG MS_RPC_PORT
EXPOSE ${MS_RPC_PORT}
ENV PATH=/varlog/bin:$PATH
WORKDIR /varlog
COPY --from=builder /varlog/bin/vms.py /varlog/bin/vms.py
COPY --from=builder /varlog/pylib /varlog/pylib
COPY --from=builder /varlog/bin/vms /varlog/bin/vms
COPY --from=builder /varlog/bin/vmc /varlog/bin/vmc
COPY --from=builder /bin/grpc_health_probe /varlog/tools/grpc_health_probe
COPY --from=builder /varlog/docker/docker-entrypoint.sh /varlog/docker-entrypoint.sh
RUN chown -R varlog:varlog /varlog
USER varlog
ENTRYPOINT ["/varlog/docker-entrypoint.sh"]
CMD ["/varlog/bin/vms"]


FROM release-base AS varlog-mr
EXPOSE ${MR_RPC_PORT}
EXPOSE ${MR_RAFT_PORT}
ENV PATH=/varlog/bin:$PATH
WORKDIR /varlog
COPY --from=builder /varlog/bin/vmr.py /varlog/bin/vmr.py
COPY --from=builder /varlog/pylib /varlog/pylib
COPY --from=builder /varlog/bin/vmr /varlog/bin/vmr
COPY --from=builder /varlog/bin/vmc /varlog/bin/vmc
COPY --from=builder /bin/grpc_health_probe /varlog/tools/grpc_health_probe
COPY --from=builder /varlog/docker/docker-entrypoint.sh /varlog/docker-entrypoint.sh
RUN chown -R varlog:varlog /varlog
USER varlog
ENTRYPOINT ["/varlog/docker-entrypoint.sh"]
CMD ["/varlog/bin/vmr"]


FROM release-base AS varlog-sn
EXPOSE ${SN_RPC_PORT}
ENV PATH=/varlog/bin:$PATH
WORKDIR /varlog
COPY --from=builder /varlog/bin/vsn.py /varlog/bin/vsn.py
COPY --from=builder /varlog/pylib /varlog/pylib
COPY --from=builder /varlog/bin/vsn /varlog/bin/vsn
COPY --from=builder /varlog/bin/vmc /varlog/bin/vmc
COPY --from=builder /varlog/bin/rpcbench_server /varlog/bin/rpcbench_server
COPY --from=builder /varlog/bin/rpcbench_client /varlog/bin/rpcbench_client
COPY --from=builder /bin/grpc_health_probe /varlog/tools/grpc_health_probe
COPY --from=builder /varlog/docker/docker-entrypoint.sh /varlog/docker-entrypoint.sh
RUN chown -R varlog:varlog /varlog
USER varlog
ENTRYPOINT ["/varlog/docker-entrypoint.sh"]
CMD ["/varlog/bin/vsn"]

FROM release-base AS rpc-test-server
ARG RPC_TEST_SERVER_PORT
EXPOSE ${RPC_TEST_SERVER_PORT}
ENV PATH=/varlog/bin:$PATH
WORKDIR /varlog
COPY --from=builder /varlog/bin/rpc_test_server /varlog/bin/rpc_test_server
COPY --from=builder /bin/grpc_health_probe /varlog/tools/grpc_health_probe
COPY --from=builder /varlog/docker/docker-entrypoint.sh /varlog/docker-entrypoint.sh
RUN chown -R varlog:varlog /varlog
USER varlog
ENTRYPOINT ["/varlog/docker-entrypoint.sh"]
CMD ["/varlog/bin/rpc_test_server"]