ARG GO_VERSION=1.18
ARG BASE_IMAGE_NAME=python
ARG PYTHON_VERSION=3.9
ARG GRPC_HEALTH_PROBE_VERSION=v0.4.10
ARG DEBIAN_FRONTEND=noninteractive

ARG MS_RPC_PORT=9093
ARG SN_RPC_PORT=9091
ARG MR_RPC_PORT=9092
ARG MR_RAFT_PORT=10000


FROM ${BASE_IMAGE_NAME}:${PYTHON_VERSION} AS varlog-test
ARG DEBIAN_FRONTEND
ARG GO_VERSION
ARG GRPC_HEALTH_PROBE_VERSION
EXPOSE ${MS_RPC_PORT}
EXPOSE ${MR_RPC_PORT}
EXPOSE ${MR_RAFT_PORT}
EXPOSE ${SN_RPC_PORT}
RUN groupadd -r varlog && useradd -r -g varlog varlog --home-dir=/varlog \
    && apt-get -y update \
    && apt-get install -y --no-install-recommends \
        make \
        wget \
    && rm -rf /var/lib/apt/lists/* \
    && wget -qO/bin/grpc_health_probe https://github.com/grpc-ecosystem/grpc-health-probe/releases/download/${GRPC_HEALTH_PROBE_VERSION}/grpc_health_probe-linux-amd64 \
    && chmod +x /bin/grpc_health_probe \
    && wget -q -O go.tar.gz https://golang.org/dl/go${GO_VERSION}.linux-amd64.tar.gz \
    && tar xvzf go.tar.gz -C /usr/local \
    && rm -f go.tar.gz
ENV GOPATH /go
RUN mkdir -p ${GOPATH}/src ${GOPATH}/bin ${GOPATH}/pkg && chmod -R 777 ${GOPATH}
ENV PATH $GOPATH/bin:/usr/local/go/bin:/usr/local/bin:$PATH

USER root
WORKDIR /varlog
COPY . ./build
RUN mkdir /varlog/tools \
    && chown -R varlog:varlog /varlog

USER varlog
RUN cd /varlog/build \
    && make build \
    && cp -R /varlog/build/bin /varlog/bin \
    && cp -R /varlog/build/pylib /varlog/pylib \
    && cp /bin/grpc_health_probe /varlog/tools/grpc_health_probe
ENV PATH /varlog/bin:$GOPATH/bin:/usr/local/go/bin:/usr/local/bin:$PATH
