ARG MS_RPC_PORT=9093
ARG MR_RPC_PORT=9092
ARG MR_RAFT_PORT=10000
ARG SN_RPC_PORT=9091

FROM idock.daumkakao.io/varlog/builder:v0.0.2 AS varlog-all-in-one
ARG MS_RPC_PORT
EXPOSE ${MS_RPC_PORT}
EXPOSE ${MR_RPC_PORT}
EXPOSE ${MR_RAFT_PORT}
EXPOSE ${SN_RPC_PORT}
ENV PATH=/varlog/bin:$PATH

WORKDIR /varlog
COPY . ./build

USER root
RUN mkdir /varlog/tools \
    && cp /bin/grpc_health_probe /varlog/tools/grpc_health_probe \
    && chown -R varlog:varlog /varlog

USER varlog
RUN cd /varlog/build \
    && CGO_ENABLED=0 make build GOFLAGS="" \
    && cp /varlog/build/build/docker-entrypoint.sh /varlog/docker-entrypoint.sh \
    && cp -R /varlog/build/bin /varlog/bin \
    && cp -R /varlog/build/pylib /varlog/pylib
ENTRYPOINT ["/varlog/docker-entrypoint.sh"]
