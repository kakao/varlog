ARG MR_RPC_PORT=9092
ARG MR_RAFT_PORT=10000

FROM idock.daumkakao.io/varlog/release-base:v0.0.1 AS varlog-mr
EXPOSE ${MR_RPC_PORT}
EXPOSE ${MR_RAFT_PORT}
ENV PATH=/varlog/bin:$PATH
WORKDIR /varlog
COPY --from=idock.daumkakao.io/varlog/builder:v0.0.1 /varlog/bin/vmr.py /varlog/bin/vmr.py
COPY --from=idock.daumkakao.io/varlog/builder:v0.0.1 /varlog/pylib /varlog/pylib
COPY --from=idock.daumkakao.io/varlog/builder:v0.0.1 /varlog/bin/vmr /varlog/bin/vmr
COPY --from=idock.daumkakao.io/varlog/builder:v0.0.1 /varlog/bin/vmc /varlog/bin/vmc
COPY --from=idock.daumkakao.io/varlog/builder:v0.0.1 /bin/grpc_health_probe /varlog/tools/grpc_health_probe
COPY --from=idock.daumkakao.io/varlog/builder:v0.0.1 /varlog/build/docker-entrypoint.sh /varlog/docker-entrypoint.sh
RUN chown -R varlog:varlog /varlog
USER varlog
ENTRYPOINT ["/varlog/docker-entrypoint.sh"]
CMD ["/varlog/bin/vmr"]
