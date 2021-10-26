ARG MR_RPC_PORT=9092
ARG MR_RAFT_PORT=10000

FROM idock.daumkakao.io/varlog/release-base AS varlog-mr
EXPOSE ${MR_RPC_PORT}
EXPOSE ${MR_RAFT_PORT}
ENV PATH=/varlog/bin:$PATH
WORKDIR /varlog
COPY --from=builder /varlog/bin/vmr.py /varlog/bin/vmr.py
COPY --from=builder /varlog/pylib /varlog/pylib
COPY --from=builder /varlog/bin/vmr /varlog/bin/vmr
COPY --from=builder /varlog/bin/vmc /varlog/bin/vmc
COPY --from=builder /bin/grpc_health_probe /varlog/tools/grpc_health_probe
COPY --from=builder /varlog/build/docker-entrypoint.sh /varlog/docker-entrypoint.sh
RUN chown -R varlog:varlog /varlog
USER varlog
ENTRYPOINT ["/varlog/docker-entrypoint.sh"]
CMD ["/varlog/bin/vmr"]
