ARG SN_RPC_PORT=9091

FROM idock.daumkakao.io/varlog/release-base AS varlog-sn
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
COPY --from=builder /varlog/build/docker-entrypoint.sh /varlog/docker-entrypoint.sh
RUN chown -R varlog:varlog /varlog
USER varlog
ENTRYPOINT ["/varlog/docker-entrypoint.sh"]
CMD ["/varlog/bin/vsn"]
