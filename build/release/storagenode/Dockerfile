ARG SN_RPC_PORT=9091

FROM idock.daumkakao.io/varlog/release-base:v0.0.1 AS varlog-sn
EXPOSE ${SN_RPC_PORT}
ENV PATH=/varlog/bin:$PATH
WORKDIR /varlog
COPY --from=idock.daumkakao.io/varlog/builder:v0.0.1 /varlog/bin/vsn.py /varlog/bin/vsn.py
COPY --from=idock.daumkakao.io/varlog/builder:v0.0.1 /varlog/pylib /varlog/pylib
COPY --from=idock.daumkakao.io/varlog/builder:v0.0.1 /varlog/bin/vsn /varlog/bin/vsn
COPY --from=idock.daumkakao.io/varlog/builder:v0.0.1 /varlog/bin/vmc /varlog/bin/vmc
COPY --from=idock.daumkakao.io/varlog/builder:v0.0.1 /varlog/bin/rpcbench_server /varlog/bin/rpcbench_server
COPY --from=idock.daumkakao.io/varlog/builder:v0.0.1 /varlog/bin/rpcbench_client /varlog/bin/rpcbench_client
COPY --from=idock.daumkakao.io/varlog/builder:v0.0.1 /bin/grpc_health_probe /varlog/tools/grpc_health_probe
COPY --from=idock.daumkakao.io/varlog/builder:v0.0.1 /varlog/build/docker-entrypoint.sh /varlog/docker-entrypoint.sh
RUN chown -R varlog:varlog /varlog
USER varlog
ENTRYPOINT ["/varlog/docker-entrypoint.sh"]
CMD ["/varlog/bin/vsn"]
