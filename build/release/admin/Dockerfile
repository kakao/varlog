ARG MS_RPC_PORT=9093

FROM idock.daumkakao.io/varlog/release-base AS varlog-vms
ARG MS_RPC_PORT
EXPOSE ${MS_RPC_PORT}
ENV PATH=/varlog/bin:$PATH
WORKDIR /varlog
COPY --from=builder /varlog/bin/vms.py /varlog/bin/vms.py
COPY --from=builder /varlog/pylib /varlog/pylib
COPY --from=builder /varlog/bin/vms /varlog/bin/vms
COPY --from=builder /varlog/bin/vmc /varlog/bin/vmc
COPY --from=builder /bin/grpc_health_probe /varlog/tools/grpc_health_probe
COPY --from=builder /varlog/build/docker-entrypoint.sh /varlog/docker-entrypoint.sh
RUN chown -R varlog:varlog /varlog
USER varlog
ENTRYPOINT ["/varlog/docker-entrypoint.sh"]
CMD ["/varlog/bin/vms"]
