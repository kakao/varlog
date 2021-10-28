FROM idock.daumkakao.io/varlog/release-base:v0.0.1 AS varlog-vmc
ENV PATH=/varlog/bin:$PATH
WORKDIR /varlog
RUN chown -R varlog:varlog /varlog
USER varlog
CMD ["/varlog/bin/vmc"]
