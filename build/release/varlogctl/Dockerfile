FROM idock.daumkakao.io/varlog/release-base AS varlog-vmc
ENV PATH=/varlog/bin:$PATH
WORKDIR /varlog
RUN chown -R varlog:varlog /varlog
USER varlog
CMD ["/varlog/bin/vmc"]
