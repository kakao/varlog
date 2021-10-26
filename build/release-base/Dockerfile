ARG PYTHON_VERSION=3.9
ARG ALPINE_VERSION=3.14

FROM mdock.daumkakao.io/python:${PYTHON_VERSION}-alpine${ALPINE_VERSION} AS release-base
RUN addgroup -S -g 1000 varlog \
    && adduser -H -S -D -h /varlog -G varlog -u 999 -s /bin/sh varlog \
    && mkdir /varlog \
    && chown -R varlog:varlog /varlog
