FROM ubuntu:16.04
ENV DEBIAN_FRONTEND noninteractive

ARG GO_VERSION
ARG PROTO_VERSION
ENV GO_VERSION ${GO_VERSION}
ENV PROTO_VERSION ${PROTO_VERSION}

USER root

RUN sed 's/archive.ubuntu.com/ftp.daum.net/g' -i /etc/apt/sources.list   && \
    apt-get update                                                       && \
    apt-get -y install sudo                                              && \
    apt-get install -y build-essential                                   && \
    apt-get install -y unzip                                             && \
    apt-get install -y make                                              && \
    apt-get install -y wget                                              && \
    apt-get install -y python                                            && \
    echo '/usr/local/lib'       >> /etc/ld.so.conf                       && \
    echo 'deploy ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers

RUN set -eux; \
    wget -q -O go.tar.gz https://golang.org/dl/go${GO_VERSION}.linux-amd64.tar.gz; \
    tar xvzf go.tar.gz -C /usr/local; \
    rm -f go.tar.gz

RUN set -eux; \
    wget -q -O protoc.zip https://github.com/google/protobuf/releases/download/v${PROTO_VERSION}/protoc-${PROTO_VERSION}-linux-$(uname -m).zip; \
    unzip -o protoc.zip -d /usr/local bin/protoc; \
    unzip -o protoc.zip -d /usr/local 'include/*'; \
    chmod 755 /usr/local/bin/protoc; \
    chmod -R 755 /usr/local/include; \
    rm -f protoc.zip
   
ENV GOPATH /go

RUN mkdir -p ${GOPATH}/src ${GOPATH}/bin ${GOPATH}/pkg && chmod -R 777 ${GOPATH}

ENV PATH $GOPATH/bin:/usr/local/go/bin:$PATH

RUN useradd -m deploy -g users
RUN chown -R deploy:users /home/deploy
#RUN /sbin/ldconfig
USER deploy
WORKDIR /home/deploy
