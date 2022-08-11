#!/usr/bin/env bash

echo $PAT | docker login ghcr.io -u $USERNAME --password-stdin

docker build -t ghcr.io/kakao/varlog-protobuf:$TAG . && \
docker push "ghcr.io/kakao/varlog-protobuf:$TAG"
