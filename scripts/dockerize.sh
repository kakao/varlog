#!/bin/bash

set -eux

scriptdir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" > /dev/null && pwd )"

TAG=0.0.1-alpine

docker login ***REMOVED*** 
for name in mc ms mr sn;
do
    docker build --target release-${name} -f ${scriptdir}/../docker/alpine/Dockerfile -t ***REMOVED***/varlog/${name}:${TAG} .
    docker push ***REMOVED***/varlog/${name}:${TAG}
done
