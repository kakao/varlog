#!/bin/bash

scriptdir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" > /dev/null && pwd )"

grpcui \
    --plaintext \
    --import-path ${scriptdir}/../vendor \
    --import-path ${scriptdir}/../proto \
    --import-path $GOPATH/src \
    --proto mrpb/metadata_repository.proto \
    --proto mrpb/management.proto \
    $@
