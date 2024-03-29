#!/bin/bash

scriptdir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" > /dev/null && pwd )"

grpcui \
    --plaintext \
    --import-path ${scriptdir}/../vendor \
    --import-path ${scriptdir}/../proto \
    --import-path $GOPATH/src \
    --proto admpb/admin.proto \
    $@
