#!/bin/bash

scriptdir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" > /dev/null && pwd )"

TEST_USE_LOGGER=1 \
TERM=sh \
GRPC_GO_LOG_VERBOSITY_LEVEL=99 \
GRPC_GO_LOG_SEVERITY_LEVEL=info \
go test ${scriptdir}/../test/rpc_e2e -tags=rpc_e2e -v -failfast -count 10000 -run TestRPC \
-server-addr $@
