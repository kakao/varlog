#!/bin/bash

scriptdir="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null && pwd)"

grpcui \
  --plaintext \
  --import-path ${scriptdir}/../vendor \
  --import-path ${scriptdir}/../proto \
  --import-path $GOPATH/src \
  --proto snpb/management.proto \
  --proto snpb/log_io.proto \
  --proto snpb/log_stream_reporter.proto \
  $@
