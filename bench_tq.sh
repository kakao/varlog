#!/usr/bin/env bash

set -e

go test -c ./pkg/varlog
./varlog.test -test.v -test.run - -test.bench BenchmarkTransmitQueue -test.count 20 -test.benchmem -test.timeout 10h | tee out
grep -v PreAlloc out | sed 's,/NoAlloc,,g' >out.NoAlloc
grep -v NoAlloc out | sed 's,/PreAlloc,,g' >out.PreAlloc
benchstat out.NoAlloc out.PreAlloc
