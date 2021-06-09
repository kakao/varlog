#!/bin/bash 

scriptdir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" > /dev/null && pwd )"

${scriptdir}/../bin/benchmark start \
    --cid=1 \
    --mraddr=10.202.62.171:9092 \
    --mraddr=10.202.133.119:9092 \
    --mraddr=10.202.62.236:9092 \
    --clients 10 \
    --data-size 128B \
    --max-ops-per-client 1000

