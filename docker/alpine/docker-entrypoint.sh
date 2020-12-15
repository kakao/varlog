#!/bin/sh

set -e

ulimit -n 65536
ulimit -u 2048
ulimit -c unlimited

# sysctl: error setting key 'kernel.core_pattern': Read-only file system
# not recommended way: --previleged in docker run
# sysctl -w kernel.core_pattern=core.%e.%p
# sysctl vm.swappiness=0
# sysctl -p

exec "$@"
