#!/bin/sh

set -e

ulimit -n 100000 
ulimit -u unlimited 
ulimit -c unlimited

# sysctl: error setting key 'kernel.core_pattern': Read-only file system
# not recommended way: --previleged in docker run
# sysctl -w kernel.core_pattern=core.%e.%p
# sysctl vm.swappiness=0
# sysctl -p

exec "$@"
