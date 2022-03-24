#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import subprocess
import sys
import time

cwd = os.path.dirname(os.path.realpath(__file__))  # noqa
binpath = os.path.join(cwd, "..", "bin")  # noqa
pylib = os.path.join(cwd, "..", "pylib")  # noqa
if os.path.isdir(pylib):  # noqa
    sys.path.insert(0, pylib)  # noqa

from varlog.killer import Killer  # noqa
from varlog import procutil  # noqa
from varlog import limits  # noqa
from varlog.logger import get_logger  # noqa

logger = get_logger("varlogadm")

RETRY_INTERVAL_SEC = 3
DEFAULT_REP_FACTOR = "1"
DEFAULT_CLUSTER_ID = "1"
DEFAULT_RPC_PORT = "9093"
ENV_KEY_MR_ADDRESS = "MR_ADDRESS"
ENV_KEY_RPC_PORT = "RPC_PORT"


def get_mr_addr():
    addr = os.getenv(ENV_KEY_MR_ADDRESS)
    if addr is not None:
        return addr
    raise Exception(f"no mr address, set {ENV_KEY_MR_ADDRESS}")


def get_rpc_addr():
    return "0.0.0.0:%s" % (os.getenv(ENV_KEY_RPC_PORT, DEFAULT_RPC_PORT))


def main():
    limits.set_limits()
    cluster_id = os.getenv("CLUSTER_ID", DEFAULT_CLUSTER_ID)
    rep_factor = os.getenv("REP_FACTOR", DEFAULT_REP_FACTOR)
    mr_addr = get_mr_addr()
    rpc_addr = get_rpc_addr()

    cmd = [
        f"{binpath}/varlogadm",
        "start",
        f"--cluster-id={cluster_id}",
        f"--replication-factor={rep_factor}",
        f"--mr-address={mr_addr}",
        f"--rpc-bind-address={rpc_addr}"
    ]

    killer = Killer()
    while not killer.kill_now:
        if procutil.check_liveness("varlogadm"):
            time.sleep(RETRY_INTERVAL_SEC)
            continue
        try:
            procutil.kill("varlogadm")
            logger.info(f"running management server: {cmd}")
            subprocess.Popen(cmd)
        except (OSError, ValueError, subprocess.SubprocessError):
            logger.exception("could not run management server")
        time.sleep(RETRY_INTERVAL_SEC)
    procutil.stop("varlogadm")


if __name__ == "__main__":
    main()
