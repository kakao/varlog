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
from varlog.logger import get_logger  # noqa

logger = get_logger("vms")

CHECK_TIME = 3
DEFAULT_REP_FACTOR = "1"
DEFAULT_CLUSTER_ID = "1"
DEFAULT_RPC_PORT = "9090"


def get_mr_addr():
    addr = os.getenv("MR_ADDRESS")
    if addr is not None:
        return addr
    raise Exception(
        "MR check error! check environment value(export MR_ADDRESS=)")


def get_rpc_addr():
    return "0.0.0.0:%s" % (os.getenv("RPC_PORT", DEFAULT_RPC_PORT))


def main():
    vms = [f"{binpath}/vms",
           "start",
           "--cluster-id=%s" % os.getenv("CLUSTER_ID", DEFAULT_CLUSTER_ID),
           "--replication-factor=%s" % os.getenv("REP_FACTOR",
                                                 DEFAULT_REP_FACTOR),
           "--mr-address=%s" % get_mr_addr(),
           " --rpc-bind-address=%s" % get_rpc_addr()]

    killer = Killer()
    while not killer.kill_now:
        if procutil.check("vms"):
            time.sleep(CHECK_TIME)
            continue
        try:
            logger.info("vms is not running, start it")
            procutil.kill("vms")
            subprocess.Popen(vms)
        except (OSError, ValueError, subprocess.SubprocessError):
            logger.exception("could not execute vms")
        time.sleep(CHECK_TIME)
    procutil.stop("vms")


if __name__ == "__main__":
    main()
