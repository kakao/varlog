#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import os
import subprocess
import sys
import time

cwd = os.path.dirname(os.path.realpath(__file__))  # noqa
binpath = os.path.join(cwd, "..", "bin")  # noqa
pylib = os.path.join(cwd, "..", "pylib")  # noqa
if os.path.isdir(pylib):  # noqa
    sys.path.insert(0, pylib)  # noqa

from varlog import limits  # noqa
from varlog import procutil  # noqa
from varlog.killer import Killer  # noqa
from varlog.logger import get_logger  # noqa

APP_NAME = "varlogadm"
DEFAULT_RETRY_INTERVAL_SECONDS = 3
DEFAULT_REPLICATION_FACTOR = "1"
DEFAULT_CLUSTER_ID = "1"
DEFAULT_PORT = "9093"

logger = get_logger(APP_NAME)


def start(cluster_id: int, listen: str, replication_factor: int,
          mr_address: str, logdir: str = None) -> None:
    if logdir:
        os.makedirs(logdir, exist_ok=True)

    cmd = [
        f"{binpath}/varlogadm",
        "start",
        f"--cluster-id={cluster_id}",
        f"--replication-factor={replication_factor}",
        f"--mr-address={mr_address}",
        f"--rpc-bind-address={listen}",
        "--logtostderr",
        "--logfile-retention-days=7",
        "--logfile-compression"
    ]

    if logdir:
        cmd.append(f"--logdir={logdir}")

    killer = Killer()
    while not killer.kill_now:
        if procutil.check_liveness(APP_NAME):
            time.sleep(DEFAULT_RETRY_INTERVAL_SECONDS)
            continue
        try:
            procutil.kill(APP_NAME)
            logger.info(f"running management server: {cmd}")
            subprocess.Popen(cmd)
        except (OSError, ValueError, subprocess.SubprocessError):
            logger.exception("could not run management server")
        time.sleep(DEFAULT_RETRY_INTERVAL_SECONDS)
    procutil.stop(APP_NAME)


def main() -> None:
    parser = argparse.ArgumentParser(description=APP_NAME)
    parser.add_argument("--cluster-id", default=DEFAULT_CLUSTER_ID, type=int)
    parser.add_argument("--listen", default=f"0.0.0.0:{DEFAULT_PORT}")
    parser.add_argument("--replication-factor", required=True, type=int)
    parser.add_argument("--mr-address", required=True)
    parser.add_argument("--logdir")
    args = parser.parse_args()
    logger.info(f"args: {args}")

    limits.set_limits()
    start(
        args.cluster_id,
        args.listen,
        args.replication_factor,
        args.mr_address,
        args.logdir,
    )


if __name__ == "__main__":
    main()
