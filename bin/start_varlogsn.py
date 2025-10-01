#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import json
import os
import random
import shutil
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import List, Tuple

cwd = os.path.dirname(os.path.realpath(__file__))  # noqa
binpath = os.path.join(cwd, "..", "bin")  # noqa
pylib = os.path.join(cwd, "..", "pylib")  # noqa
if os.path.isdir(pylib):  # noqa
    sys.path.insert(0, pylib)  # noqa

from varlog.killer import Killer  # noqa
from varlog.logger import get_logger  # noqa

from varlog import limits  # noqa
from varlog import procutil  # noqa

APP_NAME = "varlogsn"
DEFAULT_CLUSTER_ID = 1
DEFAULT_PORT = 9091
DEFAULT_RETRY_INTERVAL_SECONDS = 3
DEFAULT_RETRY_REGISTER = 5
DEFAULT_ADD_AFTER_SECONDS = 5

logger = get_logger(APP_NAME)


def fetch_storage_node_id(admin: str, advertise: str) -> Tuple[int, bool]:
    """Fetch storage node id.

    Args:
        admin: admin address
        advertise: advertise address

    Returns:
        storage node id and boolean represented if already registered

    Raises:
        subprocess.CalledProcessError: if command failed
    """
    cmd = [f"{binpath}/varlogctl", "sn", "describe", f"--admin={admin}"]
    logger.info(f"fetch_storage_node_id: cmd={cmd}")
    out = subprocess.check_output(cmd)
    snms = json.loads(out)
    for snm in snms:
        if snm["address"] == advertise:
            return snm["storageNodeId"], True
    return random.randint(1, 2**10), False


def get_data_dirs(admin: str, snid: int) -> List[str]:
    cmd = [f"{binpath}/varlogctl", "sn", "get", f"--admin={admin}", f"--snid={snid}"]
    out = subprocess.check_output(cmd)
    snm = json.loads(out)
    logStreamReplicas = snm.get("logStreamReplicas")
    if not logStreamReplicas:
        logStreamReplicas = []
    return [logStreamReplica["path"] for logStreamReplica in logStreamReplicas]


def truncate(path: str) -> None:
    """Truncate directory.

    Args:
        path: directory path
    """
    logger.info(f"truncate path={path}")
    shutil.rmtree(path, ignore_errors=True)
    os.makedirs(path, exist_ok=True)


def register_storage_node(
    admin: str, snid: int, advertise: str, retry: int, backoff: int
) -> None:
    """Register storage node to cluster.

    Args:
        admin: admin address
        snid: storage node id
        advertise: advertise address
        retry: the number of retrial of register
        backoff: backoff duration in seconds

    Raises:
        subprocess.CalledProcessError: if command failed
    """
    cmd = [
        f"{binpath}/varlogctl",
        "sn",
        "add",
        f"--storage-node-id={snid}",
        f"--storage-node-address={advertise}",
        f"--admin={admin}",
    ]
    logger.info(f"register_storage_node: cmd={cmd}")
    for i in range(0, retry):
        try:
            subprocess.check_output(cmd)
            return
        except subprocess.CalledProcessError:
            logger.info(f"register_storage_node: failed, try={i + 1}/{retry}")
            if i < retry - 1:
                time.sleep(backoff)
    raise subprocess.CalledProcessError


def start(args: argparse.Namespace) -> None:
    """Start storage node.

    Args:
        args: arguments
    """

    for volume in args.volumes:
        os.makedirs(volume, exist_ok=True)
    if args.log_dir:
        os.makedirs(args.log_dir, exist_ok=True)

    volumes = [str(Path(volume).resolve()) for volume in args.volumes]

    killer = Killer()
    ok = False
    while not killer.kill_now:
        if ok and procutil.check_liveness(APP_NAME):
            time.sleep(args.retry_interval_seconds)
            continue
        try:
            procutil.kill(APP_NAME)

            snid, registered = fetch_storage_node_id(args.admin, args.advertise)
            logger.info(f"storage node id: {snid}, already registered: {registered}")

            datadirs = set()
            if registered:
                for datadir in get_data_dirs(args.admin, snid):
                    if not os.path.isdir(datadir):
                        logger.info(f"no data directory: {datadir}")
                        sys.exit(1)
                    datadirs.add(datadir)

            for volume in volumes:
                sndir = f"cid_{args.cluster_id}_snid_{snid}/*"
                for datadir in Path(volume).glob(sndir):
                    if not datadir.is_dir():
                        continue
                    if str(datadir) not in datadirs:
                        logger.info(f"remove garbage data directory {datadir}")
                        shutil.rmtree(datadir, ignore_errors=True)

            cmd = [
                f"{binpath}/varlogsn",
                "start",
                f"--cluster-id={args.cluster_id}",
                f"--storage-node-id={snid}",
                f"--listen-address={args.listen}",
                f"--advertise-address={args.advertise}",
            ]
            for volume in volumes:
                cmd.append(f"--volumes={volume}")

            if args.ballast_size:
                cmd.append(f"--ballast-size={args.ballast_size}")

            if args.append_pipeline_size:
                cmd.append(f"--append-pipeline-size={args.append_pipeline_size}")

            # logging options
            if args.logtostderr:
                cmd.append("--logtostderr")
            if args.log_dir:
                cmd.append(f"--logdir={args.log_dir}")

            logger.info(f"varlogsn: cmd={cmd}")
            subprocess.Popen(cmd)

            time.sleep(args.add_after_seconds)
            if not registered:
                register_storage_node(
                    args.admin,
                    snid,
                    args.advertise,
                    args.retry_register,
                    args.retry_interval_seconds,
                )
                logger.info("registered storage node to cluster")
            ok = True
        except (OSError, ValueError, subprocess.SubprocessError):
            ok = False
            logger.exception("could not run storage node")
        finally:
            time.sleep(args.retry_interval_seconds)
    procutil.stop(APP_NAME)


def main() -> None:
    addr = socket.gethostbyname(socket.gethostname())
    parser = argparse.ArgumentParser(description=APP_NAME)
    parser.add_argument("--cluster-id", default=DEFAULT_CLUSTER_ID, type=int)
    parser.add_argument("--listen", default=f"0.0.0.0:{DEFAULT_PORT}")
    parser.add_argument("--advertise", default=f"{addr}:{DEFAULT_PORT}")
    parser.add_argument("--admin", required=True)
    parser.add_argument(
        "--volumes", nargs="+", required=True, action="extend", type=str
    )
    parser.add_argument("--ballast-size", type=str)
    parser.add_argument("--append-pipeline-size", type=int)

    # logging options
    parser.add_argument("--logtostderr", action="store_true")
    parser.add_argument("--log-dir")

    # start_varlogsn options
    parser.add_argument(
        "--add-after-seconds", default=DEFAULT_ADD_AFTER_SECONDS, type=int
    )
    parser.add_argument("--retry-register", default=DEFAULT_RETRY_REGISTER, type=int)
    parser.add_argument(
        "--retry-interval-seconds", default=DEFAULT_RETRY_INTERVAL_SECONDS, type=int
    )

    args = parser.parse_args()
    logger.info(f"args: {args}")

    random.seed()
    limits.set_limits()
    start(args)


if __name__ == "__main__":
    main()
