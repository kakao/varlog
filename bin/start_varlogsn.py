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
from typing import Tuple

cwd = os.path.dirname(os.path.realpath(__file__))  # noqa
binpath = os.path.join(cwd, "..", "bin")  # noqa
pylib = os.path.join(cwd, "..", "pylib")  # noqa
if os.path.isdir(pylib):  # noqa
    sys.path.insert(0, pylib)  # noqa

from varlog import limits  # noqa
from varlog import procutil  # noqa
from varlog.killer import Killer  # noqa
from varlog.logger import get_logger  # noqa

APP_NAME = "vsn"
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
    meta = json.loads(out)
    data = meta.get("data", dict())
    items = data.get("items", list())
    for item in items:
        if item["address"] == advertise:
            return item["storageNodeId"], True
    return random.randint(1, (2 ^ 31) - 1), False


def truncate(path: str) -> None:
    """Truncate directory.

    Args:
        path: directory path
    """
    logger.info(f"truncate path={path}")
    shutil.rmtree(path, ignore_errors=True)
    os.makedirs(path, exist_ok=True)


def register_storage_node(admin: str, advertise: str, retry: int,
                          backoff: int) -> None:
    """Register storage node to cluster.

    Args:
        admin: admin address
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


def start(
    cluster_id: int,
    listen: str,
    advertise: str,
    admin: str,
    volumes: list,
    add_after_seconds: int,
    retry_register: int,
    retry_interval_seconds: int,
    logtostderr: bool = False,
    logdir: str = None,
) -> None:
    """Start storage node.

    Args:
        cluster_id: cluster id
        listen: listen address
        advertise: advertise address
        admin: admin address
        volumes: volumes
        add_after_seconds: add after seconds
        retry_register: the number of retrial of register
        retry_interval_seconds: retry interval seconds
        logtostderr: log to stderr
        logdir: log directory
    """
    for volume in volumes:
        os.makedirs(volume, exist_ok=True)
    os.makedirs(logdir, exist_ok=True)

    killer = Killer()
    ok = False
    while not killer.kill_now:
        if ok and procutil.check_liveness(APP_NAME):
            time.sleep(retry_interval_seconds)
            continue
        try:
            procutil.kill(APP_NAME)

            snid, registered = fetch_storage_node_id(admin, advertise)
            logger.info(
                f"storage node id: {snid}, already registered: {registered}")

            cmd = [
                f"{binpath}/vsn",
                "start",
                f"--cluster-id={cluster_id}",
                f"--storage-node-id={snid}",
                f"--listen-address={listen}",
                f"--advertise-address={advertise}",
                "--disable-write-sync",
                "--disable-commit-sync",
            ]
            for volume in volumes:
                if not registered:
                    truncate(volume)
                cmd.append(f"--volumes={volume}")
            if logtostderr:
                cmd.append("--logtostderr")
            if logdir:
                cmd.append(f"--log-dir={logdir}")

            logger.info(f"vsn: cmd={cmd}")
            subprocess.Popen(cmd)

            time.sleep(add_after_seconds)
            if not registered:
                register_storage_node(admin, advertise, retry_register,
                                      retry_interval_seconds)
                logger.info("registered storage node to cluster")
            ok = True
        except (OSError, ValueError, subprocess.SubprocessError):
            ok = False
            logger.exception("could not run storage node")
        finally:
            time.sleep(retry_interval_seconds)
    procutil.stop(APP_NAME)


def main() -> None:
    addr = socket.gethostbyname(socket.gethostname())
    parser = argparse.ArgumentParser(description=APP_NAME)
    parser.add_argument("--cluster-id", default=DEFAULT_CLUSTER_ID, type=int)
    parser.add_argument("--listen", default=f"0.0.0.0:{DEFAULT_PORT}")
    parser.add_argument("--advertise", default=f"{addr}:{DEFAULT_PORT}")
    parser.add_argument("--admin", required=True)
    parser.add_argument("--volumes", nargs="+", required=True, action="append")
    parser.add_argument("--add-after-seconds",
                        default=DEFAULT_ADD_AFTER_SECONDS,
                        type=int)
    parser.add_argument("--retry-register",
                        default=DEFAULT_RETRY_REGISTER,
                        type=int)
    parser.add_argument("--retry-interval-seconds",
                        default=DEFAULT_RETRY_INTERVAL_SECONDS,
                        type=int)
    parser.add_argument("--logtostderr", action="store_true")
    parser.add_argument("--log-dir")
    args = parser.parse_args()
    logger.info(f"args: {args}")

    random.seed()
    limits.set_limits()
    start(
        args.cluster_id,
        args.listen,
        args.advertise,
        args.admin,
        args.volumes,
        args.add_after_seconds,
        args.retry_register,
        args.retry_interval_seconds,
        args.logtostderr,
        args.log_dir,
    )


if __name__ == "__main__":
    main()
