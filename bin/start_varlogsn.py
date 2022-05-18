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
    meta = json.loads(out)
    data = meta.get("data", dict())
    items = data.get("items", list())
    for item in items:
        if item["address"] == advertise:
            return item["storageNodeId"], True
    return random.randint(1, 2 ** 10), False


def truncate(path: str) -> None:
    """Truncate directory.

    Args:
        path: directory path
    """
    logger.info(f"truncate path={path}")
    shutil.rmtree(path, ignore_errors=True)
    os.makedirs(path, exist_ok=True)


def register_storage_node(admin: str, snid: int, advertise: str, retry: int,
                          backoff: int) -> None:
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

    killer = Killer()
    ok = False
    while not killer.kill_now:
        if ok and procutil.check_liveness(APP_NAME):
            time.sleep(args.retry_interval_seconds)
            continue
        try:
            procutil.kill(APP_NAME)

            snid, registered = fetch_storage_node_id(args.admin, args.advertise)
            logger.info(
                f"storage node id: {snid}, already registered: {registered}")

            cmd = [
                f"{binpath}/varlogsn",
                "start",
                f"--cluster-id={args.cluster_id}",
                f"--storage-node-id={snid}",
                f"--listen-address={args.listen}",
                f"--advertise-address={args.advertise}"
            ]
            for volume in args.volumes:
                if not registered:
                    truncate(volume)
                cmd.append(f"--volumes={volume}")

            # grpc options
            if args.server_read_buffer_size:
                cmd.append(
                    f"--server-read-buffer-size={args.server_read_buffer_size}")
            if args.server_write_buffer_size:
                cmd.append(
                    f"--server-write-buffer-size={args.server_write_buffer_size}")
            if args.replication_client_read_buffer_size:
                cmd.append(
                    f"--replication-client-read-buffer-size={args.replication_client_read_buffer_size}")
            if args.replication_client_write_buffer_size:
                cmd.append(
                    f"--replication-client-write-buffer-size={args.replication_client_write_buffer_size}")

            # storage options
            if args.storage_disable_wal:
                cmd.append("--storage-disable-wal")
            if args.storage_no_sync:
                cmd.append("--storage-no-sync")
            if args.storage_l0_compaction_threshold:
                cmd.append(f"--storage-l0-compaction-threshold={args.storage_l0_compaction_threshold}")
            if args.storage_l0_stop_writes_threshold:
                cmd.append(f"--storage-l0-stop-writes-threshold={args.storage_l0_stop_writes_threshold}")
            if args.storage_lbase_max_bytes:
                cmd.append(f"--storage-lbase-max-bytes={args.storage_lbase_max_bytes}")
            if args.storage_max_open_files:
                cmd.append(f"--storage-max-open-files={args.storage_max_open_files}")
            if args.storage_mem_table_size:
                cmd.append(f"--storage-mem-table-size={args.storage_mem_table_size}")
            if args.storage_mem_table_stop_writes_threshold:
                cmd.append(f"--storage-mem-table-stop-writes-threshold={args.storage_mem_table_stop_writes_threshold}")
            if args.storage_max_concurrent_compaction:
                cmd.append(f"--storage-max-concurrent-compaction={args.storage_max_concurrent_compaction}")
            if args.storage_verbose:
                cmd.append("--storage-verbose")

            # logging options
            if args.logtostderr:
                cmd.append("--logtostderr")
            if args.log_dir:
                cmd.append(f"--logdir={args.log_dir}")

            logger.info(f"varlogsn: cmd={cmd}")
            subprocess.Popen(cmd)

            time.sleep(args.add_after_seconds)
            if not registered:
                register_storage_node(args.admin, snid, args.advertise, args.retry_register,
                                      args.retry_interval_seconds)
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
    parser.add_argument("--volumes", nargs="+", required=True, action="extend",
                        type=str)
    parser.add_argument("--ballast-size", type=str)

    # grpc options
    parser.add_argument("--server-read-buffer-size", type=str)
    parser.add_argument("--server-write-buffer-size", type=str)
    parser.add_argument("--replication-client-read-buffer-size", type=str)
    parser.add_argument("--replication-client-write-buffer-size", type=str)

    # storage options
    parser.add_argument("--storage-disable-wal", action="store_true")
    parser.add_argument("--storage-no-sync", action="store_true")
    parser.add_argument("--storage-l0-compaction-threshold", type=int)
    parser.add_argument("--storage-l0-stop-writes-threshold", type=int)
    parser.add_argument("--storage-lbase-max-bytes", type=str)
    parser.add_argument("--storage-max-open-files", type=int)
    parser.add_argument("--storage-mem-table-size", type=str)
    parser.add_argument("--storage-mem-table-stop-writes-threshold", type=int)
    parser.add_argument("--storage-max-concurrent-compaction", type=int)
    parser.add_argument("--storage-verbose", action="store_true")

    # logging options
    parser.add_argument("--logtostderr", action="store_true")
    parser.add_argument("--log-dir")

    # start_varlogsn options
    parser.add_argument("--add-after-seconds",
                        default=DEFAULT_ADD_AFTER_SECONDS,
                        type=int)
    parser.add_argument("--retry-register",
                        default=DEFAULT_RETRY_REGISTER,
                        type=int)
    parser.add_argument("--retry-interval-seconds",
                        default=DEFAULT_RETRY_INTERVAL_SECONDS,
                        type=int)

    args = parser.parse_args()
    logger.info(f"args: {args}")

    random.seed()
    limits.set_limits()
    start(args)


if __name__ == "__main__":
    main()
