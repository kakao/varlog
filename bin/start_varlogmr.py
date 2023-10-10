#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import glob
import json
import os
import shutil
import socket
import subprocess
import sys
import time
from typing import List

cwd = os.path.dirname(os.path.realpath(__file__))  # noqa
binpath = os.path.join(cwd, "..", "bin")  # noqa
pylib = os.path.join(cwd, "..", "pylib")  # noqa
if os.path.isdir(pylib):  # noqa
    sys.path.insert(0, pylib)  # noqa

from varlog import limits  # noqa
from varlog import procutil  # noqa
from varlog.killer import Killer  # noqa
from varlog.logger import get_logger  # noqa

APP_NAME = "vmr"
DEFAULT_CLUSTER_ID = 1
DEFAULT_RPC_PORT = 9092
DEFAULT_RAFT_PORT = 10000
DEFAULT_RETRY_INTERVAL_SECONDS = 3
DEFAULT_ADD_AFTER_SECONDS = 5

logger = get_logger(APP_NAME)


class Node:
    """Node represents metadata repository node.

    Attributes:
        raft_address: An address to use RAFT protocol
        rpc_address: An address to use RPC communications.
    """

    def __init__(self, raft_address: str, rpc_address: str):
        self.raft_address = raft_address
        self.rpc_address = rpc_address


def add_raft_peer(admin: str, raft_addr: str, rpc_addr: str) -> bool:
    """Add this node to metadata repository cluster.

    Returns:
        True if this node is added to the metadata repository cluster
        successfully.
    """
    try:
        cmd = [
            f"{binpath}/varlogctl",
            "mr",
            "add",
            f"--raft-url={raft_addr}",
            f"--rpc-addr={rpc_addr}",
            f"--admin={admin}",
        ]
        logger.info(f"add_raft_peer: {cmd}")
        out = subprocess.check_output(cmd)
        mrnode = json.loads(out)
        node_id = mrnode.get("nodeId", 0)
        return node_id != 0
    except subprocess.CalledProcessError:
        return False


def exists_wal(path: str) -> bool:
    """Check if the WAL file exists.

    Args:
        path: Path to the WAL file.

    Returns:
        True if the WAL file exists.
    """
    wal = glob.glob(os.path.join(f"{path}/wal", "*", "*.wal"))
    for f in wal:
        logger.info(f"WAL: {f}")
    return len(wal) > 0


def fetch_peers(cluster_id: int, seed_addr: str) -> List[Node]:
    """Get a list of Nodes from a seed metadata repository.

    Fetch a list of Nodes by connecting to the metadata repository identified
    by the cluster_id and seed_addr.

    Args:
        cluster_id: Cluster ID.
        seed_addr: Seed metadata repository address. Virtual IP for the metadata
          repositories also can be used.

    Returns:
        A list of Node.
    """
    peers = list()
    try:
        cmd = [
            f"{binpath}/mrtool",
            "describe",
            f"--cluster-id={cluster_id}",
            f"--address={seed_addr}",
        ]
        logger.info(f"fetch_peers: {cmd}")
        out = subprocess.check_output(cmd)
        cluster_info = json.loads(out)
        members = cluster_info.get("members", dict())
        for node_id, member in members.items():
            peer = member.get("peer", "")
            endpoint = member.get("endpoint", "")
            learner = member.get("learner", False)
            if len(peer) == 0 or len(endpoint) == 0 or learner:
                continue
            peers.append(Node(peer, endpoint))
    except subprocess.CalledProcessError:
        logger.info("could not fetch peers")

    for peer in peers:
        logger.info(
            f"peer: raft_addr={peer.raft_address}\trpc_addr={peer.rpc_address}"
        )
    return peers


def truncate(path: str) -> None:
    """Truncate directory.

    Args:
        path: directory path
    """
    shutil.rmtree(path, ignore_errors=True)
    os.makedirs(path, exist_ok=True)


def start(
    cluster_id: int,
    raft_addr: str,
    listen: str,
    seed: str,
    admin: str,
    replication_factor: int,
    reportcommitter_read_buffer_size: str,
    reportcommitter_write_buffer_size: str,
    add_after_seconds: int,
    retry_interval_seconds: int,
    raft_dir: str,
    log_dir: str = None,
) -> None:
    """Start a metadata repository node.

    Args:
        cluster_id:Cluster ID.
        raft_addr: An address to use RAFT protocol.
        listen: An address to use RPC communications.
        seed: Seed metadata repository address. For example, virtual IP for the
          metadata repositories can be used.
        admin: An address to connect to admin server.
        replication_factor: Replication factor.
        reportcommitter_write_buffer_size:
        reportcommitter_read_buffer_size:
        add_after_seconds: Add this node to the metadata repository cluster
          after this number of seconds.
        retry_interval_seconds: Retry interval seconds.
        raft_dir: Path to the RAFT directory.
        log_dir: Path to the log directory.
    """
    os.makedirs(raft_dir, exist_ok=True)
    if log_dir:
        os.makedirs(log_dir, exist_ok=True)

    # If the join is true, the old write-ahead-log will be re-used.
    restart = exists_wal(raft_dir)
    if restart:
        logger.info(f"WAL already exists. (restart={restart})")
    # if restart:
    #     # FIXME: Removing old WAL is only for testing.
    #     try:
    #         shutil.rmtree(f'{get_raft_dir()}/wal')
    #     except FileNotFoundError:
    #         pass
    #     restart = False

    killer = Killer()
    ok = False
    while not killer.kill_now:
        if ok and procutil.check_liveness(APP_NAME):
            time.sleep(retry_interval_seconds)
            continue

        procutil.kill(APP_NAME)

        peers = fetch_peers(cluster_id, seed)

        cmd = [
            f"{binpath}/vmr",
            "start",
            f"--cluster-id={cluster_id}",
            f"--raft-address={raft_addr}",
            f"--bind={listen}",
            f"--replication-factor={replication_factor}",
            f"--raft-dir={raft_dir}",
        ]

        if log_dir:
            cmd.append(f"--log-dir={log_dir}")

        if reportcommitter_read_buffer_size:
            cmd.append(
                f"--reportcommitter-read-buffer-size={reportcommitter_read_buffer_size}")

        if reportcommitter_write_buffer_size:
            cmd.append(
                f"--reportcommitter-write-buffer-size={reportcommitter_write_buffer_size}")

        if len(peers) > 0 or restart:
            cmd.append("--join=true")

        if not restart:
            for peer in peers:
                cmd.append(f"--peers={peer.raft_address}")

        logger.info(f"running metadata repository: {cmd}")
        subprocess.Popen(cmd)

        time.sleep(add_after_seconds)
        if not restart and len(peers) > 0:
            added = add_raft_peer(admin, raft_addr, listen)
            if not added:
                procutil.kill(APP_NAME)
                truncate(raft_dir)
                logger.error("could not add mr")
        ok = True
        time.sleep(retry_interval_seconds)
    procutil.stop(APP_NAME)


def main() -> None:
    addr = socket.gethostbyname(socket.gethostname())
    parser = argparse.ArgumentParser(description=APP_NAME)
    parser.add_argument("--cluster-id", default=DEFAULT_CLUSTER_ID, type=int)
    parser.add_argument("--raft-address",
                        default=f"{addr}:{DEFAULT_RAFT_PORT}")
    parser.add_argument("--listen", default=f"0.0.0.0:{DEFAULT_RPC_PORT}")
    parser.add_argument("--seed", required=True)
    parser.add_argument("--admin", required=True)
    parser.add_argument("--replication-factor", required=True, type=int)
    parser.add_argument("--reportcommitter-read-buffer-size", type=str)
    parser.add_argument("--reportcommitter-write-buffer-size", type=str)
    parser.add_argument("--raft-dir")
    parser.add_argument("--log-dir")
    parser.add_argument("--add-after-seconds",
                        default=DEFAULT_ADD_AFTER_SECONDS,
                        type=int)
    parser.add_argument("--retry-interval-seconds",
                        default=DEFAULT_RETRY_INTERVAL_SECONDS,
                        type=int)
    args = parser.parse_args()
    logger.info(args)

    limits.set_limits()
    start(
        args.cluster_id,
        args.raft_address,
        args.listen,
        args.seed,
        args.admin,
        args.replication_factor,
        args.reportcommitter_read_buffer_size,
        args.reportcommitter_write_buffer_size,
        args.add_after_seconds,
        args.retry_interval_seconds,
        args.raft_dir,
        args.log_dir,
    )


if __name__ == "__main__":
    main()
