#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import glob
import json
import os
import socket
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

logger = get_logger("vmr")

DEFAULT_CLUSTER_ID = 1
RETRY_INTERVAL_SEC = 3

DEFAULT_REP_FACTOR = 1
DEFAULT_VMR_HOME = '/home/deploy/varlog-mr'
LOCAL_ADDRESS = socket.gethostbyname(socket.gethostname())


class Node:
    """Node represents metadata repository node.

    Attributes:
        raft_address: An address to use RAFT protocol
        rpc_address: An address to use RPC communications.
    """

    def __init__(self, raft_address: str, rpc_address: str):
        self.raft_address = raft_address
        self.rpc_address = rpc_address


def get_cluster_id() -> int:
    """Get Cluster ID.

    Returns:
        Cluster ID.
    """
    return int(os.getenv('CLUSTER_ID', str(DEFAULT_CLUSTER_ID)))


def get_raft_address() -> str:
    """Get local RAFT address.

    Get RAFT address defined by environment variable RAFT_ADDRESS.

    Raises:
        ValueError: An error occurred when not defined RAFT_ADDRESS.
    """
    raft_addr = os.getenv('RAFT_ADDRESS')
    if raft_addr is None:
        raise ValueError('no raft address')
    return raft_addr


def get_rpc_address() -> str:
    """Get local RPC address.

    Get local RPC address defined by environment variable RPC_ADDRESS.

    Raises:
        ValueError: An error occurred when not defined RPC_ADDRESS.
    """
    rpc_addr = os.getenv('RPC_ADDRESS')
    if rpc_addr is None:
        raise ValueError('no rpc address')
    return rpc_addr


def get_seed_address() -> str:
    """Get seed metadata repository address.

    Get seed metadata repository address defined by environment variable
    SEED_ADDRESS. If the environment variable is not defined, empty string is
    returned.
    """
    return os.getenv('SEED_ADDRESS', '')


def get_vms_addr() -> str:
    addr = os.getenv('VMS_ADDRESS')
    if addr is None:
        raise ValueError('no admin address')
    return addr


def get_raft_dir() -> str:
    home = os.getenv('VMR_HOME', DEFAULT_VMR_HOME)
    return f"{home}/raftdata"


def get_log_dir() -> str:
    home = os.getenv('VMR_HOME', DEFAULT_VMR_HOME)
    return f"{home}/log"


def get_replication_factor() -> int:
    return int(os.getenv('REPLICATION_FACTOR', str(DEFAULT_REP_FACTOR)))


def add_raft_peer() -> bool:
    """Add this node to metadata repository cluster.

    Returns:
        True if this node is added to the metadata repository cluster
        successfully.

    Raises:
        ValueError: If environment variables RAFT_ADDRESS and RPC_ADDRESS are
          not defined, it raises ValueError.
        subprocess.CalledProcessError:
    """
    raft_url = get_raft_address()
    rpc_addr = get_rpc_address()
    cmd = [
        f'{binpath}/varlogctl',
        'mr',
        'add',
        f'--raft-url={raft_url}',
        f'--rpc-addr={rpc_addr}',
        f'--admin={get_vms_addr()}'
    ]
    logger.info(f'add peer: {cmd}')
    out = subprocess.check_output(cmd)
    result = json.loads(out)
    data = result.get('data', dict())
    items = data.get('items', list())
    node_id = items[0].get('nodeId', 0)
    return node_id != 0


def prepare_vmr_home():
    home = os.getenv("VMR_HOME", DEFAULT_VMR_HOME)
    os.makedirs(home, exist_ok=True)


def exists_wal(path: str) -> bool:
    wal = glob.glob(os.path.join(f'{path}/wal', '*', '*.wal'))
    for f in wal:
        logger.info(f'wal: {f}')
    return len(wal) > 0


def fetch_peers(cluster_id: int, seed_addr: str) -> list[Node]:
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
    try:
        cmd = [f'{binpath}/mrtool',
               'describe',
               f'--cluster-id={cluster_id}',
               f'--address={seed_addr}']
        out = subprocess.check_output(cmd)
        cluster_info = json.loads(out)
        members = cluster_info.get('members', dict())
        peers = list()
        for node_id, member in members.items():
            peer = member.get('peer', '')
            endpoint = member.get('endpoint', '')
            learner = member.get('learner', False)
            if len(peer) == 0 or len(endpoint) == 0 or learner:
                continue
            peers.append(Node(peer, endpoint))
        for peer in peers:
            logger.info(f'peer: raft_addr={peer.raft_address}\trpc_addr={peer.rpc_address}')
        return peers
    except subprocess.CalledProcessError:
        return list()


def get_command(restart: bool, peers: list[Node]) -> list[str]:
    cluster_id = get_cluster_id()
    replication_factor = get_replication_factor()
    raft_address = get_raft_address()
    rpc_address = get_rpc_address()
    raft_dir = get_raft_dir()
    log_dir = get_log_dir()

    cmd = [
        f'{binpath}/vmr',
        'start',
        f'--cluster-id={cluster_id}',
        f'--raft-address={raft_address}',
        f'--bind={rpc_address}',
        f'--log-rep-factor={replication_factor}',
        f'--raft-dir={raft_dir}',
        f'--log-dir={log_dir}'
    ]
    if len(peers) > 0:
        cmd.append('--join=true')
    if restart:
        return cmd
    for peer in peers:
        cmd.append(f'--peers={peer.raft_address}')
    return cmd


def main():
    limits.set_limits()
    prepare_vmr_home()

    # If the join is true, the old write-ahead-log will be re-used.
    restart = exists_wal(get_raft_dir())
    if restart:
        logger.info(f'WAL already exists. (restart={restart})')
    # if restart:
    #     # FIXME: Removing old WAL is only for testing.
    #     try:
    #         shutil.rmtree(f'{get_raft_dir()}/wal')
    #     except FileNotFoundError:
    #         pass
    #     restart = False

    killer = Killer()
    while not killer.kill_now:
        if procutil.check_liveness('vmr'):
            time.sleep(RETRY_INTERVAL_SEC)
            continue

        procutil.kill('vmr')
        cluster_id = get_cluster_id()
        seed_address = get_seed_address()
        peers = fetch_peers(cluster_id, seed_address)
        cmd = get_command(restart, peers)
        logger.info(f'running metadata repository: {cmd}')
        subprocess.Popen(cmd)
        if not restart and len(peers) > 0:
            if not add_raft_peer():
                logger.exception('could not add mr')
        time.sleep(RETRY_INTERVAL_SEC)
    procutil.stop('vmr')


if __name__ == '__main__':
    main()
