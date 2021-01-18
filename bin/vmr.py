#!/usr/bin/python
# -*- coding: utf-8 -*-

import grp
import json
import os
import pwd
import shutil
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

RETRY_INTERVAL_SEC = 3
DEFAULT_CLUSTER_ID = "1"
DEFAULT_REP_FACTOR = "1"
DEFAULT_RPC_PORT = "9092"
DEFAULT_RAFT_PORT = "10000"
DEFAULT_VMR_HOME = "/home/deploy/varlog-mr"
LOCAL_ADDRESS = socket.gethostbyname(socket.gethostname())


def get_raft_url():
    raft_port = os.getenv("RAFT_PORT", DEFAULT_RAFT_PORT)
    return f"http://{LOCAL_ADDRESS}:{raft_port}"


def get_rpc_addr():
    rpc_port = os.getenv("RPC_PORT", DEFAULT_RPC_PORT)
    return f"{LOCAL_ADDRESS}:{rpc_port}"


def get_vms_addr():
    addr = os.getenv("VMS_ADDRESS")
    if addr is None:
        raise Exception("no vms address")
    return addr


def get_raft_dir():
    home = os.getenv("VMR_HOME", DEFAULT_VMR_HOME)
    return f"{home}/raftdata"


def get_log_dir():
    home = os.getenv("VMR_HOME", DEFAULT_VMR_HOME)
    return f"{home}/log"


def get_info():
    try:
        out = subprocess.check_output([
            f"{binpath}/vmc",
            "mr",
            "info"
        ])
        info = json.loads(out)
        members = info["members"].values()
        return info["replicationFactor"], members
    except Exception:
        logger.exception("could not get peers")
        return int(os.getenv("REP_FACTOR", DEFAULT_REP_FACTOR)), None


def add_raft_peer():
    try:
        raft_url = get_raft_url()
        rpc_addr = get_rpc_addr()

        out = subprocess.check_output([
            f"{binpath}/vmc",
            "mr",
            "add",
            f"--raft-url={raft_url}",
            f"--rpc-addr={rpc_addr}"
        ])
        info = json.loads(out)
        node_id = info["nodeId"]
        return node_id != "0"
    except Exception:
        logger.exception("could not add peer")
        return False


def prepare_vmr_home():
    home = os.getenv("VMR_HOME", DEFAULT_VMR_HOME)
    os.makedirs(home, exist_ok=True)


def main():
    limits.set_limits()
    prepare_vmr_home()

    cluster_id = os.getenv("CLUSTER_ID", DEFAULT_CLUSTER_ID)
    rep_factor, peers = get_info()
    raft_url = get_raft_url()
    rpc_addr = get_rpc_addr()
    raft_dir = get_raft_dir()
    log_dir = get_log_dir()

    common_command = [
        f"{binpath}/vmr",
        "start",
        f"--cluster-id={cluster_id}",
        f"--log-rep-factor={rep_factor}",
        f"--raft-address={raft_url}",
        f"--bind={rpc_addr}",
        f"--raft-dir={raft_dir}",
        f"--log-dir={log_dir}"
    ]
    metadata_repository = common_command.copy()

    need_add_peer = False
    if peers is not None:
        metadata_repository.append("--join=true")
        for peer in peers:
            metadata_repository.append(f"--peers={peer}")
        if raft_url not in peers:
            need_add_peer = True

    metadata_repository_restart = common_command.copy()
    metadata_repository_restart.append("--join=true")

    if peers is None or need_add_peer:
        logger.info(f"remove {raft_dir}")
        try:
            shutil.rmtree(raft_dir)
        except FileNotFoundError:
            pass

    restart = False
    killer = Killer()
    while not killer.kill_now:
        if procutil.check_liveness("vmr"):
            if need_add_peer:
                logger.info("adding metadata repository to cluster")
                need_add_peer = not add_raft_peer()
            time.sleep(RETRY_INTERVAL_SEC)
            continue
        try:
            procutil.kill("vmr")
            cmd = metadata_repository_restart if restart else metadata_repository
            restart = True
            logger.info(f"running metadata repository: {cmd}")
            subprocess.Popen(cmd)
        except (OSError, ValueError, subprocess.SubprocessError):
            logger.exception("could not run metadata repository")
        time.sleep(RETRY_INTERVAL_SEC)
    procutil.stop("vmr")


if __name__ == '__main__':
    main()
