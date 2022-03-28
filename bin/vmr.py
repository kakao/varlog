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
import glob

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


def get_local_addr():
    host = os.getenv("HOST_IP", LOCAL_ADDRESS)
    return host


def get_raft_url():
    local_addr = get_local_addr()
    raft_port = os.getenv("RAFT_PORT", DEFAULT_RAFT_PORT)
    return f"http://{local_addr}:{raft_port}"


def get_rpc_addr():
    local_addr = get_local_addr()
    rpc_port = os.getenv("RPC_PORT", DEFAULT_RPC_PORT)
    return f"{local_addr}:{rpc_port}"


def get_vms_addr():
    addr = os.getenv("VMS_ADDRESS")
    if addr is None:
        raise Exception("no admin address")
    return addr


def get_raft_dir():
    home = os.getenv("VMR_HOME", DEFAULT_VMR_HOME)
    return f"{home}/raftdata"


def get_log_dir():
    home = os.getenv("VMR_HOME", DEFAULT_VMR_HOME)
    return f"{home}/log"


def get_info():
    try:
        out = subprocess.check_output([f"{binpath}/varlogctl", "mr", "describe", f"--admin={get_vms_addr()}"])
        info = json.loads(out)

        members = None
        rep_factor = get_replication_factor()
        if "error" not in info:
            data = info.get("data", dict())
            items = data.get("items", list())
            if len(items) > 0:
                rep_factor = items[0].get("replicationFactor", rep_factor)
            members = [item["raftURL"] for item in items if "raftURL" in item]
        return rep_factor, members
    except Exception:
        logger.exception("could not get peers")
        return get_replication_factor(), None

def get_replication_factor():
    return int(os.getenv("REP_FACTOR", DEFAULT_REP_FACTOR))


def add_raft_peer():
    try:
        raft_url = get_raft_url()
        rpc_addr = get_rpc_addr()

        out = subprocess.check_output([
            f"{binpath}/varlogctl",
            "mr",
            "add",
            f"--raft-url={raft_url}",
            f"--rpc-addr={rpc_addr}",
            f"--admin={get_vms_addr()}"
        ])
        info = json.loads(out)
        node_id = info["data"]["items"][0]["nodeId"]
        return node_id != "0"
    except Exception:
        logger.exception("could not add peer")
        return False


def remove_raft_peer():
    try:
        raft_url = get_raft_url()

        _, members = get_info()
        if members is None:
            return False
        elif raft_url not in members:
            return True

        out = subprocess.check_output([
            f"{binpath}/varlogctl",
            "mr",
            "remove",
            f"--raft-url={raft_url}",
            f"--admin={get_vms_addr()}"
        ])
        logger.info("remove raft:" + str(out))
        json.loads(out)

        return True
    except Exception:
        logger.exception("")
        return False


def prepare_vmr_home():
    home = os.getenv("VMR_HOME", DEFAULT_VMR_HOME)
    os.makedirs(home, exist_ok=True)


def check_standalone():
    home = os.getenv("VMR_HOME", DEFAULT_VMR_HOME)
    ret = os.path.exists(f"{home}/.standalone")
    return ret


def clear_standalone():
    logger.info("clear standalone")
    home = os.getenv("VMR_HOME", DEFAULT_VMR_HOME)
    if os.path.exists(f"{home}/.standalone"):
        os.remove(f"{home}/.standalone")


def exists_wal(path):
    wal = glob.glob(os.path.join(f"{path}/wal", "*", "*.wal"))
    return len(wal) > 0


def exists_cluster():
    _, peers = get_info()
    return peers is not None


def get_metadata_repository_cmd(standalone):
    cluster_id = os.getenv("CLUSTER_ID", DEFAULT_CLUSTER_ID)
    rep_factor, peers = get_info()
    raft_url = get_raft_url()
    rpc_addr = get_rpc_addr()
    raft_dir = get_raft_dir()
    log_dir = get_log_dir()

    command = [
        f"{binpath}/vmr",
        "start",
        f"--cluster-id={cluster_id}",
        f"--log-rep-factor={rep_factor}",
        f"--raft-address={raft_url}",
        f"--bind={rpc_addr}",
        f"--raft-dir={raft_dir}",
        f"--log-dir={log_dir}"
    ]

    if peers is not None:
        command.append("--join=true")
        for peer in peers:
            command.append(f"--peers={peer}")
    elif not standalone:
        return None

    return command


def print_mr_home():
    home = os.getenv("VMR_HOME", DEFAULT_VMR_HOME)
    arr = os.listdir(f"{home}")
    logger.info(arr)


def main():
    logger.info("start")

    print_mr_home()

    limits.set_limits()
    prepare_vmr_home()

    standalone = False
    if not exists_cluster():
        if check_standalone():
            standalone = True
            logger.info("it starts standalone")
        elif exists_wal(get_raft_dir()):
            standalone = True
            logger.info("it runs standalone with wal")
        else:
            logger.info("it could not run as standalone. check configuration")
            return

    clear_standalone()

    need_add_peer = False
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

            if not standalone and not remove_raft_peer():
                logger.info("could not leave peer")
                return

            cmd = get_metadata_repository_cmd(standalone)
            if cmd is None:
                logger.info(f"could not make command. check configuration")
                return

            logger.info(f"running metadata repository: {cmd}")
            subprocess.Popen(cmd)

            need_add_peer = not standalone
            standalone = False
        except (OSError, ValueError, subprocess.SubprocessError):
            logger.exception("could not run metadata repository")
        time.sleep(RETRY_INTERVAL_SEC)
    procutil.stop("vmr")


if __name__ == '__main__':
    main()
