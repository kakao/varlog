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
from random import randint

cwd = os.path.dirname(os.path.realpath(__file__))  # noqa
binpath = os.path.join(cwd, "..", "bin")  # noqa
pylib = os.path.join(cwd, "..", "pylib")  # noqa
if os.path.isdir(pylib):  # noqa
    sys.path.insert(0, pylib)  # noqa

from varlog.killer import Killer  # noqa
from varlog import procutil  # noqa
from varlog import limits  # noqa
from varlog.logger import get_logger  # noqa

logger = get_logger("vsn")

RETRY_INTERVAL_SEC = 3
ADD_STORAGE_NODE_INTERVAL_SEC = 1
DEFAULT_CLUSTER_ID = "1"
DEFAULT_RPC_PORT = "9091"
DEFAULT_VOLUMES = "/varlog/sn"
TEST_STORAGE = "/home/deploy/storage"
LOCAL_ADDRESS = socket.gethostbyname(socket.gethostname())


def get_advertise_addr():
    host = os.getenv("HOST_IP", LOCAL_ADDRESS)
    port = os.getenv("RPC_PORT", DEFAULT_RPC_PORT)
    return f"{host}:{port}"


def get_vms_addr():
    addr = os.getenv("VMS_ADDRESS")
    if addr is None:
        raise Exception("no vms address")
    return addr


def get_storage_node_id():
    try:
        out = subprocess.check_output(
            [f"{binpath}/vmc", "--vms-address=%s" % get_vms_addr(), "meta", "sn"])
        meta = json.loads(out)
        my_addr = get_advertise_addr()
        for snid, addr in meta["storagenodes"].items():
            if addr == my_addr:
                return snid, True
        return randint(1, (2 ^ 31) - 1), False
    except Exception as e:
        logger.exception("could not get StorageNodeID")
        raise e


def get_volumes(truncate=False):
    datapaths = []
    volumes = os.getenv("VOLUMES", DEFAULT_VOLUMES).strip().split(",")
    # FIXME: Do not add "data" subdirectory.
    for volume in volumes:
        datapath = f"{volume}/data"
        datapaths.append(datapath)

    if truncate:
        for datapath in datapaths:
            try:
                logger.info(f"remove volume {datapath}")
                shutil.rmtree(datapath)
            except FileNotFoundError:
                pass

    for datapath in datapaths:
        os.makedirs(datapath, exist_ok=True)
    return datapaths


def get_logdir():
    home = os.getenv("VOLUMES", DEFAULT_VOLUMES)
    logdir = f"{home}/logs"
    os.makedirs(logdir, exist_ok=True)
    return logdir


def add_storage_node(addr):
    try:
        subprocess.check_output(
            [f"{binpath}/vmc", "--vms-address=%s" % get_vms_addr(),
                "add", "sn", f"--storage-node-address={addr}"])
    except subprocess.CalledProcessError as e:
        logger.exception("could not add storagenode")
        raise e


def main():
    limits.set_limits()
    cluster_id = os.getenv("CLUSTER_ID", DEFAULT_CLUSTER_ID)
    listen_port = os.getenv("RPC_PORT", DEFAULT_RPC_PORT)
    listen_addr = f"0.0.0.0:{listen_port}"
    advertise_addr = get_advertise_addr()
    # logdir = get_logdir()

    killer = Killer()
    ok = False
    while not killer.kill_now:
        if ok and procutil.check_liveness("vsn"):
            time.sleep(RETRY_INTERVAL_SEC)
            continue
        try:
            procutil.kill("vsn")

            snid, exist = get_storage_node_id()
            volumes = get_volumes(truncate=not exist)
            storage_node = [
                f"{binpath}/vsn",
                "start",
                f"--cluster-id={cluster_id}",
                f"--storage-node-id={snid}",
                f"--listen-address={listen_addr}",
                f"--advertise-address={advertise_addr}",
                "--disable-write-sync",
                "--disable-commit-sync",
            ]
            for volume in volumes:
                storage_node.append(f"--volumes={volume}")

            # cmd = f"{binpath}/vsn start"
            # cmd += f" --cluster-id={cluster_id}"
            # cmd += f" --storage-node-id={snid}"
            # cmd += f" --listen-address={listen_addr}"
            # cmd += f" --volumes={volume}"
            # cmd += f" --advertise-address={advertise_addr}"
            # cmd += f" 2>&1 | tee -a {logdir}/log.txt"

            logger.info(f"running storage node: {storage_node}, already registered={exist}")
            subprocess.Popen(storage_node)
            # subprocess.Popen(cmd, shell=True)

            ok = True
            time.sleep(ADD_STORAGE_NODE_INTERVAL_SEC)
            if not exist:
                logger.info("adding storage node to cluster")
                add_storage_node(advertise_addr)
        except (OSError, ValueError, subprocess.SubprocessError):
            logger.exception("could not run storage node")
            ok = False
        time.sleep(RETRY_INTERVAL_SEC)
    procutil.stop("vsn")


if __name__ == "__main__":
    main()
