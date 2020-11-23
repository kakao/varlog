#!/usr/bin/python
# -*- coding: utf-8 -*-

import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
from random import randint

cwd = os.path.dirname(os.path.realpath(__file__))  # noqa
binpath = os.path.join(cwd, "..", "bin")  # noqa
pylib = os.path.join(cwd, "..", "pylib")  # noqa
if os.path.isdir(pylib):  # noqa
    sys.path.insert(0, pylib)  # noqa

from varlog.killer import Killer  # noqa
from varlog import procutil  # noqa
from varlog.logger import get_logger  # noqa

logger = get_logger("vsn")

CHECK_TIME = 3
DEFAULT_CLUSTER_ID = "1"
DEFAULT_RPC_PORT = "9091"
TEST_STORAGE = "/home/deploy/storage"
MY_HOST = socket.gethostname()
MY_IP = socket.gethostbyname(MY_HOST)


def get_rpc_addr():
    return "%s:%s" % (MY_IP, os.getenv("RPC_PORT", DEFAULT_RPC_PORT))


def get_vms_addr():
    addr = os.getenv("VMS_ADDRESS")
    if addr is None:
        raise Exception("no vms address")
    return addr


def get_storage_node_id():
    try:
        out = subprocess.check_output(
            [f"{binpath}/vmc", "meta", "sn",
             "--vms-address=%s" % get_vms_addr()])
        meta = json.loads(out)
        my_addr = get_rpc_addr()
        for snid, addr in meta["storagenodes"].items():
            if addr == my_addr:
                return snid, True
        return randint(1, (2 ^ 31) - 1), False
    except Exception as e:
        logger.exception("could not get StorageNodeID")
        raise e


def get_volume(volume, temp=False):
    if temp:
        volume = tempfile.mkdtemp(prefix="varlog", suffix="volume")
    else:
        if os.path.exists(volume):
            shutil.rmtree(volume)
        os.mkdir(volume)

    def deleter():
        shutil.rmtree(volume)

    return volume, deleter

    # # TODO:: fix it
    # if os.path.exists(TEST_STORAGE):
    #     shutil.rmtree(TEST_STORAGE)
    # os.mkdir(TEST_STORAGE)
    # return TEST_STORAGE


def add_storage_node(addr):
    try:
        subprocess.check_output(
            [f"{binpath}/vmc", "add", "sn", f"--storage-node-address={addr}",
             "--vms-address=%s" % get_vms_addr()])
    except subprocess.CalledProcessError as e:
        logger.exception("could not add storagenode")
        raise e


def main():
    snid, exist = get_storage_node_id()
    volume, deleter = get_volume("", temp=True)
    sn_addr = get_rpc_addr()
    storage_node = [
        f"{binpath}/vsn",
        "start",
        "--cluster-id=%s" % os.getenv("CLUSTER_ID", DEFAULT_CLUSTER_ID),
        "--storage-node-id=%s" % snid,
        "--rpc-bind-address=0.0.0.0:%s" % os.getenv("RPC_PORT",
                                                    DEFAULT_RPC_PORT),
        "--volumes=%s" % volume
    ]
    killer = Killer()
    while not killer.kill_now:
        if procutil.check("vsn"):
            time.sleep(CHECK_TIME)
            continue
        try:
            logger.info("vsn is not running, start it")
            logger.debug(f"vsn: {storage_node}")
            procutil.kill("vsn")
            subprocess.Popen(storage_node)
            time.sleep(1)
            if not exist:
                logger.info("add sn")
                add_storage_node(sn_addr)
        except (OSError, ValueError, subprocess.SubprocessError):
            logger.exception("could not run vsn")
        time.sleep(CHECK_TIME)
    procutil.stop("vsn")
    deleter()


if __name__ == "__main__":
    main()
