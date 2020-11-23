#!/usr/bin/python
# -*- coding: utf-8 -*-

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
from varlog.logger import get_logger  # noqa

logger = get_logger("vmr")

CHECK_TIME = 3
DEFAULT_REP_FACTOR = "1"
DEFAULT_RPC_PORT = "9092"
DEFAULT_RAFT_PORT = "10000"
MY_HOST = socket.gethostname()
MY_IP = socket.gethostbyname(MY_HOST)


def get_raft_url():
    return "http://%s:%s" % (MY_IP, os.getenv("RAFT_PORT", DEFAULT_RAFT_PORT))


def get_rpc_addr():
    return "0.0.0.0:%s" % (os.getenv("RPC_PORT", DEFAULT_RPC_PORT))


def get_vms_addr():
    addr = os.getenv("VMS_ADDRESS")
    if addr is None:
        raise Exception("no vms address")
    return addr


def get_info():
    try:
        out = subprocess.check_output([
            f"{binpath}/vmc",
            "mr",
            "info"
        ])
        info = json.loads(out)
        members = info['members'].values()
        return info['replicationFactor'], ' '.join(members)
    except Exception:
        logger.exception("could not get peers")
        return int(os.getenv("REP_FACTOR", DEFAULT_REP_FACTOR)), None


def main():
    rep_factor, peers = get_info()
    raft_url = get_raft_url()
    rpc_addr = get_rpc_addr()

    metadata_repository = [
        f"{binpath}/vmr",
        "start",
        f"--log-rep-factor={rep_factor}",
        f"--raft-address={raft_url}",
        f"--bind={rpc_addr}"]

    if peers is not None:
        metadata_repository += f" --join=true --peers={peers}"

    metadata_repository_restart = [
        f"{binpath}/vmr",
        "start",
        f"--log-rep-factor={rep_factor}",
        f"--raft-address={raft_url}",
        f"--bind={rpc_addr}",
        "--join=true"]

    logger.info("start metadata_repository")
    restart = False
    killer = Killer()
    while not killer.kill_now:
        if procutil.check("vmr"):
            time.sleep(CHECK_TIME)
            continue
        try:
            procutil.kill("vmr")
            cmd = metadata_repository_restart if restart else metadata_repository
            restart = True
            subprocess.Popen(cmd)
        except (OSError, ValueError, subprocess.SubprocessError):
            logger.exception("could not run vmr")
        time.sleep(CHECK_TIME)
    procutil.stop("vmr")


if __name__ == '__main__':
    main()
