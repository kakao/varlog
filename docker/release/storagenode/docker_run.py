#!/usr/bin/python
# -*- coding: utf-8 -*-
import commands
import json
import os
import os.path
import shutil
import signal
import socket
import sys
import time
import traceback
from random import randint

reload(sys)
sys.setdefaultencoding('utf-8')

CHECK_TIME = 3

DEFAULT_CLUSTER_ID = '1'
DEFAULT_RPC_PORT = '9091'
TEST_STORAGE = "/home/deploy/storage"

STORAGE_NODE_STOP = "ps -fC storagenode | grep storagenode | awk '{print $2}' " \
                    "| xargs kill -SIGTERM "
STORAGE_NODE_KILL = "ps -fC storagenode | grep storagenode | awk '{print $2}' " \
                    "| xargs kill -SIGKILL "
STORAGE_NODE_CHECK_PROCESS = "ps -fC storagenode | grep storagenode | grep -v " \
                             "defunct "

MY_HOST = socket.gethostname()
MY_IP = socket.gethostbyname(MY_HOST)


def _to_string(obj):
    try:
        if isinstance(obj, unicode):
            return obj.encode('utf-8')
        else:
            return str(obj)
    except TypeError:
        return ''


def log_print(*msgs):
    sys.stderr.write("[%s]%s\n" % (time.strftime("%Y/%m/%d %H:%M:%S"),
                                   ' '.join(map(_to_string, msgs)).decode(
                                       'utf8')))


def cmd_run(cmd):
    os.system("ulimit -c unlimited; %s" % cmd)


def get_env(key, default):
    try:
        return os.environ[key]
    except KeyError:
        return default


def get_rpc_addr():
    return "%s:%s" % (MY_IP, get_env("RPC_PORT", DEFAULT_RPC_PORT))


def get_storage_node_id():
    try:
        meta_storage_node = "./vmc meta sn"

        resp = commands.getstatusoutput(meta_storage_node)
        if resp[0] != 0:
            raise Exception(resp[1])

        meta = json.loads(resp[1])
        storagenodes = meta['storagenodes']

        my_addr = get_rpc_addr()
        for snid, addr in storagenodes.items():
            if addr == my_addr:
                return snid, True

        return randint(1, 2 ^ 31 - 1), False
    except Exception as e:
        log_print("[ERROR] getStorageNodeID,", str(e))
        raise e


def get_volumes():
    # TODO:: fix it
    if os.path.exists(TEST_STORAGE):
        shutil.rmtree(TEST_STORAGE)
    os.mkdir(TEST_STORAGE)
    return TEST_STORAGE


def storagenode_check_process(is_print):
    try:
        ret = commands.getstatusoutput(STORAGE_NODE_CHECK_PROCESS)[1]
        if is_print:
            log_print(ret)
        return len(ret) > 0
    except Exception as e:
        log_print("[ERROR] storagenode check process,", str(e))
        return False


# signal
class Killer:
    kill_now = False

    def __init__(self):
        signal.signal(signal.SIGINT, self.exit)
        signal.signal(signal.SIGTERM, self.exit)

    def exit(self):
        self.kill_now = True


def stop():
    log_print("[STOP] storagenode stop.")
    cmd_run(STORAGE_NODE_STOP)
    while True:
        if not storagenode_check_process(True):
            log_print("storagenode is not running")
            break

        time.sleep(0.1)

    time.sleep(2)


def add_storage_node(addr):
    try:
        cmd = "./vmc add sn --storage-node-address=%s" % addr
        resp = commands.getstatusoutput(cmd)
        if resp[0] != 0:
            raise Exception("[ERROR] add storagenode," + resp[1])
    except Exception as e:
        log_print("[ERROR] add storagenode,", str(e))
        raise e


def main():
    try:
        snid, exist = get_storage_node_id()
        volumes = get_volumes()
        sn_addr = get_rpc_addr()

        storage_node = "nohup ./storagenode start --cluster-id=%s " \
                       "--storage-node-id=%s --rpc-bind-address=0.0.0.0:%s " \
                       "--volumes=%s &" \
                       % (get_env("CLUSTER_ID", DEFAULT_CLUSTER_ID),
                          snid,
                          get_env("RPC_PORT", DEFAULT_RPC_PORT),
                          volumes)

        log_print("start storagenode")
        log_print(storage_node)

        cmd_run(storage_node)

        time.sleep(1)

        if not exist:
            log_print("add storagenode")
            add_storage_node(sn_addr)

        killer = Killer()
        log_print("Loop start")
        time.sleep(CHECK_TIME)
        while True:
            try:
                if not storagenode_check_process(False):
                    log_print("storagenode is not running. restart")

                    cmd_run(STORAGE_NODE_KILL)
                    cmd_run(storage_node)

            except Exception as e:
                log_print("[ERROR] loop,", str(e))
                log_print("[TRACEBACK] ", traceback.format_exc())

            if killer.kill_now:
                break

            time.sleep(CHECK_TIME)
    except Exception:
        log_print("[EXCEPTION] ", traceback.format_exc())
    finally:
        stop()


if __name__ == '__main__':
    main()
