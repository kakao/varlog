#!/usr/bin/python
# -*- coding: utf-8 -*-
import commands
import json
import os
import signal
import socket
import sys
import time
import traceback

reload(sys)
sys.setdefaultencoding('utf-8')

CHECK_TIME = 3

DEFAULT_REP_FACTOR = '1'
DEFAULT_RPC_PORT = '9092'
DEFAULT_RAFT_PORT = '10000'

METADATA_REPOSITORY_STOP = "ps -fC vmr | grep " \
                           "vmr | awk '{print $2}' | xargs " \
                           "kill " \
                           "-SIGTERM "
METADATA_REPOSITORY_KILL = "ps -fC vmr | grep " \
                           "vmr | awk '{print $2}' | xargs " \
                           "kill " \
                           "-SIGKILL "
METADATA_REPOSITORY_CHECK_PROCESS = "ps -fC vmr | grep " \
                                    "vmr | grep -v defunct "

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


def get_raft_url():
    return "http://%s:%s" % (MY_IP, get_env("RAFT_PORT", DEFAULT_RAFT_PORT))


def get_rpc_addr():
    return "%s:%s" % (MY_IP, get_env("RPC_PORT", DEFAULT_RPC_PORT))


def get_info():
    try:
        get_peers = "./vmc mr info"

        resp = commands.getstatusoutput(get_peers)
        info = json.loads(resp[1])

        members = info['members'].values()
        return info['replicationFactor'], members
    except Exception as ex:
        log_print("[ERROR] get peers,", str(ex))
        return int(get_env("REP_FACTOR", DEFAULT_REP_FACTOR)), None

def add_raft_peer():
    try:
        add_peer = "./vmc mr add --raft-url=%s --rpc-addr=%s:%s" % (
                get_raft_url(), 
                MY_IP, get_env("RPC_PORT", DEFAULT_RPC_PORT))

        log_print("add peer:: " + add_peer)

        resp = commands.getstatusoutput(add_peer)
        info = json.loads(resp[1])

        log_print(info)

        node_id = info['nodeId']
        return node_id != '0'
    except Exception as ex:
        log_print("[ERROR] add peer,", str(ex))
        return False


def metadata_repository_check_process(is_print):
    try:
        ret = commands.getstatusoutput(METADATA_REPOSITORY_CHECK_PROCESS)[1]
        if is_print:
            log_print(ret)
        return len(ret) > 0
    except Exception as ex:
        log_print("[ERROR] metadata_repository check process,", str(ex))
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
    log_print("[STOP] metadata_repository stop.")
    cmd_run(METADATA_REPOSITORY_STOP)
    while True:
        if not metadata_repository_check_process(True):
            log_print("metadata_repository is not running")
            break
        time.sleep(0.1)
    time.sleep(2)


def main():
    rep_factor, peers = get_info()
    raft_url = get_raft_url()
    rpc_addr = get_rpc_addr()

    metadata_repository = "nohup ./vmr start " \
                          "--log-rep-factor=%d " \
                          "--raft-address=%s --bind=%s" \
                          % (rep_factor, raft_url, rpc_addr)

    need_add_raft_peer = False
    if peers is not None:
        metadata_repository += " --join=true"

        for peer in peers:
            metadata_repository += " --peers=%s" % peer

        if raft_url not in peers:
            need_add_raft_peer = True

    metadata_repository += " &"

    metadata_repository_restart = "nohup ./vmr start " \
                                  "--log-rep-factor=%d --raft-address=%s " \
                                  "--bind=%s " \
                                  "--join=true &" \
                                  % (rep_factor, raft_url, rpc_addr)

    try:
        cmd_run("sudo sysctl -w kernel.core_pattern=core.%e.%p; sudo sysctl -p")

        log_print("start metadata_repository:: " + metadata_repository)
        cmd_run(metadata_repository)

        time.sleep(1)

        killer = Killer()
        log_print("Loop start")
        time.sleep(CHECK_TIME)
        while True:
            try:
                if not metadata_repository_check_process(False):
                    log_print("metadata_repository is not running. restart")

                    cmd_run(METADATA_REPOSITORY_KILL)
                    cmd_run(metadata_repository_restart)
                elif need_add_raft_peer:
                    need_add_raft_peer = not add_raft_peer()

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
