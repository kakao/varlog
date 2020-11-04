#!/usr/bin/python
#-*- coding: utf-8 -*-
import os,sys
import json
import os.path
import commands
import time
import traceback
import signal
import socket
import shutil
from random import randint

reload(sys)
sys.setdefaultencoding('utf-8')

CHECK_TIME=3

DEFAULT_CLUSTER_ID = '1'
DEFAULT_RPC_PORT = '9091'
TEST_STORAGE="/home/deploy/storage"

STORAGE_NODE_STOP="ps -fC storagenode | grep storagenode | awk '{print $2}' | xargs kill -SIGTERM"
STORAGE_NODE_KILL="ps -fC storagenode | grep storagenode | awk '{print $2}' | xargs kill -SIGKILL"
STORAGE_NODE_CHECK_PROCESS="ps -fC storagenode | grep storagenode | grep -v defunct"

MY_HOST = socket.gethostname()
MY_IP = socket.gethostbyname(MY_HOST)

def _toString( obj ):
    try:
        if isinstance(obj,unicode):
            return obj.encode('utf-8')
        else:
            return str(obj)
    except:
        return ''

def log_print(*msgs):
    sys.stderr.write("[%s]%s\n" % (time.strftime("%Y/%m/%d %H:%M:%S"),  ' '.join(map(_toString, msgs)).decode('utf8')))

def cmd_run(cmd):
    os.system("ulimit -c unlimited; %s" % cmd)

def getEnv(key, default):
    try:
        return os.environ[key]
    except KeyError:
        return default

def getRPCAddr():
    return "%s:%s" % (MY_IP,getEnv("RPC_PORT", DEFAULT_RPC_PORT))

def getStorageNodeID():
    # TODO:: fix it using vmc
    return randint(1, 2^31 - 1)

def getValumes():
    # TODO:: fix it
    if os.path.exists(TEST_STORAGE) == True:
        shutil.rmtree(TEST_STORAGE)
    os.mkdir(TEST_STORAGE)
    return TEST_STORAGE

def storagenode_check_process(isPrint):
    try:
        ret = commands.getstatusoutput(STORAGE_NODE_CHECK_PROCESS)[1]
        if isPrint == True:
            log_print(ret)
        return len(ret) > 0
    except Exception as e:
        log_print("[ERROR] storagenode check process,", str(e) )
        return False

#signal
class Killer:
    kill_now = False
    def __init__(self):
        signal.signal(signal.SIGINT, self.exit)
        signal.signal(signal.SIGTERM, self.exit)

    def exit(self,signum, frame):
        self.kill_now = True

def stop():
    log_print("[STOP] storagenode stop.")
    cmd_run(STORAGE_NODE_STOP)
    while True:
        if storagenode_check_process(True) == False:
            log_print("storagenode is not running")
            break

        time.sleep(0.1)

    time.sleep(2)

def addStorageNode(addr):
    try:
        ADD_STORAGE_NODE="./vmc add sn --storage-node-address=%s" % (addr)

        resp = commands.getstatusoutput(ADD_STORAGE_NODE)
        if resp[0] != 0:
            raise Exception("[ERROR] add storagenode," + resp[1])
    except Exception as e:
        log_print("[ERROR] add storagenode,", str(e) )
        raise e

SNID=getStorageNodeID()
VOLUMES=getValumes()
SN_ADDR=getRPCAddr()

STORAGE_NODE="nohup ./storagenode start --cluster-id=%s --storage-node-id=%s --rpc-bind-address=0.0.0.0:%s --volumes=%s &" \
        % (getEnv("CLUSTER_ID", DEFAULT_CLUSTER_ID), \
        SNID, \
        getEnv("RPC_PORT", DEFAULT_RPC_PORT), \
        VOLUMES)

try:
    cmd_run("sudo sysctl -w kernel.core_pattern=core.%e.%p; sudo sysctl -p");

    log_print("start storagenode")
    print(STORAGE_NODE)
    cmd_run(STORAGE_NODE)

    time.sleep(1)

    addStorageNode(SN_ADDR)

    killer = Killer()
    log_print("Loop start")
    time.sleep(CHECK_TIME)
    while True:
        try:
            if storagenode_check_process(False) == False:
                log_print("storagenode is not running. restart")

                cmd_run(STORAGE_NODE_KILL)
                cmd_run(STORAGE_NODE)

        except Exception as e:
            log_print("[ERROR] loop,",str(e))
            log_print("[TRACEBACK] ",traceback.format_exc())

        if killer.kill_now:
            break

        time.sleep(CHECK_TIME)
except Exception as e:
    log_print("[EXCEPTION] ",traceback.format_exc())
finally:
    stop()
