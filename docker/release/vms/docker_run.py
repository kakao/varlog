#!/usr/bin/python
#-*- coding: utf-8 -*-
import os,sys
import json
import commands
import time
import traceback
import signal

reload(sys)
sys.setdefaultencoding('utf-8')

CHECK_TIME=3

DEFAULT_REP_FACTOR = '1'
DEFAULT_CLUSTER_ID = '1'
DEFAULT_RPC_PORT = '9090'

VMS_STOP="ps -fC vms | grep vms | awk '{print $2}' | xargs kill -SIGTERM"
VMS_KILL="ps -fC vms | grep vms | awk '{print $2}' | xargs kill -SIGKILL"
VMS_CHECK_PROCESS="ps -fC vms | grep vms | grep -v defunct"

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

def getMRAddr():
    addr = getEnv("MR_ADDRESS", None)
    if addr != None:
        return addr
    raise "MR check error! check environment value(export MR_ADDRESS=)"

def getRPCAddr():
    return "0.0.0.0:%s" % (getEnv("RPC_PORT", DEFAULT_RPC_PORT))

def vms_check_process(isPrint):
    try:
        ret = commands.getstatusoutput(VMS_CHECK_PROCESS)[1]
        if isPrint == True:
            log_print(ret)
        return len(ret) > 0
    except Exception as e:
        log_print("[ERROR] vms check process,", str(e) )
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
    log_print("[STOP] vms stop.")
    cmd_run(VMS_STOP)
    while True:
        if vms_check_process(True) == False:
            log_print("vms is not running")
            break

        time.sleep(0.1)

    time.sleep(2)

VMS="nohup ./vms start --cluster-id=%s --replication-factor=%s --mr-address=%s --rpc-bind-address=%s &" \
        % (getEnv("CLUSTER_ID", DEFAULT_CLUSTER_ID), \
        getEnv("REP_FACTOR", DEFAULT_REP_FACTOR), \
        getMRAddr(), \
        getRPCAddr())

try:
    cmd_run("sudo sysctl -w kernel.core_pattern=core.%e.%p; sudo sysctl -p");

    log_print("start vms")
    print(VMS)
    cmd_run(VMS)

    time.sleep(1)

    killer = Killer()
    log_print("Loop start")
    time.sleep(CHECK_TIME)
    while True:
        try:
            if vms_check_process(False) == False:
                log_print("vms is not running. restart")

                cmd_run(VMS_KILL)
                cmd_run(VMS)

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
