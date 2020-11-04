#!/usr/bin/python
#-*- coding: utf-8 -*-
import os,sys
import json
import commands
import socket
import time
import traceback
import signal

reload(sys)
sys.setdefaultencoding('utf-8')

CHECK_TIME=3

DEFAULT_REP_FACTOR = '1'
DEFAULT_RPC_PORT = '9092'
DEFAULT_RAFT_PORT = '10000'

METADATA_REPOSITORY_STOP="ps -fC metadata_repository | grep metadata_repository | awk '{print $2}' | xargs kill -SIGTERM"
METADATA_REPOSITORY_KILL="ps -fC metadata_repository | grep metadata_repository | awk '{print $2}' | xargs kill -SIGKILL"
METADATA_REPOSITORY_CHECK_PROCESS="ps -fC metadata_repository | grep metadata_repository | grep -v defunct"

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

def getRaftURL():
    return "http://%s:%s" % (MY_IP, getEnv("RAFT_PORT", DEFAULT_RAFT_PORT))

def getRPCAddr():
    return "0.0.0.0:%s" % (getEnv("RPC_PORT", DEFAULT_RPC_PORT))

def getInfo():
    try:
        GET_PEERS="./vmc mr info"

        resp = commands.getstatusoutput(GET_PEERS)
        info = json.loads(resp[1])

        members = info['members'].values()
        return info['replicationFactor'], ' '.join(members)
    except Exception as e:
        log_print("[ERROR] get peers,", str(e) )
        return int(getEnv("REP_FACTOR", DEFAULT_REP_FACTOR)), None

def metadata_repository_check_process(isPrint):
    try:
        ret = commands.getstatusoutput(METADATA_REPOSITORY_CHECK_PROCESS)[1]
        if isPrint == True:
            log_print(ret)
        return len(ret) > 0
    except Exception as e:
        log_print("[ERROR] metadata_repository check process,", str(e) )
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
    log_print("[STOP] metadata_repository stop.")
    cmd_run(METADATA_REPOSITORY_STOP)
    while True:
        if metadata_repository_check_process(True) == False:
            log_print("metadata_repository is not running")
            break

        time.sleep(0.1)

    time.sleep(2)

rep_factor, peers = getInfo()
raft_url = getRaftURL()
rpc_addr = getRPCAddr()

METADATA_REPOSITORY="nohup ./metadata_repository start --log-rep-factor=%d --raft-address=%s --bind=%s &" \
        % (rep_factor, raft_url, rpc_addr)
if peers != None :
    METADATA_REPOSITORY = METADATA_REPOSITORY + " --join=true --peers=%s" % (peers)

METADATA_REPOSITORY_RESTART="nohup ./metadata_repository start --log-rep-factor=%d --raft-address=%s --bind=%s --join=true &"\
        % (rep_factor, raft_url, rpc_addr)

try:
    cmd_run("sudo sysctl -w kernel.core_pattern=core.%e.%p; sudo sysctl -p");

    log_print("start metadata_repository")
    print(METADATA_REPOSITORY)
    cmd_run(METADATA_REPOSITORY)

    time.sleep(1)

    killer = Killer()
    log_print("Loop start")
    time.sleep(CHECK_TIME)
    while True:
        try:
            if metadata_repository_check_process(False) == False:
                log_print("metadata_repository is not running. restart")

                cmd_run(METADATA_REPOSITORY_KILL)
                cmd_run(METADATA_REPOSITORY_RESTART)

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
