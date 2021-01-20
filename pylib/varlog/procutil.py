import os
import signal
import time
from subprocess import PIPE
from subprocess import Popen


def get_pids(name):
    pids = list()
    with Popen(["ps", "acx"], stdout=PIPE) as ps:
        with Popen(["grep", "-e", f"{name} "], stdin=ps.stdout, stdout=PIPE) as grep:
            ps.stdout.close()
            grepv_cmd = ["grep", "-v", "-e", "grep", "-e", "defunct"]
            with Popen(grepv_cmd, stdin=grep.stdout, stdout=PIPE) as grepv:
                grep.stdout.close()
                outs, _ = grepv.communicate()
                if grepv.returncode != 0:
                    return pids
                for line in outs.splitlines():
                    toks = line.split()
                    pids.append(int(toks[0]))
                return pids


def check_liveness(name):
    pids = get_pids(name)
    return len(pids) > 0


def stop(name):
    send_signal_by_name(name, signal.SIGTERM)
    while True:
        if not check_liveness(name):
            break
        time.sleep(0.1)


def kill(name):
    send_signal_by_name(name, signal.SIGKILL)
    while True:
        if not check_liveness(name):
            break
        time.sleep(0.1)


def send_signal_by_name(name, sig):
    pids = get_pids(name)
    for pid in pids:
        send_signal(pid, sig)


def send_signal(pid, sig):
    os.kill(pid, sig)
