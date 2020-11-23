import time
from subprocess import PIPE
from subprocess import Popen


def check(name):
    with Popen(["ps", "-cx"], stdout=PIPE) as ps:
        with Popen(["grep", name], stdin=ps.stdout, stdout=PIPE) as grep:
            ps.stdout.close()
            with Popen(["grep", "-v", "defunct"], stdin=grep.stdout,
                       stdout=PIPE) as grepv:
                grep.stdout.close()
                outs, _ = grepv.communicate()
                return grepv.returncode == 0 and len(outs) > 0
    raise Exception(f"checking process failed: {name}")


def stop(name):
    send_signal(name, "SIGTERM")
    while True:
        if not check(name):
            break
        time.sleep(0.1)


def kill(name):
    send_signal(name, "SIGKILL")
    while True:
        if not check(name):
            break
        time.sleep(0.1)


def send_signal(name, sig):
    with Popen(["ps", "-cx"], stdout=PIPE) as ps:
        with Popen(["grep", name], stdin=ps.stdout, stdout=PIPE) as grep:
            ps.stdout.close()
            with Popen(["awk", "{print $1}"], stdin=grep.stdout,
                       stdout=PIPE) as awk:
                grep.stdout.close()
                with Popen(["xargs", "kill", f"-{sig}"], stdin=awk.stdout,
                           stdout=PIPE) as xargs:
                    awk.stdout.close()
                    xargs.communicate()
                    return
    raise Exception(f"sending signal failed: {name} - {sig}")
