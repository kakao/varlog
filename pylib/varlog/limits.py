import os


def set_limits():
    commands = [
        "ulimit -n 100000",     # fd
        "ulimit -u unlimited",  # process
        "ulimit -c unlimited"   # core file size
    ]
    for cmd in commands:
        os.system(cmd)
