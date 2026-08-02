#!/usr/bin/env python3
"""Drive a remote full-screen TUI over ssh from a pty, logging the raw stream.

usage: drive.py <rawlog> <script-file>
The script file is one step per line: `sleep <secs>` or `send <literal>`,
where the literal understands \r \t \x1b escapes.
"""
import os, pty, select, sys, time, fcntl, termios, struct

raw_path, script_path = sys.argv[1], sys.argv[2]
steps = []
for line in open(script_path):
    line = line.rstrip("\n")
    if not line or line.startswith("#"):
        continue
    op, _, arg = line.partition(" ")
    steps.append((op, arg))

cmd = ["ssh", "-tt", "-o", "StrictHostKeyChecking=no",
       "eric@192.168.122.10", r"C:\bin\netidx.exe admin"]

pid, fd = pty.fork()
if pid == 0:
    os.execvp(cmd[0], cmd)

fcntl.ioctl(fd, termios.TIOCSWINSZ, struct.pack("HHHH", 50, 200, 0, 0))
log = open(raw_path, "wb")


def pump(seconds):
    end = time.time() + seconds
    while time.time() < end:
        r, _, _ = select.select([fd], [], [], 0.2)
        if fd in r:
            try:
                data = os.read(fd, 65536)
            except OSError:
                return False
            if not data:
                return False
            log.write(data)
            log.flush()
    return True


for op, arg in steps:
    if op == "sleep":
        if not pump(float(arg)):
            break
    elif op == "send":
        os.write(fd, arg.encode().decode("unicode_escape").encode())
    elif op == "mark":
        log.write(("\n<<<MARK:" + arg + ">>>\n").encode())
        log.flush()

pump(2)
log.close()
try:
    os.kill(pid, 9)
except ProcessLookupError:
    pass
os.waitpid(pid, 0)
print("done")
