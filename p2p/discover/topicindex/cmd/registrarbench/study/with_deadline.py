"""Runs a command and kills its process group at a local wall-clock time.

Usage: with_deadline.py HH:MM command [args...]
Exits with the command's status, or 124 if the deadline killed it.
"""
import datetime as dt
import os
import signal
import subprocess
import sys

h, m = map(int, sys.argv[1].split(":"))
now = dt.datetime.now()
end = now.replace(hour=h, minute=m, second=0, microsecond=0)
if end <= now:
    end += dt.timedelta(days=1)
p = subprocess.Popen(sys.argv[2:], start_new_session=True)
try:
    sys.exit(p.wait(timeout=(end - now).total_seconds()))
except subprocess.TimeoutExpired:
    os.killpg(p.pid, signal.SIGTERM)
    try:
        p.wait(timeout=30)
    except subprocess.TimeoutExpired:
        os.killpg(p.pid, signal.SIGKILL)
    sys.exit(124)
