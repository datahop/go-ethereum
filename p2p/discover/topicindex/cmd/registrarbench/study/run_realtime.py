"""Realtime block of docs/registrar-overhead (§6.4): throughput, queueing and
the registrar's busy fraction under offered load.

Usage (from p2p/discover):
    run_realtime.py <results-root> --deadline HH:MM [--window 10m] [--smoke]

All points use the hybrid clock (simulated warm-up, then wall-clock
measurement), spin pacing, Poisson arrivals and no raw operation records. The
stages run in order and each point is skipped when it cannot finish before the
deadline; when the remaining time is short, the window drops to 5 min.

  a. query-rate sweep at C=1000, 100 fresh attempts/s;
  b. registration-heavy points;
  c. CPU and heap profiles at the knee found in (a);
  d. a fully realtime point (validates the hybrid clock) and two points at C=5000.

A monitor thread records the load average, the top processes and the thermal
state (pmset -g therm) every minute to realtime/monitor.jsonl; the analysis
flags points whose window overlaps foreign load or a thermal warning.
"""
import argparse
import datetime as dt
import json
import os
import subprocess
import sys
import threading
import time

HERE = os.path.dirname(os.path.abspath(__file__))
BASE = {
    "mode": "hybrid", "pacing": "spin", "arrivals": "poisson", "cache-size": 1000,
    "fresh-rate": 100, "query-rate": 1000, "advertisers": 200000, "topics": 300,
    "ad-lifetime": "15m", "warmup": "3h", "sample-interval": "10s", "raw-ops": "false",
    "independent-enr": "false", "abandon": "production", "seed": 1,
    "ip-file": "results/data/dnslist-ipv4.txt",
}
QUERY_SWEEP = [1000, 4000, 16000, 64000, 160000, 320000, 640000]
FRESH_HEAVY = [500, 1500, 4000]
SETUP_S = 240  # population build and simulated warm-up, generous


def log(msg):
    print(f"{dt.datetime.now():%H:%M:%S} {msg}", flush=True)


def monitor(path, stop):
    with open(path, "a") as f:
        while not stop.wait(0 if f.tell() == 0 else 60):
            rec = {"time": time.time()}
            try:
                rec["loadavg"] = [float(x) for x in subprocess.check_output(["sysctl", "-n", "vm.loadavg"], text=True).strip("{} \n").split()]
                ps = subprocess.check_output(["ps", "-Ao", "pcpu=,comm=", "-r"], text=True).splitlines()[:6]
                rec["top"] = [[float(l.split(None, 1)[0]), l.split(None, 1)[1].strip()] for l in ps if l.strip()]
                rec["therm"] = subprocess.run(["pmset", "-g", "therm"], capture_output=True, text=True).stdout.strip()
            except Exception as e:  # keep monitoring
                rec["error"] = str(e)
            f.write(json.dumps(rec) + "\n")
            f.flush()


def done(path):
    if not os.path.exists(path):
        return False
    with open(path, "rb") as f:
        f.seek(0, 2)
        f.seek(max(0, f.tell() - (1 << 20)))
        return b'"type":"summary"' in f.read()


def summary(path):
    with open(path, "rb") as f:
        f.seek(0, 2)
        f.seek(max(0, f.tell() - (1 << 20)))
        return json.loads(f.read().splitlines()[-1])


def parse_window(w):
    """Seconds in a duration like 90s, 10m or 3h."""
    return int(w[:-1]) * {"s": 1, "m": 60, "h": 3600}[w[-1]]


class Runner:
    def __init__(self, root, binary, deadline, window, smoke):
        self.root, self.binary, self.deadline, self.window, self.smoke = root, binary, deadline, window, smoke
        self.out = os.path.join(root, "realtime")
        os.makedirs(self.out, exist_ok=True)
        self.status = []

    def remaining(self):
        return self.deadline - time.time()

    def point(self, stage, name, over, extra_s=0):
        path = os.path.join(self.out, stage, name + ".jsonl")
        if done(path):
            self.status.append((stage, name, "done earlier"))
            return path
        win = self.window
        need = SETUP_S + win + extra_s
        if self.remaining() < need and win > 300 and not self.smoke:
            win = 300
            need = SETUP_S + win + extra_s
        if self.remaining() < need:
            log(f"skip {stage}/{name}: {self.remaining() / 60:.0f} min left, needs {need / 60:.0f}")
            self.status.append((stage, name, "skipped (deadline)"))
            return None
        os.makedirs(os.path.dirname(path), exist_ok=True)
        args = dict(BASE, **over)
        warm = parse_window(args["warmup"])
        args["duration"] = f"{warm + win}s"
        tmp = path + ".partial"
        cmd = [self.binary] + [f"--{k}={v}" for k, v in args.items()] + [f"--output={tmp}"]
        log(f"start {stage}/{name} window={win}s")
        t0 = time.time()
        with open(path + ".log", "w") as lf:
            lf.write(" ".join(cmd) + "\n")
            lf.flush()
            try:
                rc = subprocess.call(cmd, stdout=lf, stderr=subprocess.STDOUT, timeout=max(60, self.remaining()))
            except subprocess.TimeoutExpired:
                rc = "timeout"
        state = f"rc={rc} wall={time.time() - t0:.0f}s window={win}s"
        if rc == 0 and done(tmp):
            os.replace(tmp, path)
        else:
            path = None
        log(f"end {stage}/{name} {state}")
        self.status.append((stage, name, state))
        return path

    def write_status(self):
        with open(os.path.join(self.out, "status.json"), "w") as f:
            json.dump(self.status, f, indent=1)


def knee(paths):
    """Largest offered query rate that was served: achieved >= 98% of offered
    and queue-delay p99 below 10 ms."""
    best = None
    for q, p in paths:
        if not p:
            continue
        s = summary(p)
        qd = s.get("queue_delay") or {}
        ok = s["rates"]["query_rps"] >= 0.98 * q and (qd.get("p99_ns_upper") or 0) < 10e6
        if ok:
            best = q
    return best


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("root")
    ap.add_argument("--deadline", required=True, help="HH:MM local time by which the block must end")
    ap.add_argument("--window", default="10m")
    ap.add_argument("--smoke", action="store_true", help="tiny configuration for a functional check")
    a = ap.parse_args()
    root = os.path.abspath(a.root)
    now = dt.datetime.now()
    h, m = map(int, a.deadline.split(":"))
    end = now.replace(hour=h, minute=m, second=0, microsecond=0)
    if end <= now:
        end += dt.timedelta(days=1)
    binary = os.path.join(root, "bin", "registrarbench")
    subprocess.check_call([os.path.join(HERE, "build.sh"), binary])
    global QUERY_SWEEP, FRESH_HEAVY, SETUP_S
    if a.smoke:
        BASE.update({"warmup": "20m", "advertisers": 20000})
        QUERY_SWEEP, FRESH_HEAVY, SETUP_S = [1000, 64000], [500], 20
    r = Runner(root, binary, end.timestamp(), 30 if a.smoke else parse_window(a.window), a.smoke)
    stop = threading.Event()
    mon = threading.Thread(target=monitor, args=(os.path.join(r.out, "monitor.jsonl"), stop), daemon=True)
    mon.start()
    try:
        # a. query-rate sweep
        sweep = [(q, r.point("a-query", f"C1000-fresh100-query{q}", {"query-rate": q})) for q in QUERY_SWEEP]
        r.write_status()
        # b. registration-heavy points: the population must cover fresh x E
        # advertisers in cooldown after abandoning (Registration.overloaded).
        for f in FRESH_HEAVY:
            adv = 20000 if a.smoke else int(f * 1200)
            r.point("b-registration", f"C1000-fresh{f}-query1000", {"fresh-rate": f, "advertisers": adv})
        r.write_status()
        # c. profiles at the knee
        k = knee(sweep) or QUERY_SWEEP[0]
        prof = os.path.join(r.out, "c-profile")
        os.makedirs(prof, exist_ok=True)
        r.point("c-profile", f"C1000-fresh100-query{k}", {"query-rate": k, "cpu-profile": os.path.join(prof, "cpu.pprof"),
                                                          "heap-profile": os.path.join(prof, "heap.pprof")})
        r.write_status()
        # d. fully realtime validation point and C=5000
        rt_warm = "1m" if a.smoke else "45m"
        r.point("d-validation", "C1000-fresh100-query16000-realtime", {"mode": "realtime", "warmup": rt_warm, "query-rate": 16000},
                extra_s=parse_window(rt_warm))
        r.point("d-validation", "C1000-fresh100-query16000-hybrid", {"query-rate": 16000})
        for q in ([1000] if a.smoke else [16000, k]):
            r.point("d-capacity", f"C5000-fresh100-query{q}", {"cache-size": 5000, "query-rate": q})
        r.write_status()
    finally:
        stop.set()
        r.write_status()
    for row in r.status:
        log("status " + " ".join(map(str, row)))


if __name__ == "__main__":
    main()
