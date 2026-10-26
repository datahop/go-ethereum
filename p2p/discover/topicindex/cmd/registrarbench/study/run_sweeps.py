#!/usr/bin/env python3
"""Runs the simulated registrar experiments of docs/registrar-overhead.

Usage (from p2p/discover):
    run_sweeps.py <results-root> [--only EXP ...] [--jobs N] [--seeds K] [--dry-run]

Each run is one registrarbench invocation, written to
<results-root>/sim/<experiment>/<point>/seed<k>.jsonl. Completed runs (with a
summary record) are skipped, so the script can be re-run after interruption.
"""
import argparse
import concurrent.futures
import json
import os
import platform
import subprocess
import sys

E = "15m"
BASE = {
    "mode": "simulated", "cache-size": 1000, "fresh-rate": 5, "query-rate": 20,
    "advertisers": 200000, "topics": 300, "zipf-alpha": 1.0, "ad-lifetime": E,
    "duration": "18h", "warmup": "6h", "sample-interval": "10s", "abandon": "production",
    "arrivals": "deterministic", "ip-dist": "empirical", "ip-file": "results/data/dnslist-ipv4.txt",
    "raw-ops": "false",
    # ENR decoding (signature recovery) is handler work outside the timed
    # registrar core; memory per ad is measured separately in memfill mode.
    "independent-enr": "false",
}

FRESH = [0.25, 0.5, 1, 2, 5, 10, 20, 50, 100]


def experiments():
    """Yields (experiment, point-name, overrides)."""
    for c in (1000, 5000):
        for r in FRESH:
            yield "demand", f"C{c}-fresh{r:g}", {"cache-size": c, "fresh-rate": r}
    for q in (1, 10, 100, 1000):
        yield "query", f"C1000-fresh5-query{q}", {"query-rate": q}
    for c in (500, 1000, 2000, 5000, 10000):
        for r in (5, 20):
            yield "capacity", f"C{c}-fresh{r:g}", {"cache-size": c, "fresh-rate": r}
    for a in (0, 0.5, 1.0, 1.5):
        yield "skew", f"C1000-fresh5-alpha{a:g}", {"zipf-alpha": a}
    for dist in ("empirical", "diverse", "concentrated"):
        yield "ipdist", f"C1000-fresh5-{dist}", {"ip-dist": dist}
    # Without abandonment, attempts last hours: a larger population avoids
    # scarcity and a longer warm-up reaches steady state. All policies use the
    # same settings so they stay comparable.
    for policy in ("production", "spec", "none"):
        for r in (1, 5, 20):
            yield "abandon", f"C1000-fresh{r:g}-{policy}", {"abandon": policy, "fresh-rate": r,
                                                           "advertisers": 1000000, "duration": "72h", "warmup": "48h"}
    # Bounded registrar state: high topic churn, many distinct IPs.
    for r in (5, 50):
        yield "bounded", f"C1000-fresh{r:g}-topics50000-uniform-diverse", {
            "topics": 50000, "zipf-alpha": 0, "ip-dist": "diverse", "fresh-rate": r}


def done(path):
    if not os.path.exists(path):
        return False
    with open(path, "rb") as f:
        f.seek(0, 2)
        f.seek(max(0, f.tell() - (1 << 20)))
        return b'"type":"summary"' in f.read()


def run(binary, path, args):
    tmp = path + ".partial"
    cmd = [binary] + [f"--{k}={v}" for k, v in args.items()] + [f"--output={tmp}"]
    with open(path + ".log", "w") as log:
        rc = subprocess.call(cmd, stdout=log, stderr=subprocess.STDOUT)
    if rc != 0:
        return path, rc
    os.replace(tmp, path)
    return path, 0


def main():
    p = argparse.ArgumentParser()
    p.add_argument("root")
    p.add_argument("--only", nargs="*")
    p.add_argument("--jobs", type=int, default=6)
    p.add_argument("--seeds", type=int, default=5)
    p.add_argument("--dry-run", action="store_true")
    a = p.parse_args()
    arch = {"x86_64": "amd64", "arm64": "arm64", "aarch64": "arm64"}[platform.machine()]
    binary = os.path.join(a.root, "bin", "registrarbench")
    os.makedirs(os.path.dirname(binary), exist_ok=True)
    env = dict(os.environ, GOARCH=arch)
    subprocess.check_call(["go", "build", "-o", binary, "./topicindex/cmd/registrarbench"], env=env)
    info = subprocess.check_output(["file", "-b", binary]).decode()
    if platform.machine() not in info:
        sys.exit(f"binary is not native: {info}")
    jobs = []
    for exp, point, over in experiments():
        if a.only and exp not in a.only:
            continue
        for seed in range(1, a.seeds + 1):
            path = os.path.join(a.root, "sim", exp, point, f"seed{seed}.jsonl")
            if done(path):
                continue
            os.makedirs(os.path.dirname(path), exist_ok=True)
            jobs.append((path, dict(BASE, **over, seed=seed)))
    print(f"{len(jobs)} runs pending", flush=True)
    if a.dry_run:
        for path, args in jobs:
            print(path, json.dumps(args))
        return
    failed = 0
    with concurrent.futures.ThreadPoolExecutor(a.jobs) as pool:
        for i, (path, rc) in enumerate(pool.map(lambda j: run(binary, *j), jobs)):
            print(f"[{i + 1}/{len(jobs)}] rc={rc} {path}", flush=True)
            failed += rc != 0
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
