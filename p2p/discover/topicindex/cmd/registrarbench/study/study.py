#!/usr/bin/env python3
"""Aggregates registrar-overhead results into tables, figures and the write-up.

Usage (from p2p/discover):
    study.py <results-root> <docs-dir>

Reads
    <results-root>/sim/<experiment>/<point>/seed<k>.jsonl   simulated runs
    <results-root>/memfill/*.jsonl                          controlled-fill memory runs
    <results-root>/microbench/bench.txt, env.json           testing.B output
    <results-root>/realtime/<experiment>/<point>/*.jsonl    realtime / hybrid runs
and writes
    <docs-dir>/generated/*.csv, *.md, values.json           tables and scalar values
    <docs-dir>/figures/*.png, *.pdf                         figures
    <docs-dir>/registrar-overhead.md                        rendered from registrar-overhead.md.in

Every number in the rendered document comes from values.json or a generated
table: the template references values as {{key|format}} and tables as
{{table:name}}.
"""
import csv
import glob
import json
import math
import os
import re
import sys
from collections import defaultdict

import numpy as np
from scipy import stats

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

# Validation thresholds.
LITTLE_TOL = 0.02  # |time-avg occupancy / (admission rate * E) - 1|
DRIFT_OCC = 0.05  # relative occupancy change between measurement halves
DRIFT_WAIT = 0.03  # absolute wait-fraction change between halves
MIN_SEEDS = 5

VALUES = {}


def put(key, value):
    VALUES[key] = value


# ---------------------------------------------------------------- loading


def read_run(path):
    meta = summary = None
    samples = []
    memfill = []
    with open(path) as f:
        for line in f:
            if '"type":"operation"' in line:
                continue
            r = json.loads(line)
            t = r.get("type")
            if t == "metadata":
                meta = r
            elif t == "summary":
                summary = r
            elif t == "sample":
                samples.append(r)
            elif t in ("memfill", "memfill_bounds"):
                memfill.append(r)
    return meta, summary, samples, memfill


def admissibility(meta):
    """Reasons a run may not be used in the write-up."""
    reasons = []
    p = (meta or {}).get("platform")
    if not p:
        return ["no platform record (legacy run)"]
    if p.get("rosetta_translated"):
        reasons.append("Rosetta-translated")
    if p.get("revision") in (None, "", "unknown"):
        reasons.append("unknown revision")
    if p.get("dirty"):
        reasons.append("dirty working tree")
    if meta.get("schema_version", 0) < 6:
        reasons.append("schema < 6")
    return reasons


def ci(xs):
    xs = np.asarray([x for x in xs if x is not None and not (isinstance(x, float) and math.isnan(x))], float)
    n = len(xs)
    if n == 0:
        return float("nan"), float("nan"), 0
    m = float(xs.mean())
    if n < 2:
        return m, float("nan"), n
    h = float(stats.t.ppf(0.975, n - 1) * xs.std(ddof=1) / math.sqrt(n))
    return m, h, n


def run_metrics(meta, s, samples):
    E = meta["config"]["Lifetime"] / 1e9
    C = meta["config"]["CacheSize"]
    r = s["rates"]
    meas = [x for x in samples if x["measurement"]]
    h = s["halves"]
    occ1, occ2 = h[0]["time_avg_occupancy"] or 0, h[1]["time_avg_occupancy"] or 0
    occ = s["time_avg_occupancy"] or 0
    m = {
        "time_avg_occupancy": occ,
        "utilisation": s["time_avg_utilisation"] or 0,
        "fresh_arrival_rps": r["fresh_arrival_rps"],
        "fresh_attempt_rps": r["fresh_attempt_rps"],
        "registration_rps": r["registration_rps"],
        "retry_rps": r["retry_rps"],
        "admission_rps": r["admission_rps"],
        "abandon_rps": r["abandon_rps"],
        "query_rps": r["query_rps"],
        "wait_fraction": s["wait_fraction_of_requests"],
        "immediate_fraction": s["immediate_admission_fraction_of_fresh"],
        "abandon_fraction": s["abandon_fraction_of_fresh"],
        "admit_fraction": r["admission_rps"] / r["fresh_attempt_rps"] if r["fresh_attempt_rps"] else float("nan"),
        "retries_per_admission": s["retries_per_admission"],
        "requests_per_admission": r["registration_rps"] / r["admission_rps"] if r["admission_rps"] else float("nan"),
        "quoted_wait_mean_s": s["quoted_wait"]["mean_s"],
        "quoted_wait_p95_s": s["quoted_wait"]["p95_s_upper"],
        "cumwait_all_median_s": s["cumulative_wait_at_admission"]["median_s_upper"],
        "cumwait_all_p95_s": s["cumulative_wait_at_admission"]["p95_s_upper"],
        "cumwait_waited_median_s": s["cumulative_wait_at_admission_after_wait"]["median_s_upper"],
        "cumwait_waited_p95_s": s["cumulative_wait_at_admission_after_wait"]["p95_s_upper"],
        "expired_retry_rps": r["expired_retry_rps"],
        "little_ratio": s["littles_law_ratio"] if s["littles_law_ratio"] is not None else float("nan"),
        "occ_drift": (occ2 - occ1) / occ if occ else 0.0,
        "wait_drift": (h[1]["wait_fraction_of_requests"] - h[0]["wait_fraction_of_requests"]),
        "ipv4_tree_nodes": np.mean([x["state"]["ipv4_tree_nodes"] for x in meas]) if meas else float("nan"),
        "topic_bounds": np.mean([x["state"]["topic_bounds"] for x in meas]) if meas else float("nan"),
        "effective_topic_bounds": np.mean([x["state"]["effective_topic_bounds"] for x in meas]) if meas else float("nan"),
        "effective_ip_bounds": np.mean([x["state"]["effective_ip_bounds"] for x in meas]) if meas else float("nan"),
        "occupied_topics": np.mean([x["state"]["occupied_topics"] for x in meas]) if meas else float("nan"),
        "max_bounds_minus_topics": max((x["state"]["topic_bounds"] - x["state"]["occupied_topics"] for x in samples), default=0),
        "max_tree_nodes_per_entry": max((x["state"]["ipv4_tree_nodes"] / max(1, x["state"]["ipv4_entries"]) for x in samples), default=0),
        "max_utilisation": max((x["utilisation"] for x in samples), default=0),
        "capacity": C,
        "lifetime_s": E,
    }
    return m


def flags(meta, s, m):
    f = []
    if not s["load_valid"]:
        f.append("population scarcity")
    if not math.isnan(m["little_ratio"]) and abs(m["little_ratio"] - 1) > LITTLE_TOL:
        f.append(f"Little ratio {m['little_ratio']:.3f}")
    if abs(m["occ_drift"]) > DRIFT_OCC:
        f.append(f"occupancy drift {m['occ_drift']:+.1%}")
    if abs(m["wait_drift"]) > DRIFT_WAIT:
        f.append(f"wait-fraction drift {m['wait_drift']:+.3f}")
    return f


def load_sim(root):
    runs = defaultdict(list)  # (exp, point) -> [metrics]
    excluded = []
    flagged = []
    meta_of = {}
    for path in sorted(glob.glob(os.path.join(root, "sim", "*", "*", "seed*.jsonl"))):
        exp, point = path.split(os.sep)[-3:-1]
        meta, s, samples, _ = read_run(path)
        if s is None:
            excluded.append((path, "incomplete (no summary)"))
            continue
        reasons = admissibility(meta)
        if reasons:
            excluded.append((path, "; ".join(reasons)))
            continue
        m = run_metrics(meta, s, samples)
        m["seed"] = meta["config"]["Seed"]
        m["flags"] = flags(meta, s, m)
        if m["flags"]:
            flagged.append((path, "; ".join(m["flags"])))
        runs[(exp, point)].append(m)
        meta_of[(exp, point)] = meta
    return runs, excluded, flagged, meta_of


# ---------------------------------------------------------------- output helpers


def write_csv(path, header, rows):
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)


def fmt_num(v, nd):
    if v is None or (isinstance(v, float) and (math.isnan(v) or math.isinf(v))):
        return "–"
    return f"{v:,.{nd}f}"


def fmt_ci(m, h, nd):
    if math.isnan(h):
        return fmt_num(m, nd)
    return f"{fmt_num(m, nd)} ± {fmt_num(h, nd)}"


def write_md_table(path, header, rows):
    with open(path, "w") as f:
        f.write("| " + " | ".join(header) + " |\n")
        f.write("|" + "|".join("---" for _ in header) + "|\n")
        for r in rows:
            f.write("| " + " | ".join(str(c) for c in r) + " |\n")


def savefig(fig, figdir, name):
    fig.tight_layout()
    for ext in ("png", "pdf"):
        fig.savefig(os.path.join(figdir, f"{name}.{ext}"), dpi=200)
    plt.close(fig)


# ---------------------------------------------------------------- simulated experiments

COLUMNS = [
    ("time_avg_occupancy", "occupancy", 0),
    ("utilisation", "occ/C", 3),
    ("fresh_attempt_rps", "fresh/s", 2),
    ("admission_rps", "admit/s", 3),
    ("admit_fraction", "admit/fresh", 3),
    ("abandon_fraction", "abandon/fresh", 3),
    ("wait_fraction", "wait frac", 3),
    ("registration_rps", "REGTOPIC/s", 2),
    ("requests_per_admission", "req/admit", 2),
    ("quoted_wait_mean_s", "quote mean s", 0),
    ("cumwait_waited_median_s", "cum. wait p50 s", 0),
    ("cumwait_waited_p95_s", "cum. wait p95 s", 0),
    ("little_ratio", "Little", 3),
]

POINT_RE = re.compile(r"(?:C(?P<C>\d+))?-?(?:fresh(?P<fresh>[\d.]+))?")


def point_params(point):
    p = {}
    for part in point.split("-"):
        for key, rx in (("C", r"^C(\d+)$"), ("fresh", r"^fresh([\d.]+)$"), ("query", r"^query(\d+)$"),
                        ("alpha", r"^alpha([\d.]+)$"), ("topics", r"^topics(\d+)$")):
            mm = re.match(rx, part)
            if mm:
                p[key] = float(mm.group(1))
        if part in ("empirical", "diverse", "concentrated"):
            p["ipdist"] = part
        if part in ("production", "spec", "none"):
            p["abandon"] = part
    return p


def aggregate(runs, docs, figdir):
    gen = os.path.join(docs, "generated")
    by_exp = defaultdict(list)
    for (exp, point), ms in runs.items():
        by_exp[exp].append((point, ms))
    allkeys = [k for k, _, _ in COLUMNS] + ["retry_rps", "abandon_rps", "query_rps", "immediate_fraction",
                                            "quoted_wait_p95_s", "cumwait_all_median_s", "cumwait_all_p95_s",
                                            "ipv4_tree_nodes", "topic_bounds", "effective_topic_bounds",
                                            "effective_ip_bounds", "occupied_topics", "max_bounds_minus_topics",
                                            "max_tree_nodes_per_entry", "max_utilisation", "occ_drift", "wait_drift",
                                            "expired_retry_rps"]
    summary = {}
    for exp, points in by_exp.items():
        def order(pm):
            p = point_params(pm[0])
            return (p.get("C", 0), p.get("fresh", 0), p.get("query", 0), p.get("alpha", 0), p.get("ipdist", ""), p.get("abandon", ""))
        points.sort(key=order)
        rows_csv, rows_md = [], []
        for point, ms in points:
            agg = {}
            for k in allkeys:
                mean, half, n = ci([m[k] for m in ms])
                agg[k] = (mean, half, n)
                put(f"{exp}.{point}.{k}", {"mean": mean, "ci": half, "n": n})
            put(f"{exp}.{point}.seeds", len(ms))
            put(f"{exp}.{point}.flagged", sum(1 for m in ms if m["flags"]))
            summary[(exp, point)] = agg
            rows_csv.append([point, len(ms)] + [x for k in allkeys for x in agg[k][:2]])
            rows_md.append([point, len(ms)] + [fmt_ci(agg[k][0], agg[k][1], nd) for k, _, nd in COLUMNS])
        write_csv(os.path.join(gen, f"sim_{exp}.csv"), ["point", "seeds"] + [f"{k}_{s}" for k in allkeys for s in ("mean", "ci95")], rows_csv)
        write_md_table(os.path.join(gen, f"sim_{exp}.md"), ["point", "seeds"] + [h for _, h, _ in COLUMNS], rows_md)
    # Per-seed appendix.
    rows = []
    for (exp, point), ms in sorted(runs.items()):
        for m in sorted(ms, key=lambda m: m["seed"]):
            rows.append([exp, point, m["seed"]] + [m[k] for k in allkeys] + ["; ".join(m["flags"])])
    write_csv(os.path.join(gen, "sim_per_seed.csv"), ["experiment", "point", "seed"] + allkeys + ["flags"], rows)
    return summary


def series(summary, exp, key, filt, xkey):
    xs, ys, hs = [], [], []
    for (e, point), agg in summary.items():
        if e != exp:
            continue
        p = point_params(point)
        if not filt(p):
            continue
        xs.append(p[xkey])
        ys.append(agg[key][0])
        hs.append(agg[key][1])
    o = np.argsort(xs)
    return np.array(xs)[o], np.array(ys)[o], np.nan_to_num(np.array(hs)[o])


def caption_meta(meta_of, exp):
    for (e, _), meta in meta_of.items():
        if e == exp:
            p = meta["platform"]
            return f"{p['goos']}/{p['goarch']}, {p['cpu_model']}, rev {p['revision'][:9]}"
    return ""


def sim_figures(summary, meta_of, figdir):
    if not summary:
        return
    # Equilibrium occupancy vs fresh demand.
    fig, axes = plt.subplots(1, 3, figsize=(13, 3.8))
    for C, color in ((1000, "C0"), (5000, "C1")):
        x, y, h = series(summary, "demand", "time_avg_occupancy", lambda p: p.get("C") == C, "fresh")
        axes[0].errorbar(x, y, yerr=h, marker="o", color=color, label=f"C = {C}", capsize=3)
        axes[0].axhline(C, color=color, ls=":", lw=1)
        x, y, h = series(summary, "demand", "admission_rps", lambda p: p.get("C") == C, "fresh")
        axes[1].errorbar(x, y, yerr=h, marker="o", color=color, label=f"C = {C}", capsize=3)
        x, y, h = series(summary, "demand", "abandon_fraction", lambda p: p.get("C") == C, "fresh")
        axes[2].errorbar(x, y, yerr=h, marker="o", color=color, label=f"abandon/fresh, C = {C}", capsize=3)
        x, y, h = series(summary, "demand", "wait_fraction", lambda p: p.get("C") == C, "fresh")
        axes[2].errorbar(x, y, yerr=h, marker="s", ls="--", color=color, label=f"wait/request, C = {C}", capsize=3)
    for ax in axes:
        ax.set_xscale("log")
        ax.set_xlabel("fresh registration attempts / s")
        ax.grid(alpha=.3)
    axes[0].set_yscale("log")
    axes[0].set_ylabel("time-averaged occupancy (ads)")
    axes[1].set_ylabel("admissions / s")
    axes[2].set_ylabel("fraction")
    axes[0].legend(fontsize=8)
    axes[2].legend(fontsize=7)
    savefig(fig, figdir, "occupancy_vs_demand")
    # Capacity sweep.
    fig, ax = plt.subplots(1, 2, figsize=(9, 3.6))
    for r, color in ((5, "C0"), (20, "C1")):
        x, y, h = series(summary, "capacity", "time_avg_occupancy", lambda p: p.get("fresh") == r, "C")
        ax[0].errorbar(x, y, yerr=h, marker="o", color=color, label=f"{r:g} fresh/s", capsize=3)
        x, y, h = series(summary, "capacity", "utilisation", lambda p: p.get("fresh") == r, "C")
        ax[1].errorbar(x, y, yerr=h, marker="o", color=color, label=f"{r:g} fresh/s", capsize=3)
    ax[0].plot([500, 10000], [500, 10000], "k:", lw=1, label="occupancy = C")
    for a in ax:
        a.set_xscale("log")
        a.set_xlabel("cache capacity C")
        a.grid(alpha=.3)
        a.legend(fontsize=8)
    ax[0].set_yscale("log")
    ax[0].set_ylabel("time-averaged occupancy")
    ax[1].set_ylabel("occupancy / C")
    savefig(fig, figdir, "occupancy_vs_capacity")


# ---------------------------------------------------------------- microbenchmarks

BENCH_RE = re.compile(r"^(Benchmark\S+?)-\d+\s+\d+\s+(.*)$")


def load_bench(path):
    res = defaultdict(lambda: defaultdict(list))
    with open(path) as f:
        for line in f:
            mm = BENCH_RE.match(line.strip())
            if not mm:
                continue
            name, rest = mm.groups()
            fields = rest.split()
            for v, unit in zip(fields[0::2], fields[1::2]):
                res[name][unit].append(float(v))
    return res


def bench_params(name):
    fam = name.split("/")[0].replace("Benchmark", "")
    p = {"family": fam}
    for part in name.split("/")[1:]:
        k, _, v = part.partition("=")
        p[k] = float(v) if re.match(r"^[\d.]+$", v) else v
    return p


def fit(xs, ys):
    xs, ys = np.asarray(xs, float), np.asarray(ys, float)
    A = np.vstack([np.ones_like(xs), xs]).T
    coef, *_ = np.linalg.lstsq(A, ys, rcond=None)
    pred = A @ coef
    ss = ((ys - ys.mean()) ** 2).sum()
    r2 = 1 - ((ys - pred) ** 2).sum() / ss if ss > 0 else 1.0
    # Standard error of the slope.
    n = len(xs)
    se = math.sqrt(((ys - pred) ** 2).sum() / max(n - 2, 1) / max(((xs - xs.mean()) ** 2).sum(), 1e-12))
    return coef[0], coef[1], r2, se * stats.t.ppf(0.975, max(n - 2, 1))


def microbench(root, docs, figdir):
    path = os.path.join(root, "microbench", "bench.txt")
    if not os.path.exists(path):
        return
    env = json.load(open(os.path.join(root, "microbench", "env.json")))
    put("micro.env", env)
    res = load_bench(path)
    gen = os.path.join(docs, "generated")
    rows, rows_md = [], []
    for name in res:
        p = bench_params(name)
        ns = ci(res[name]["ns/op"])
        bop = np.mean(res[name].get("B/op", [float("nan")]))
        aop = np.mean(res[name].get("allocs/op", [float("nan")]))
        per = ci(res[name]["ns/entry"]) if "ns/entry" in res[name] else (float("nan"), float("nan"), 0)
        key = name.replace("Benchmark", "")
        put(f"micro.{key}.ns", {"mean": ns[0], "ci": ns[1], "n": ns[2]})
        put(f"micro.{key}.bytes", bop)
        put(f"micro.{key}.allocs", aop)
        if not math.isnan(per[0]):
            put(f"micro.{key}.ns_per_entry", {"mean": per[0], "ci": per[1], "n": per[2]})
        rows.append([key, ns[2], ns[0], ns[1], bop, aop, per[0]])
        rows_md.append([key, fmt_ci(ns[0], ns[1], 1), fmt_num(bop, 0), fmt_num(aop, 1), fmt_num(per[0], 1)])
    write_csv(os.path.join(gen, "micro.csv"), ["benchmark", "count", "ns_op_mean", "ns_op_ci95", "B_op", "allocs_op", "ns_per_entry"], rows)
    write_md_table(os.path.join(gen, "micro.md"), ["benchmark", "ns/op", "B/op", "allocs/op", "ns/entry"], rows_md)

    # Complexity fits: (family, x parameter, fixed parameters).
    fits = [
        ("RegisterFresh", "T", {"N": 5000}, "Register, fresh ad (wait ticket) vs topic size T"),
        ("RegisterFresh", "N", {"T": 16}, "Register, fresh ad vs occupancy N"),
        ("RegisterDuplicate", "T", {"N": 5000}, "Register, duplicate ad vs T"),
        ("RegisterRetryAdmit", "T", {"N": 5000}, "Register, admitted retry vs T"),
        ("RegisterRetryAdmit", "N", {"T": 16}, "Register, admitted retry vs N"),
        ("RandomNodes", "T", {"N": 5000}, "RandomNodes(16) vs T"),
        ("RandomNodes", "N", {"T": 16}, "RandomNodes(16) vs N"),
        ("RemoveNode", "N", {}, "RemoveNode (absent ID) vs N"),
    ]
    frows, fmd = [], []
    fig, axes = plt.subplots(2, 4, figsize=(15, 6.5))
    for ax, (fam, xk, fixed, label) in zip(axes.flat, fits):
        pts = []
        for name in res:
            p = bench_params(name)
            if p["family"] != fam or xk not in p or any(p.get(k) != v for k, v in fixed.items()):
                continue
            for v in res[name]["ns/op"]:
                pts.append((p[xk], v))
        if len(pts) < 3:
            continue
        xs, ys = zip(*pts)
        a, b, r2, bci = fit(xs, ys)
        key = f"fit.{fam}.{xk}"
        put(key, {"intercept_ns": a, "slope_ns": b, "slope_ci": bci, "r2": r2})
        frows.append([label, fam, xk, json.dumps(fixed), a, b, bci, r2])
        fmd.append([label, fmt_num(a, 0), fmt_ci(b, bci, 2), f"{r2:.3f}"])
        ux = sorted(set(xs))
        ax.errorbar(ux, [np.mean([y for x, y in pts if x == u]) for u in ux],
                    yerr=[np.std([y for x, y in pts if x == u]) for u in ux], fmt="o", capsize=3)
        xx = np.linspace(0, max(xs), 50)
        ax.plot(xx, a + b * xx, "k--", lw=1, label=f"{a:.0f} + {b:.2f}·{xk} ns")
        ax.set_title(label, fontsize=9)
        ax.set_xlabel(xk)
        ax.set_ylabel("ns/op")
        ax.legend(fontsize=8)
        ax.grid(alpha=.3)
    savefig(fig, figdir, "microbench_complexity")
    write_csv(os.path.join(gen, "micro_fits.csv"), ["label", "family", "x", "fixed", "intercept_ns", "slope_ns", "slope_ci95", "r2"], frows)
    write_md_table(os.path.join(gen, "micro_fits.md"), ["operation", "intercept (ns)", "slope (ns per entry)", "R²"], fmd)


# ---------------------------------------------------------------- memory


def memory(root, docs, figdir):
    paths = sorted(glob.glob(os.path.join(root, "memfill", "*.jsonl")))
    if not paths:
        return
    gen = os.path.join(docs, "generated")
    rows, md = [], []
    fig, ax = plt.subplots(figsize=(6, 4))
    for path in paths:
        meta, _, _, recs = read_run(path)
        if admissibility(meta):
            continue
        C = meta["config"]["CacheSize"]
        tag = os.path.basename(path).replace(".jsonl", "")
        put(f"mem.{tag}.encoded_enr_bytes", meta["mean_encoded_enr_bytes"])
        for variant in ("shared", "independent"):
            slopes, nodes_per_ad = [], []
            for rep in sorted({r["rep"] for r in recs}):
                pts = [(r["occupancy"], r["heap_bytes"]) for r in recs if r["type"] == "memfill" and r["variant"] == variant and r["rep"] == rep]
                xs, ys = zip(*pts)
                slopes.append(fit(xs, ys)[1])
                full = [r for r in recs if r["type"] == "memfill" and r["variant"] == variant and r["rep"] == rep and r["occupancy"] == max(xs)][0]
                nodes_per_ad.append(full["state"]["ipv4_tree_nodes"] / max(1, full["occupancy"]))
                if rep == 0:
                    ax.plot(xs, (np.array(ys) - ys[0]) / 1e6, "o-", label=f"{tag} {variant}")
            s = ci(slopes)
            npa = ci(nodes_per_ad)
            b = [r["heap_delta_bytes"] / r["topic_bounds_added"] for r in recs if r["type"] == "memfill_bounds" and r["variant"] == variant and r["topic_bounds_added"]]
            bb = ci(b)
            put(f"mem.{tag}.{variant}.bytes_per_ad", {"mean": s[0], "ci": s[1], "n": s[2]})
            put(f"mem.{tag}.{variant}.tree_nodes_per_ad", {"mean": npa[0], "ci": npa[1], "n": npa[2]})
            put(f"mem.{tag}.{variant}.bytes_per_topic_bound", {"mean": bb[0], "ci": bb[1], "n": bb[2]})
            put(f"mem.{tag}.{variant}.at_capacity_mib", s[0] * C / 2**20)
            rows.append([tag, variant, C, s[0], s[1], s[2], npa[0], bb[0], s[0] * C])
            md.append([tag, variant, C, fmt_ci(s[0], s[1], 0), fmt_num(npa[0], 2), fmt_ci(bb[0], bb[1], 0), fmt_num(s[0] * C / 2**20, 2)])
        sh = VALUES[f"mem.{tag}.shared.bytes_per_ad"]["mean"]
        ind = VALUES[f"mem.{tag}.independent.bytes_per_ad"]["mean"]
        put(f"mem.{tag}.enr_copy_bytes", ind - sh)
        # Analytic bound from the brief: C * (max encoded ENR 300 B + per-entry overhead).
        put(f"mem.{tag}.analytic_bound_mib", C * (300 + sh) / 2**20)
        put(f"mem.{tag}.measured_full_mib", C * ind / 2**20)
    ax.set_xlabel("occupancy (ads)")
    ax.set_ylabel("post-GC heap above empty table (MB)")
    ax.grid(alpha=.3)
    ax.legend(fontsize=7)
    savefig(fig, figdir, "memory_fill")
    write_csv(os.path.join(gen, "memory.csv"), ["run", "variant", "C", "bytes_per_ad", "ci95", "reps", "ipv4_tree_nodes_per_ad", "bytes_per_topic_bound", "bytes_at_capacity"], rows)
    write_md_table(os.path.join(gen, "memory.md"), ["run", "ENR", "C", "bytes per cached ad", "IPv4 tree vertices per ad", "bytes per topic bound", "MiB at occupancy = C"], md)


# ---------------------------------------------------------------- rendering


def fmt_value(key, spec):
    if key not in VALUES:
        raise KeyError(key)
    v = VALUES[key]
    kind, _, nd = (spec or "f:2").partition(":")
    nd = int(nd or 0)
    if isinstance(v, dict) and kind in ("ci", "f", "pct", "pctci"):
        m, h = v["mean"], v.get("ci", float("nan"))
    else:
        m, h = v, float("nan")
    if kind == "ci":
        return fmt_ci(m, h, nd)
    if kind == "pct":
        return f"{100 * m:.{nd}f}%"
    if kind == "pctci":
        return f"{100 * m:.{nd}f} ± {100 * h:.{nd}f}%" if not math.isnan(h) else f"{100 * m:.{nd}f}%"
    if kind == "s":
        return str(m)
    return fmt_num(m, nd)


def render(docs):
    tpl = os.path.join(docs, "registrar-overhead.md.in")
    if not os.path.exists(tpl):
        return []
    text = open(tpl).read()
    missing = []

    def table(mm):
        p = os.path.join(docs, "generated", mm.group(1) + ".md")
        if not os.path.exists(p):
            missing.append("table:" + mm.group(1))
            return "*(table not generated)*"
        return open(p).read().rstrip()

    def value(mm):
        try:
            return fmt_value(mm.group(1), mm.group(2))
        except KeyError:
            missing.append(mm.group(1))
            return "**MISSING**"

    text = re.sub(r"\{\{table:([\w.-]+)\}\}", table, text)
    text = re.sub(r"\{\{([\w.\-=/]+)(?:\|([\w:]+))?\}\}", value, text)
    header = "<!-- Generated by topicindex/cmd/registrarbench/study/study.py from registrar-overhead.md.in. Do not edit. -->\n"
    open(os.path.join(docs, "registrar-overhead.md"), "w").write(header + text)
    return missing


def main():
    root, docs = sys.argv[1], sys.argv[2]
    figdir = os.path.join(docs, "figures")
    os.makedirs(figdir, exist_ok=True)
    os.makedirs(os.path.join(docs, "generated"), exist_ok=True)
    runs, excluded, flagged, meta_of = load_sim(root)
    put("sim.runs", sum(len(v) for v in runs.values()))
    put("sim.points", len(runs))
    put("sim.excluded", len(excluded))
    put("sim.flagged", len(flagged))
    put("sim.min_seeds", min((len(v) for v in runs.values()), default=0))
    write_md_table(os.path.join(docs, "generated", "sim_excluded.md"), ["run", "reason"], [(os.path.relpath(p, root), r) for p, r in excluded] or [("–", "none")])
    write_md_table(os.path.join(docs, "generated", "sim_flagged.md"), ["run", "flags"], [(os.path.relpath(p, root), r) for p, r in flagged] or [("–", "none")])
    if meta_of:
        meta = next(iter(meta_of.values()))
        put("sim.platform", meta["platform"])
        put("sim.dataset_sha256", meta["dataset"]["sha256"])
    summary = aggregate(runs, docs, figdir)
    sim_figures(summary, meta_of, figdir)
    microbench(root, docs, figdir)
    memory(root, docs, figdir)
    for extra in sorted(glob.glob(os.path.join(os.path.dirname(os.path.abspath(__file__)), "study_*.py"))):
        # Optional extension modules (e.g. realtime analysis) share VALUES.
        ns = {"__name__": "study_ext", "VALUES": VALUES, "put": put, "read_run": read_run, "admissibility": admissibility,
              "ci": ci, "fit": fit, "write_csv": write_csv, "write_md_table": write_md_table, "fmt_ci": fmt_ci,
              "fmt_num": fmt_num, "savefig": savefig, "plt": plt, "np": np}
        exec(compile(open(extra).read(), extra, "exec"), ns)
        ns["run"](root, docs, figdir)
    json.dump(VALUES, open(os.path.join(docs, "generated", "values.json"), "w"), indent=1, sort_keys=True, default=float)
    missing = render(docs)
    if len(runs) and min(len(v) for v in runs.values()) < MIN_SEEDS:
        print(f"warning: some points have fewer than {MIN_SEEDS} seeds", file=sys.stderr)
    if missing:
        print("missing values: " + ", ".join(sorted(set(missing))), file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
