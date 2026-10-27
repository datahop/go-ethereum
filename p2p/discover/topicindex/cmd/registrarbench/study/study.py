#!/usr/bin/env python3
"""Aggregates registrar-overhead results into tables, figures and the write-up.

Usage (from p2p/discover):
    study.py <results-root> <docs-dir>

Reads
    <results-root>/sim/<experiment>/<point>/seed<k>.jsonl   simulated runs
    <results-root>/memfill/*.jsonl                          controlled-fill memory runs
    <results-root>/microbench/bench.txt, env.json           testing.B output
    <results-root>/audit/*.jsonl                            allocation audits
    <results-root>/pacing/*.jsonl                           sleep vs spin pacing check
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
        "max_topic_ads": max((max(x["ads_per_topic"] or [0]) for x in meas), default=0),
        "top_topic_cache_share": np.mean([x["ads_per_topic"][0] / x["state"]["occupancy"] for x in meas if x["state"]["occupancy"]]) if meas else float("nan"),
        "top_topic_population_share": meta["topic_populations"][0] / sum(meta["topic_populations"]),
        "wrapped_waits": s["counts"]["abandoned_wrapped_wait"],
        "single_quote_share": s["counts"]["abandoned_single_quote"] / s["counts"]["abandoned_attempts"] if s["counts"]["abandoned_attempts"] else float("nan"),
        "abandon_policy": meta["config"]["Abandon"],
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
                                            "expired_retry_rps", "max_topic_ads", "top_topic_cache_share",
                                            "top_topic_population_share", "single_quote_share"]
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

    # Complexity fits: (family, x parameter, fixed parameters). Each is fitted
    # over the cache-resident range (x <= 256 for topic size) and the full range.
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
        ranges = [("all", None)]
        if xk == "T":
            ranges.insert(0, ("small", 256))
        ux = sorted(set(x for x, _ in pts))
        ax.errorbar(ux, [np.mean([y for x, y in pts if x == u]) for u in ux],
                    yerr=[np.std([y for x, y in pts if x == u]) for u in ux], fmt="o", capsize=3)
        for rname, lim in ranges:
            sel = [(x, y) for x, y in pts if lim is None or x <= lim]
            if len({x for x, _ in sel}) < 3:
                continue
            xs, ys = zip(*sel)
            a, b, r2, bci = fit(xs, ys)
            key = f"fit.{fam}.{xk}.{rname}"
            put(key + ".slope", {"mean": b, "ci": bci})
            put(key + ".intercept", a)
            put(key + ".r2", r2)
            rng = f"{xk} ≤ {lim}" if lim else f"{xk} ≤ {max(xs):g}"
            frows.append([label, fam, xk, rng, json.dumps(fixed), a, b, bci, r2])
            fmd.append([label, rng, fmt_num(a, 0), fmt_ci(b, bci, 2), f"{r2:.3f}"])
            xx = np.linspace(0, max(xs), 50)
            ax.plot(xx, a + b * xx, "--", lw=1, label=f"{rng}: {a:.0f} + {b:.2f}·{xk} ns")
        ax.set_xscale("symlog", linthresh=16)
        ax.set_yscale("log")
        ax.set_title(label, fontsize=9)
        ax.set_xlabel(xk)
        ax.set_ylabel("ns/op")
        ax.legend(fontsize=7)
        ax.grid(alpha=.3)
    savefig(fig, figdir, "microbench_complexity")
    write_csv(os.path.join(gen, "micro_fits.csv"), ["label", "family", "x", "range", "fixed", "intercept_ns", "slope_ns", "slope_ci95", "r2"], frows)
    write_md_table(os.path.join(gen, "micro_fits.md"), ["operation", "fit range", "intercept (ns)", "slope (ns per entry)", "R²"], fmd)


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
        put(f"mem.{tag}.bound_ratio", ind / (300 + sh))
    ax.set_xlabel("occupancy (ads)")
    ax.set_ylabel("post-GC heap above empty table (MB)")
    ax.grid(alpha=.3)
    ax.legend(fontsize=7)
    savefig(fig, figdir, "memory_fill")
    write_csv(os.path.join(gen, "memory.csv"), ["run", "variant", "C", "bytes_per_ad", "ci95", "reps", "ipv4_tree_nodes_per_ad", "bytes_per_topic_bound", "bytes_at_capacity"], rows)
    write_md_table(os.path.join(gen, "memory.md"), ["run", "ENR", "C", "bytes per cached ad", "IPv4 tree vertices per ad", "bytes per topic bound", "MiB at occupancy = C"], md)


def audit(root, docs):
    """Allocation audit: bytes and allocations per registrar call, across seeds."""
    paths = sorted(glob.glob(os.path.join(root, "audit", "*.jsonl")))
    per = defaultdict(list)
    for path in paths:
        meta, s, _, _ = read_run(path)
        if admissibility(meta) or not s:
            continue
        for k, v in s["audit_per_operation"].items():
            per[k].append(v)
        c = s["counts"]
        per["expiry_bytes_per_entry"].append(c["expiry_alloc_bytes"] / c["expired_entries"])
        per["expiry_allocations_per_entry"].append(c["expiry_allocations"] / c["expired_entries"])
    rows = []
    for k, xs in sorted(per.items()):
        m = ci(xs)
        put(f"audit.{k}", {"mean": m[0], "ci": m[1], "n": m[2]})
        rows.append([k.replace("_", " "), fmt_ci(m[0], m[1], 2), m[2]])
    if rows:
        write_md_table(os.path.join(docs, "generated", "audit.md"), ["quantity per call", "mean ± 95% CI", "seeds"], rows)


IDLE_BUCKETS = [(0, 1e6, "< 1 ms"), (1e6, 1e7, "1–10 ms"), (1e7, 1e8, "10–100 ms"), (1e8, float("inf"), "≥ 100 ms")]


def op_latencies(path):
    """Per-operation latency (ns), queue delay, and the idle time before each
    operation (end of the previous operation to the start of this one)."""
    lat, qd, idle = defaultdict(list), [], defaultdict(list)
    prev_end = None
    with open(path) as f:
        for line in f:
            if '"type":"operation"' not in line:
                continue
            r = json.loads(line)
            lat[r["operation"]].append(r["latency_ns"])
            qd.append(r["queue_delay_ns"])
            if prev_end is not None:
                idle[r["operation"]].append((r["started_ns"] - prev_end, r["latency_ns"]))
            prev_end = r["started_ns"] + r["latency_ns"]
    return {k: np.array(v) for k, v in lat.items()}, np.array(qd), {k: np.array(v) for k, v in idle.items()}


def pacing(root, docs):
    """Sleep vs spin pacing check (hybrid runs with raw operation records)."""
    paths = sorted(glob.glob(os.path.join(root, "pacing", "*.jsonl")))
    rows, idle_rows = [], []
    for path in paths:
        meta, s, _, _ = read_run(path)
        if admissibility(meta) or not s:
            continue
        tag = os.path.basename(path).replace(".jsonl", "")
        lat, qd, idle = op_latencies(path)
        for op in ("registration", "query"):
            g = idle.get(op)
            if g is None or not len(g):
                continue
            cells = []
            for lo, hi, label in IDLE_BUCKETS:
                m = (g[:, 0] >= lo) & (g[:, 0] < hi)
                cells.append(f"{np.median(g[m, 1]) / 1e3:.2f} ({m.sum()})" if m.sum() >= 20 else "–")
            idle_rows.append([tag, op] + cells)
        put(f"pacing.{tag}.cpu_percent", s["cpu_percent_one_core"])
        put(f"pacing.{tag}.cpu_us_per_op", s["cpu_us_per_operation"])
        put(f"pacing.{tag}.requests_per_s", s["rates"]["registration_rps"] + s["rates"]["query_rps"])
        put(f"pacing.{tag}.performance_valid", s["performance_valid"])
        put(f"pacing.{tag}.queue_delay_p50_us", float(np.percentile(qd, 50)) / 1e3)
        put(f"pacing.{tag}.queue_delay_p99_us", float(np.percentile(qd, 99)) / 1e3)
        for op in ("registration", "query"):
            x = lat.get(op)
            if x is None or not len(x):
                continue
            for q in (0, 50, 99):
                put(f"pacing.{tag}.{op}.p{q}_us", float(np.percentile(x, q)) / 1e3)
            put(f"pacing.{tag}.{op}.mean_us", float(x.mean()) / 1e3)
            rows.append([tag, op, len(x), fmt_num(np.min(x) / 1e3, 2), fmt_num(np.median(x) / 1e3, 2), fmt_num(np.percentile(x, 99) / 1e3, 2), fmt_num(x.mean() / 1e3, 2),
                         fmt_num(np.percentile(qd, 50) / 1e3, 2), fmt_num(s["cpu_percent_one_core"], 1)])
    if rows:
        write_md_table(os.path.join(docs, "generated", "pacing.md"),
                       ["run", "operation", "calls", "min µs", "p50 µs", "p99 µs", "mean µs", "queue delay p50 µs (all ops)", "process CPU % of one core"], rows)
        write_md_table(os.path.join(docs, "generated", "pacing_idle.md"),
                       ["run", "operation"] + [f"p50 µs after idle {label} (calls)" for _, _, label in IDLE_BUCKETS], idle_rows)
    # Reference: the same calls in simulated mode, where operations run back to back.
    for point in ("C1000-fresh5", "C1000-fresh100"):
        for op in ("registration", "query"):
            xs = []
            for path in glob.glob(os.path.join(root, "sim", "demand", point, "seed*.jsonl")):
                meta, s, _, _ = read_run(path)
                if s and not admissibility(meta):
                    xs.append(s[f"{op}_latency"]["median_ns_upper"] / 1e3)
            if xs:
                m = ci(xs)
                put(f"simlat.{point}.{op}.p50_us", {"mean": m[0], "ci": m[1], "n": m[2]})


CONFIG_COLS = [("CacheSize", "C"), ("Lifetime", "E"), ("FreshRate", "fresh/s"), ("QueryRate", "query/s"), ("Advertisers", "advertisers"),
               ("Topics", "topics"), ("Alpha", "Zipf α"), ("IPDist", "IP dist."), ("Abandon", "abandon"), ("Arrivals", "arrivals"),
               ("Duration", "duration"), ("Warmup", "warm-up")]


def fmt_cfg(k, v):
    if k in ("Lifetime", "Duration", "Warmup"):
        sec = v / 1e9
        return f"{sec / 3600:g} h" if sec >= 3600 else f"{sec / 60:g} min"
    return str(v)


def cfg_sort_key(x):
    try:
        return (0, float(x.split()[0]), x)
    except ValueError:
        return (1, 0, x)


def sim_config(meta_of, docs):
    """Per experiment: the parameters that vary across its points, and the fixed ones."""
    by_exp = defaultdict(list)
    for (exp, point), meta in sorted(meta_of.items()):
        by_exp[exp].append(meta["config"])
    rows = []
    for exp, cfgs in by_exp.items():
        varied = [label + " ∈ {" + ", ".join(sorted({fmt_cfg(k, c[k]) for c in cfgs}, key=cfg_sort_key)) + "}" for k, label in CONFIG_COLS if len({str(c[k]) for c in cfgs}) > 1]
        fixed = [f"{label} = {fmt_cfg(k, cfgs[0][k])}" for k, label in CONFIG_COLS if len({str(c[k]) for c in cfgs}) == 1]
        rows.append([exp, len(cfgs), "; ".join(varied), "; ".join(fixed)])
    write_md_table(os.path.join(docs, "generated", "sim_config.md"), ["experiment", "points", "varied", "fixed"], rows)


PRODUCTION = ["topicindex", ":(exclude)topicindex/cmd", ":(exclude)topicindex/*_test.go", ":(exclude)topicindex/stats.go"]


AB_PREFIX = "ab-"


def ab(root, docs):
    """A/B: each <root>/ab-<name>/<exp>/<point> against <root>/sim/<exp>/<point>
    (Welch's t-test on the per-seed values)."""
    rows = defaultdict(list)
    keys = [("utilisation", "occupancy / C", 3), ("admission_rps", "admissions/s", 3), ("abandon_fraction", "abandoned / fresh", 3),
            ("requests_per_admission", "REGTOPIC per admission", 1), ("cumwait_waited_median_s", "cum. wait p50 s", 0)]
    for d in sorted(glob.glob(os.path.join(root, AB_PREFIX + "*", "*", "*"))):
        name = d.split(os.sep)[-3][len(AB_PREFIX):]
        exp, point = d.split(os.sep)[-2:]
        a, b = [], []
        for path in sorted(glob.glob(os.path.join(root, "sim", exp, point, "seed*.jsonl"))):
            meta, s, samples, _ = read_run(path)
            if s and not admissibility(meta):
                a.append(run_metrics(meta, s, samples))
        for path in sorted(glob.glob(os.path.join(d, "seed*.jsonl"))):
            meta, s, samples, _ = read_run(path)
            if s and not admissibility(meta):
                b.append(run_metrics(meta, s, samples))
        if not a or not b:
            continue
        for k, label, nd in keys:
            xa, xb = [m[k] for m in a], [m[k] for m in b]
            ma, mb = ci(xa), ci(xb)
            p = stats.ttest_ind(xa, xb, equal_var=False).pvalue if len(xa) > 1 and len(xb) > 1 else float("nan")
            rel = (mb[0] - ma[0]) / ma[0] if ma[0] else float("nan")
            put(f"ab.{name}.{point}.{k}.base", {"mean": ma[0], "ci": ma[1], "n": ma[2]})
            put(f"ab.{name}.{point}.{k}.fix", {"mean": mb[0], "ci": mb[1], "n": mb[2]})
            put(f"ab.{name}.{point}.{k}.rel", rel)
            put(f"ab.{name}.{point}.{k}.p", p)
            rows[name].append([point, label, fmt_ci(ma[0], ma[1], nd), fmt_ci(mb[0], mb[1], nd), f"{100 * rel:+.1f}%", fmt_num(p, 3)])
    for name, r in rows.items():
        write_md_table(os.path.join(docs, "generated", f"ab_{name}.md"), ["point", "metric", "current code", "candidate fix", "change", "Welch p"], r)


def revisions(root, docs):
    """Lists the revisions that produced the results and checks that the
    production registrar code is identical across them."""
    import subprocess
    revs = defaultdict(list)
    for path in glob.glob(os.path.join(root, "**", "*.jsonl"), recursive=True):
        with open(path) as f:
            meta = json.loads(f.readline())
        if meta.get("type") == "metadata" and "platform" in meta:
            revs[meta["platform"]["revision"]].append(os.path.relpath(path, root).split(os.sep)[0])
    env = os.path.join(root, "microbench", "env.json")
    if os.path.exists(env):
        revs[json.load(open(env))["revision"]].append("microbench")
    if not revs:
        return
    ab = {rev for rev, where in revs.items() if all(w.startswith(AB_PREFIX) for w in where)}
    ref = sorted(set(revs) - ab)[0]
    rows, same = [], True
    for rev, where in sorted(revs.items()):
        diff = subprocess.run(["git", "diff", "--quiet", ref, rev, "--"] + PRODUCTION, capture_output=True).returncode
        if rev in ab:
            status = "candidate fix (A/B only)" if diff else "identical"
        else:
            same = same and diff == 0
            status = "identical" if diff == 0 else "DIFFERS"
        rows.append([rev[:12], ", ".join(f"{k} ({where.count(k)})" for k in sorted(set(where))), status])
    put("revisions.count", len(revs))
    put("revisions.production_identical", "yes" if same else "no")
    write_md_table(os.path.join(docs, "generated", "revisions.md"), ["revision", "results (files)", f"production registrar code vs {ref[:12]}"], rows)


OCC_RE = re.compile(r"\s(\d+)% (\S+)\s+WaitTime=(\S+)\s+Register=(\S+)\s+admitted=(\w+)\s+wire=(\d+)ms client=(\S+) ticket=(.*?) topicBounds=(\d+)")
IP_RE = re.compile(r"(\S+)/(\S+)\s+scores differing from spec:\s+(\d+)/(\d+) \(mean \|diff\| ([\d.]+)\); tree vertices with wrong counter: (\d+)/(\d+)")


def short_duration(d):
    """Abbreviates a Go duration string to its leading unit."""
    m = re.match(r"(-?)(\d+)h", d)
    if m:
        h = int(m.group(2))
        return f"{m.group(1)}{h / 8766:,.0f} y" if h >= 8766 else f"{m.group(1)}{h:,} h"
    return d


def findings(root, docs):
    """Parses the conformance and high-occupancy test logs (run_findings.sh)."""
    gen = os.path.join(docs, "generated")
    occ = defaultdict(dict)
    for path in sorted(glob.glob(os.path.join(root, "findings", "*.txt"))):
        arch = os.path.basename(path).replace(".txt", "")
        text = open(path).read()
        head = text.splitlines()[0]
        put(f"findings.{arch}.header", head)
        if "dirty=false" not in head:
            print(f"warning: {path} was produced from a dirty tree", file=sys.stderr)
        if arch.endswith("-strict"):
            put(f"findings.{arch}.overflow_violations", len(re.findall(r"v5_topic_overflow_test.go:\d+: \d+%", text)))
            m = re.search(r"(\d+)/(\d+) scores differ from spec, (\d+)/(\d+) tree counters wrong", text)
            if m:
                put(f"findings.{arch}.ip_scores_differ", f"{m.group(1)}/{m.group(2)}")
                put(f"findings.{arch}.ip_counters_wrong", f"{m.group(3)}/{m.group(4)}")
            put(f"findings.{arch}.failed_tests", ", ".join(re.findall(r"--- FAIL: (\w+)", text)))
            continue
        for m in re.finditer(r"(\S+): GOARCH=\w+ first overflowing occupancy N=(\d+) of C=(\d+)", text):
            put(f"findings.{arch}.threshold.{m.group(1)}", int(m.group(2)))
        th = [v for k, v in VALUES.items() if k.startswith(f"findings.{arch}.threshold.")]
        if th:
            put(f"findings.{arch}.threshold_min", min(th))
            put(f"findings.{arch}.threshold_max", max(th))
        for m in OCC_RE.finditer(text):
            pct, case, req, reg, adm, wire, client, ticket, _ = m.groups()
            if adm == "true":
                cell = "**admitted** without credit"
            elif client.startswith("-"):
                cell = f"wait {short_duration(reg)}; advertiser decodes **{short_duration(client)}**, retries at once; ticket rejected"
            else:
                cell = f"wait {short_duration(reg)}" + ("" if ticket == "valid" else f"; ticket: {ticket}")
            occ[(int(pct), case)][arch] = cell
        if arch == "arm64":
            rows = []
            for m in IP_RE.finditer(text):
                shape, hist, d, n, md, w, v = m.groups()
                rows.append([shape, hist, f"{d}/{n}", md, f"{w}/{v}"])
            if rows:
                write_md_table(os.path.join(gen, "findings_ip.md"), ["address set", "history", "scores ≠ spec", "mean |Δscore|", "tree vertices with wrong counter"], rows)
            m = re.search(r"implemented/spec wait ratio: min ([\d.]+) median ([\d.]+) max ([\d.]+)", text)
            if m:
                put("findings.wait_ratio_median", float(m.group(2)))
                put("findings.wait_ratio_max", float(m.group(3)))
            m = re.search(r"of (\d+) quotes: spec > E in (\d+), implemented > E in (\d+), implemented <= 1s floor in (\d+)", text)
            if m:
                n = int(m.group(1))
                put("findings.quotes", n)
                put("findings.spec_over_E", int(m.group(2)) / n)
                put("findings.impl_over_E", int(m.group(3)) / n)
                put("findings.impl_floor", int(m.group(4)) / n)
            m = re.search(r"ticket issued to A \(wait (\S+)\), presented by B after the wait: confirmed=(\w+), B in cache=(\w+)", text)
            if m:
                put("findings.ticket_transfer_wait", m.group(1))
                put("findings.ticket_transfer_admitted", m.group(3))
            m = re.search(r"single quoted wait at (\d+)% occupancy, single topic: (\S+) \(E", text)
            if m:
                put("findings.nocap_occupancy_pct", int(m.group(1)))
                put("findings.nocap_wait", m.group(2))
    archs = sorted({a for v in occ.values() for a in v})
    if occ:
        write_md_table(os.path.join(gen, "findings_overflow.md"), ["occupancy", "case"] + [f"GOARCH={a}" for a in archs],
                       [[f"{pct}%", case] + [occ[(pct, case)].get(a, "–") for a in archs] for pct, case in sorted(occ)])


def equilibrium_model():
    """Occupancy u at which a fresh request's quoted wait reaches the attempt
    timeout 1.5*E, ignoring bounds and the 1 s floor:
    0.1*E*(x+s)/(1-u)^10 = 1.5*E  =>  u* = 1 - ((x+s)/15)^(1/10),
    where x = T/(N+1) is the topic share and s the IP score."""
    for xs in (0.1, 0.25, 0.5, 1.0, 2.0):
        put(f"model.ustar.{xs}", 1 - (xs / 15) ** 0.1)
        # Without the implementation's 0.1 factor (spec: E * occupancy * (...)).
        put(f"model.ustar_spec.{xs}", 1 - (xs / 1.5) ** 0.1)
    # The spec's safety constant G = 1e-7 (discv5-theory.md parameter table):
    # its contribution E*G/(1-u)^10, and the occupancy above which it alone
    # exceeds the implementation's 1 s admission floor.
    E, G = 900.0, 1e-7
    put("model.G_wait_empty_s", E * G)
    put("model.G_wait_at_30pct_ms", 1e3 * E * G / 0.7 ** 10)
    put("model.G_exceeds_floor_u", 1 - (E * G) ** 0.1)


# ---------------------------------------------------------------- rendering


def lookup(key):
    """Returns VALUES[key], descending into dict values for dotted suffixes."""
    if key in VALUES:
        return VALUES[key]
    parts = key.split(".")
    for i in range(len(parts) - 1, 0, -1):
        head = ".".join(parts[:i])
        if head in VALUES:
            v = VALUES[head]
            for p in parts[i:]:
                if not isinstance(v, dict) or p not in v:
                    raise KeyError(key)
                v = v[p]
            return v
    raise KeyError(key)


def fmt_value(key, spec):
    v = lookup(key)
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

    # Sections are template fragments in sections/<name>.md.in.
    def section(mm):
        p = os.path.join(docs, "sections", mm.group(1) + ".md.in")
        if not os.path.exists(p):
            missing.append("section:" + mm.group(1))
            return "*(section not written)*"
        return open(p).read().rstrip()

    text = re.sub(r"\{\{section:([\w.-]+)\}\}", section, text)

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
    allm = [m for v in runs.values() for m in v]
    if allm:
        put("sim.max_utilisation", max(m["max_utilisation"] for m in allm))
        put("sim.max_utilisation_production", max(m["max_utilisation"] for m in allm if m["abandon_policy"] == "production"))
        put("sim.wrapped_waits", sum(m["wrapped_waits"] for m in allm))
        put("sim.max_immediate_fraction", max(m["immediate_fraction"] for m in allm))
        put("sim.max_bounds_minus_topics", max(m["max_bounds_minus_topics"] for m in allm))
        put("sim.little_min", min(m["little_ratio"] for m in allm))
        put("sim.little_max", max(m["little_ratio"] for m in allm))
    summary = aggregate(runs, docs, figdir)
    sim_config(meta_of, docs)
    sim_figures(summary, meta_of, figdir)
    microbench(root, docs, figdir)
    memory(root, docs, figdir)
    audit(root, docs)
    pacing(root, docs)
    revisions(root, docs)
    ab(root, docs)
    findings(root, docs)
    equilibrium_model()
    json.dump(VALUES, open(os.path.join(docs, "generated", "values.json"), "w"), indent=1, sort_keys=True, default=float)
    missing = render(docs)
    if len(runs) and min(len(v) for v in runs.values()) < MIN_SEEDS:
        print(f"warning: some points have fewer than {MIN_SEEDS} seeds", file=sys.stderr)
    if missing:
        print("missing values: " + ", ".join(sorted(set(missing))), file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
