#!/usr/bin/env python3
"""Offline registrarbench JSONL analysis. Requires matplotlib (and its NumPy dependency)."""

import argparse
from array import array
from collections import defaultdict
import csv
from dataclasses import dataclass, field
import hashlib
import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np


@dataclass
class Run:
    path: Path
    name: str
    metadata: dict = field(default_factory=dict)
    summary: dict = field(default_factory=dict)
    samples: list = field(default_factory=list)
    # Compact columns: start, method latency, utilisation, rank, population, cache size.
    operations: dict = field(default_factory=lambda: {
        kind: [array("d") for _ in range(6)] for kind in ("registration", "query", "expiry")
    })

    @property
    def mode(self):
        return self.metadata.get("mode", self.metadata["config"].get("Mode", "realtime"))

    @property
    def realtime_valid(self):
        return (self.valid and self.mode == "realtime"
                and self.summary.get("realtime_metrics_valid") is not False)

    @property
    def audit(self):
        return bool(self.metadata.get("allocation_audit") or
                    self.metadata["config"].get("AllocationAudit") or
                    self.summary.get("allocation_audit"))

    @property
    def valid(self):
        return (not self.audit and self.summary.get("performance_valid") is True
                and self.metadata.get("performance_valid") is not False)

    @property
    def label(self):
        c = self.metadata["config"]
        mode = "audit" if self.audit else ("primary" if self.valid else "invalid performance")
        return (f"{self.name} | {self.mode} {mode} | reg={c['RegRate']:g}/s, query={c['QueryRate']:g}/s\n"
                f"N={c['Advertisers']}, topics={c['Topics']}, α={c['Alpha']:g}, "
                f"capacity={c['CacheSize']}, lifetime={c['Lifetime'] / 1e9:g}s")

    def values(self, kind):
        cols = [np.asarray(col) for col in self.operations[kind]]
        # Benchmark emits measured operations only. Also enforce its actual boundary.
        mask = cols[0] >= self.summary["measurement_start_ns"]
        return [col[mask] for col in cols]

    def compatibility(self):
        # Only paths and the two swept rates are deliberately ignored.
        ignored = {"RegRate", "QueryRate", "Output", "IPFile", "CPUProfile"}
        cfg = {k: v for k, v in self.metadata["config"].items() if k not in ignored}
        cfg["cpu_profile_enabled"] = bool(self.metadata["config"].get("CPUProfile"))
        ds = dict(self.metadata["dataset"])
        ds["source"] = "synthetic" if ds.get("source") == "synthetic" else "dataset"
        return {"population_model": self.metadata.get("population_model", "legacy"), "mode": self.mode, "registration_semantics": self.metadata.get("registration_semantics", "legacy-slots"), "config": cfg, "dataset": ds,
                "runtime": {k: self.metadata.get(k) for k in (
                    "go_version", "revision", "os", "arch", "gomaxprocs", "godebug",
                    "gogc_env", "gomemlimit_env", "result_limit")},
                "topic_populations": self.metadata.get("topic_populations"),
                "audit": self.audit, "performance_valid": self.valid}

    @property
    def group(self):
        encoded = json.dumps(self.compatibility(), sort_keys=True).encode()
        return hashlib.sha256(encoded).hexdigest()[:12]


def read_run(path, index):
    run = Run(path, f"{index:02d}_{path.stem}")
    fields = ("started_ns", "latency_ns", "utilisation", "topic_rank",
              "topic_population", "topic_cache_size")
    with path.open(encoding="utf-8") as source:
        for line_no, line in enumerate(source, 1):
            if not line.strip():
                continue
            try:
                record = json.loads(line)
                kind = record["type"]
                if kind in ("metadata", "summary"):
                    if getattr(run, kind):
                        raise ValueError(f"duplicate {kind}")
                    setattr(run, kind, record)
                elif kind == "sample":
                    run.samples.append(record)
                elif kind == "operation":
                    if record.get("performance_valid") is False or record.get("measurement") is False:
                        continue
                    op = record["operation"]
                    if op not in run.operations:
                        raise ValueError(f"unsupported operation {op!r}")
                    values = [float(record[key]) for key in fields]
                    if not all(np.isfinite(values)) or any(v < 0 for v in values):
                        raise ValueError("nonfinite or negative operation field")
                    for col, value in zip(run.operations[op], values):
                        col.append(value)
                elif kind in ("retry_expired", "population_scarcity"):
                    continue  # Lifecycle event, not a registrar method call.
                else:
                    raise ValueError(f"unknown record type {kind!r}")
            except (ValueError, KeyError, TypeError) as exc:
                raise ValueError(f"{path}:{line_no}: {exc}") from exc
    if not run.metadata or not run.summary:
        raise ValueError(f"{path}: metadata and final summary required; run may be incomplete")
    # Fail clearly on a different schema rather than silently infer experiment settings.
    for key in ("Advertisers", "Topics", "Alpha", "CacheSize", "Lifetime", "RegRate", "QueryRate"):
        if key not in run.metadata["config"]:
            raise ValueError(f"{path}: missing config.{key}")
    if run.summary.get("measurement_seconds", 0) <= 0:
        raise ValueError(f"{path}: missing or invalid measurement duration")
    if "measurement_start_ns" not in run.summary:
        raise ValueError(f"{path}: missing actual measurement boundary")
    run.samples.sort(key=lambda s: s["elapsed_ns"])
    return run


def save(fig, directory, name, title, caption):
    fig.suptitle(title, fontsize=10)
    fig.text(.5, .015, caption, ha="center", va="bottom", fontsize=7)
    for ax in fig.axes:
        ax.grid(True, alpha=.2)
        ax.spines[["top", "right"]].set_visible(False)
    fig.tight_layout(rect=(0, .065, 1, .91))
    directory.mkdir(parents=True, exist_ok=True)
    for ext in ("png", "pdf"):
        fig.savefig(directory / f"{name}.{ext}", dpi=300, bbox_inches="tight",
                    metadata={"Title": title, "Subject": caption} if ext == "pdf"
                    else {"Title": title, "Description": caption})
    plt.close(fig)


def sample_plots(run, directory, notes):
    if not run.samples:
        notes.append(f"{run.name}: skipped state/resource figures: no samples")
        return
    fig, ax = plt.subplots(figsize=(7, 4))
    for measured, label, style in ((False, "Warm-up", "--"), (True, "Measurement", "-")):
        samples = [s for s in run.samples if s["measurement"] is measured]
        if samples:
            ax.plot([s["elapsed_ns"] / 1e9 for s in samples],
                    [s["utilisation"] * 100 for s in samples], style, marker=".", label=label)
    boundary = run.summary["measurement_start_ns"] / 1e9
    ax.axvspan(0, boundary, color="0.8", alpha=.25)
    ax.axvline(boundary, color="0.4", lw=.8)
    ax.set(xlabel=f"Elapsed {'logical' if run.mode == 'simulated' else 'wall'} time (s)", ylabel="Cache utilisation (%)")
    ax.legend()
    save(fig, directory, "cache_over_time", run.label, "Shading denotes warm-up; continuous registrar state.")

    for metric, filename, ylabel in (
        ("incremental_live_heap_bytes", "incremental_heap", "Incremental live Go heap (MiB)"),
        ("rss_bytes", "process_rss", "Whole-process RSS (MiB)"),
    ):
        fig, ax = plt.subplots(figsize=(7, 4))
        count = 0
        for measured, label, marker in ((False, "Warm-up", "x"), (True, "Measurement", "o")):
            samples = [s for s in run.samples if s["measurement"] is measured
                       and metric in s["memory"] and
                       not (metric == "rss_bytes" and (s["memory"].get("rss_error") or
                                                      s["memory"][metric] <= 0))]
            count += len(samples)
            if samples:
                ax.scatter([s["utilisation"] * 100 for s in samples],
                           [s["memory"][metric] / 2**20 for s in samples],
                           marker=marker, s=22, alpha=.65, label=label)
        post = run.summary.get("post_measurement_forced_gc_memory", {})
        state = run.summary.get("final_state", {})
        if metric in post and state.get("capacity", 0) > 0 and not (
            metric == "rss_bytes" and (post.get("rss_error") or post[metric] <= 0)
        ):
            ax.scatter([100 * state["occupancy"] / state["capacity"]], [post[metric] / 2**20],
                       marker="*", s=100, color="black", label="Post-measurement forced GC")
            count += 1
        if not count:
            plt.close(fig)
            notes.append(f"{run.name}: skipped {filename}: no valid measurements")
            continue
        ax.set(xlabel="Cache utilisation at sampling time (%)", ylabel=ylabel)
        ax.legend(fontsize=8)
        caption = ("Last-GC live heap minus post-population baseline; not necessarily contemporaneous with occupancy."
                   if metric != "rss_bytes" else "RSS includes the entire process; it is not registrar-only memory.")
        save(fig, directory, filename, run.label, caption)
    rss_errors = sum(bool(s["memory"].get("rss_error")) for s in run.samples)
    if rss_errors:
        notes.append(f"{run.name}: omitted {rss_errors} RSS samples reporting errors")

    fields = (("ipv4_tree_nodes", "IPv4 trie nodes"), ("ipv6_tree_nodes", "IPv6 trie nodes"),
              ("topic_bounds", "Topic bound entries"),
              ("effective_topic_bounds", "Effective topic bounds"),
              ("effective_ip_bounds", "Effective IP bounds"), ("occupied_topics", "Occupied topics"))
    fig, ax = plt.subplots(figsize=(8, 4.5))
    present = False
    for color, (field_name, label) in zip(plt.rcParams["axes.prop_cycle"].by_key()["color"], fields):
        for measured, marker in ((False, "x"), (True, "o")):
            samples = [s for s in run.samples if s["measurement"] is measured and field_name in s["state"]]
            if samples:
                present = True
                ax.scatter([s["utilisation"] * 100 for s in samples],
                           [s["state"][field_name] for s in samples], color=color,
                           marker=marker, s=18, alpha=.65,
                           label=label if measured or not any(s["measurement"] for s in run.samples) else None)
    if present:
        ax.set(xlabel="Cache utilisation (%)", ylabel="Retained structures (count)")
        ax.legend(fontsize=7, ncol=2)
        save(fig, directory, "admission_state", run.label,
             "Structural counts, not byte estimates. × warm-up; ○ measurement.")
    else:
        plt.close(fig)
        notes.append(f"{run.name}: skipped admission state: structural counts absent")


def composition_plot(run, directory, notes):
    if not run.valid:
        return
    samples = [s for s in run.samples if "counts" in s]
    if len(samples) < 2:
        notes.append(f"{run.name}: skipped registration composition: cumulative counts unavailable")
        return
    rows = []
    keys = ("registrations_processed", "fresh_attempts", "retry_attempts", "new_admissions",
            "wait_responses", "expired_retries", "population_scarcity_events")
    for a, b in zip(samples, samples[1:]):
        dt = (b["elapsed_ns"] - a["elapsed_ns"]) / 1e9
        if dt <= 0:
            continue
        row = {"elapsed_seconds": b["elapsed_ns"] / 1e9,
               "measurement_only": a["elapsed_ns"] >= run.summary["measurement_start_ns"]}
        for key in keys:
            row[key] = (b["counts"].get(key, 0) - a["counts"].get(key, 0)) / dt
        rows.append(row)
    write_csv(directory / "registration_composition.csv", rows)
    fig, axes = plt.subplots(2, 1, figsize=(7, 6), sharex=True)
    for ax, series in zip(axes, (keys[:3], keys[3:])):
        for key in series:
            ax.plot([r["elapsed_seconds"] for r in rows], [r[key] for r in rows], label=key.replace("_", " "))
        ax.axvspan(0, run.summary["measurement_start_ns"] / 1e9, color="0.8", alpha=.25)
        ax.set_ylabel(f"Events / {run.mode == 'simulated' and 'logical' or 'wall'} second")
        ax.legend(fontsize=7)
    axes[0].axhline(run.metadata["config"]["RegRate"], linestyle="--", color="black", label="Target total rate")
    axes[0].legend(fontsize=7)
    axes[1].set_xlabel(f"Elapsed {run.mode == 'simulated' and 'logical' or 'wall'} time (s)")
    save(fig, directory, "registration_composition", run.label,
         "Sample-interval rates; warm-up shaded. Expired retries do not call Register.")


def latency_plots(run, directory, bins, notes):
    if not run.valid:
        notes.append(f"{run.name}: excluded latency/CPU/throughput from primary figures (audit or invalid)")
        return
    for kind in run.operations:
        values = run.values(kind)
        latency = values[1] / 1000  # ns -> microseconds, never queue_delay_ns
        if not len(latency):
            notes.append(f"{run.name}: skipped {kind} latency: no measured raw operations")
            continue
        # Rank and realized population describe popularity; cache size describes current load.
        dimensions = ((values[2] * 100, "utilisation", "Cache utilisation (%)", False),
                      (values[3], "rank", "Topic rank (1 = most popular)", True),
                      (values[4], "population", "Realized advertiser population of topic", True),
                      (values[5], "topic_size", "Cached advertisements for requested topic", False))
        if kind == "expiry":
            dimensions = dimensions[:1]
        rows = []
        for x, name, xlabel, log_axis in dimensions:
            unique = np.unique(x)
            if len(unique) <= bins:
                assignments = np.searchsorted(unique, x)
            else:
                # Equal-frequency bins in the observed x coordinate; ties are never split.
                edges = np.unique(np.quantile(x, np.linspace(0, 1, bins + 1)))
                assignments = np.searchsorted(edges[1:-1], x, side="right")
            groups = []
            for group in np.unique(assignments):
                mask = assignments == group
                observed, times = x[mask], latency[mask]
                p50, p95, p99 = np.percentile(times, (50, 95, 99))
                row = {"dimension": name, "x_min": float(observed.min()),
                       "x_max": float(observed.max()), "x_median": float(np.median(observed)),
                       "count": int(mask.sum()), "median_us": float(p50),
                       "p95_us": float(p95), "p99_us": float(p99)}
                rows.append(row)
                groups.append(row)
            fig, ax = plt.subplots(figsize=(7, 4))
            for metric, label, marker in (("median_us", "Median", "o"), ("p95_us", "p95", "s"),
                                          ("p99_us", "p99", "^")):
                ax.plot([g["x_median"] for g in groups], [g[metric] for g in groups],
                        marker=marker, markersize=4, lw=1, label=label)
            if log_axis and np.all(x > 0) and max(x) > min(x):
                ax.set_xscale("log")
            ax.set(xlabel=xlabel, ylabel=f"{kind.capitalize()} method latency (µs)")
            ax.legend()
            save(fig, directory, f"{kind}_latency_vs_{name}", run.label,
                 "Measured operations only; exact raw-data quantiles within x bins. Bin ranges/counts in CSV.")
        write_csv(directory / f"{kind}_latency_bins.csv", rows)


def audit_metrics(run):
    denominators = {"registration": "registrations_processed", "query": "queries_processed",
                    "expiry": "expiry_calls"}
    counts = run.summary.get("counts", {})
    return {key: value for key, value in run.summary.get("audit_per_operation", {}).items()
            if counts.get(denominators.get(key.split("_")[0], ""), 0) > 0}


def audit_plot(run, directory, notes):
    if not run.audit:
        return
    metrics = audit_metrics(run)
    if not metrics:
        notes.append(f"{run.name}: skipped per-operation allocation figure: audit metrics absent")
        return
    fig, axes = plt.subplots(1, 2, figsize=(8, 4))
    for ax, metric, label in zip(axes, ("bytes", "allocations"),
                                 ("Allocated bytes / operation", "Allocations / operation")):
        names = [k for k in ("registration", "query", "expiry") if f"{k}_{metric}" in metrics]
        ax.bar(names, [metrics[f"{k}_{metric}"] for k in names], color="#0072B2")
        ax.set_ylabel(label)
        ax.tick_params(axis="x", labelsize=8)
    save(fig, directory, "allocation_audit", run.label,
         "Separate instrumented audit; CPU, latency and throughput are not primary performance results.")


def write_csv(path, rows):
    if not rows:
        return
    keys = list(dict.fromkeys(key for row in rows for key in row))
    with path.open("w", newline="", encoding="utf-8") as out:
        writer = csv.DictWriter(out, fieldnames=keys)
        writer.writeheader()
        writer.writerows(rows)


def population_flags(summary):
    # Preserve unknown values for legacy outputs without phase-specific flags.
    return {"population_limited_any": summary.get("population_limited_any", summary.get("population_limited")),
            "population_limited_measurement": summary.get("population_limited_measurement", summary.get("measured_population_limited"))}


def summary_row(run):
    s, c = run.summary, run.metadata["config"]
    row = {"run": run.name, "file": str(run.path.resolve()), "compatibility_group": run.group,
           "allocation_audit": run.audit, "performance_valid": run.valid,
           "offered_registration_rps": c["RegRate"], "offered_query_rps": c["QueryRate"],
           "measurement_seconds": s["measurement_seconds"]}
    row.update({f"config_{k}": v for k, v in c.items()})
    row.update({f"dataset_{k}": v for k, v in run.metadata["dataset"].items()})
    row.update({k: v for k, v in s.items() if isinstance(v, (int, float, bool, str))})
    row.update(population_flags(s))
    row["performance_valid"] = run.valid
    row["allocation_audit"] = run.audit
    row.update({f"count_{k}": v for k, v in s.get("counts", {}).items()})
    eligible = s.get("counts", {}).get("eligible_registration_arrivals")
    row["eligible_registration_rps"] = eligible / s["measurement_seconds"] if eligible is not None else ""
    if run.audit:
        row.update({f"audit_{k}": v for k, v in audit_metrics(run).items()})
    row.update({f"final_{k}": v for k, v in s.get("final_state", {}).items()})
    row.update({f"post_gc_{k}": v for k, v in s.get("post_measurement_forced_gc_memory", {}).items()})
    if run.valid:
        for kind in run.operations:
            raw = run.values(kind)[1] / 1000
            if len(raw):
                row[f"{kind}_raw_count"] = len(raw)
                row[f"{kind}_mean_us"] = float(raw.mean())
                for label, percentile in (("median", 50), ("p95", 95), ("p99", 99)):
                    row[f"{kind}_{label}_us"] = float(np.percentile(raw, percentile))
    return row


def cross_run_plots(runs, output, notes):
    compatible = defaultdict(list)
    for run in runs:
        compatible[run.group].append(run)
    for group, members in compatible.items():
        directory = output / f"group_{group}"
        directory.mkdir(parents=True, exist_ok=True)
        (directory / "configuration.json").write_text(
            json.dumps({"compatibility": members[0].compatibility(),
                        "runs": [r.name for r in members]}, indent=2) + "\n", encoding="utf-8")
        pairs = {(r.metadata["config"]["RegRate"], r.metadata["config"]["QueryRate"])
                 for r in members}
        if members[0].realtime_valid and len(pairs) >= 2:
            for key, label, scale in (
                ("cpu_percent_one_core", "Process CPU (% of one core)", 1),
                ("allocation_bytes_per_second", "Process allocation rate (MiB/s)", 1 / 2**20),
                ("allocations_per_second", "Process allocations per second", 1),
            ):
                available = [r for r in members if isinstance(r.summary.get(key), (float, int))]
                if len(available) < 2:
                    notes.append(f"group {group}: skipped {key} load map: insufficient metrics")
                    continue
                fig, ax = plt.subplots(figsize=(7, 4.5))
                points = ax.scatter([r.metadata["config"]["RegRate"] for r in available],
                                    [r.metadata["config"]["QueryRate"] for r in available],
                                    c=[r.summary[key] * scale for r in available],
                                    s=65, cmap="viridis", edgecolors="black", linewidths=.5)
                fig.colorbar(points, ax=ax, label=label)
                ax.set(xlabel="Offered registration rate (requests/s)",
                       ylabel="Offered query rate (requests/s)")
                save(fig, directory, f"{key}_load_map", f"Compatible primary group {group}",
                     "Independent mixed-load points; color shows whole-process cost, including harness.")
        # To vary one offered rate, hold the other rate constant. Never conflate a mixed-load sweep.
        for rate_key, other_key, rate_label in (("RegRate", "QueryRate", "registration"),
                                                ("QueryRate", "RegRate", "query")):
            slices = defaultdict(list)
            for run in members:
                slices[run.metadata["config"][other_key]].append(run)
            for fixed, subset in slices.items():
                xs = [r.metadata["config"][rate_key] for r in subset]
                if len(set(xs)) < 2:
                    notes.append(f"group {group}: skipped {rate_label}-rate comparisons at "
                                 f"{other_key}={fixed:g}: need ≥2 distinct offered-load points")
                    continue
                audit, valid = subset[0].audit, subset[0].realtime_valid
                if not audit and not valid:
                    notes.append(f"group {group}: simulated or invalid performance runs excluded from CPU/saturation cross-run plots")
                    continue
                plots = []
                if valid:
                    plots.extend([
                        ("cpu", "Process CPU (% of one core)", [("cpu_percent_one_core", "Process CPU", 1)]),
                        ("allocated_bytes", "Process allocation rate (MiB/s)",
                         [("allocation_bytes_per_second", "Process allocated bytes", 1 / 2**20)]),
                        ("allocation_count", "Process allocations per second",
                         [("allocations_per_second", "Process allocations", 1)]),
                    ])
                    if rate_key == "RegRate":
                        plots.append(("registration_throughput", "Requests / admissions per second", [
                            ("fresh_registration_rps", "Fresh attempts", 1),
                            ("retry_rps", "Retry attempts", 1),
                            ("registration_rps", "Processed registrations", 1),
                            ("admission_rps", "New successful admissions", 1)]))
                    else:
                        plots.append(("query_throughput", "Topic queries per second",
                                      [("query_rps", "Processed queries", 1)]))
                if audit:
                    for metric, unit in (("bytes", "Allocated bytes / operation"),
                                         ("allocations", "Allocations / operation")):
                        plots.append((f"audit_{metric}", unit,
                                      [(f"audit_{kind}_{metric}", kind.capitalize(), 1)
                                       for kind in ("registration", "query", "expiry")]))
                rows = [summary_row(r) for r in subset]
                for name, ylabel, series in plots:
                    fig, ax = plt.subplots(figsize=(7, 4))
                    plotted = False
                    colors = plt.rcParams["axes.prop_cycle"].by_key()["color"]
                    for color, marker, (key, label, scale) in zip(colors, ("o", "+", "^"), series):
                        pairs = [(x, row[key] * scale) for x, row in zip(xs, rows)
                                 if isinstance(row.get(key), (int, float))]
                        if pairs:
                            plotted = True
                            # Independent run points: no pooling or averaging of repeated runs.
                            style = ({"facecolors": "none", "edgecolors": color} if marker == "o"
                                     else {"color": color})
                            ax.scatter(*zip(*pairs), marker=marker, s=55, alpha=.8, label=label, **style)
                    if not plotted:
                        plt.close(fig)
                        notes.append(f"group {group}: skipped {name}: metric absent")
                        continue
                    if "throughput" in name:
                        ax.plot([min(xs), max(xs)], [min(xs), max(xs)], "--", color="0.4",
                                lw=1, label="Configured offered rate")
                    ax.set(xlabel=f"Offered {rate_label} rate (requests/s)", ylabel=ylabel)
                    ax.legend(fontsize=8)
                    title = (f"Compatible group {group} | {'allocation audit' if audit else 'primary'}\n"
                             f"{other_key} fixed at {fixed:g}/s; {len(subset)} runs")
                    caption = ("Audit instrumentation: not a primary performance measurement." if audit else
                               "Process-wide costs include harness/GC/output; independent runs, no pooling.")
                    save(fig, directory, f"{name}_vs_{rate_key}_fixed_{fixed:g}", title, caption)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("files", nargs="+", type=Path, help="completed registrarbench JSONL runs")
    parser.add_argument("-o", "--output-dir", type=Path, default=Path("registrarbench-plots"))
    parser.add_argument("--bins", type=int, default=20, help="maximum x bins for latency curves (default: 20)")
    args = parser.parse_args()
    if args.bins < 1:
        parser.error("--bins must be positive")
    if len({p.resolve() for p in args.files}) != len(args.files):
        parser.error("same input supplied more than once")
    plt.rcParams.update({"font.family": "DejaVu Sans", "font.size": 9,
                         "pdf.fonttype": 42, "savefig.facecolor": "white"})
    try:
        runs = [read_run(path, i) for i, path in enumerate(args.files, 1)]
        args.output_dir.mkdir(parents=True, exist_ok=True)
        notes = ["Only performance_valid=true, non-audit runs enter primary performance plots.",
                 "Raw latency is method time, not queueing time; quantiles use NumPy linear interpolation.",
                 "Popularity: rank 1 is most popular; realized population is advertiser count per topic.",
                 "Latency bins use unique x values or equal-frequency boundaries with unsplit ties.",
                 "Sparse-bin quantiles are descriptive, not confidence intervals; inspect CSV counts.",
                 "Live heap is the last GC estimate, compared with occupancy at sampling time.",
                 "Compatibility is based on recorded settings; machine identity is not recorded by the benchmark."]
        for run in runs:
            flags = population_flags(run.summary)
            notes.append(f"{run.name}: population_limited_any={flags['population_limited_any']}; "
                         f"population_limited_measurement={flags['population_limited_measurement']} "
                         "(measurement-phase load-validity flag).")
            if flags["population_limited_measurement"]:
                notes.append(f"{run.name}: measurement population scarcity; reject/re-run this load point "
                             "if full measurement-phase offered load is required.")
            directory = args.output_dir / run.name
            directory.mkdir(parents=True, exist_ok=True)
            (directory / "metadata.json").write_text(json.dumps(run.metadata, indent=2) + "\n", encoding="utf-8")
            write_csv(directory / "summary.csv", [summary_row(run)])
            sample_plots(run, directory, notes)
            composition_plot(run, directory, notes)
            latency_plots(run, directory, args.bins, notes)
            audit_plot(run, directory, notes)
            if run.valid:
                for kind, count_key in (("registration", "registrations_processed"), ("query", "queries_processed")):
                    actual = len(run.values(kind)[1])
                    expected = run.summary.get("counts", {}).get(count_key)
                    if expected is not None and actual != expected:
                        notes.append(f"{run.name}: WARNING {kind} raw count {actual} != summary count {expected}; "
                                     "raw quantiles describe available records only")
        cross_run_plots(runs, args.output_dir, notes)
        write_csv(args.output_dir / "runs.csv", [summary_row(r) for r in runs])
        (args.output_dir / "analysis.txt").write_text("\n".join(notes) + "\n", encoding="utf-8")
        print(f"Wrote analysis for {len(runs)} run(s) to {args.output_dir}")
        for note in notes:
            if "skipped" in note or "WARNING" in note:
                print(note)
    except (OSError, ValueError, KeyError, TypeError) as exc:
        parser.exit(1, f"analysis error: {exc}\n")


if __name__ == "__main__":
    main()
