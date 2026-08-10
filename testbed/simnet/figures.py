#!/usr/bin/env python3
"""Generate figures and a Markdown report from a simnet testbed multi-topic
metrics JSON file.

Usage:
    figures.py <metrics.json> [--out-dir DIR] [--label LABEL] [--params KEY=VAL ...]

Each --params entry is stamped into the report's parameters table.

Produces in <DIR> (default ./figures-<label>):

    01_topic_distribution.{png,pdf}        nodes per topic
    02_time_to_first_cdf.{png,pdf}         CDF of time-to-first result, all searchers
    03_unique_found_over_time.{png,pdf}    per-topic mean ± std of unique registrants
                                           found vs time (drives off uniqueFoundAtMs)
    04_id_space_registrants.{png,pdf}      strip plot of admitted registrants in
                                           ID-space, one row per topic
    05_id_space_found_vs_missed.{png,pdf}  per-topic discovery coverage across ID space
    06_fanout_both_views.{png,pdf}         side-by-side: (a) per-registrant fan-out
                                           and (b) per-host load, per topic
    07_registration_latency_bar.{png,pdf}  mean ± std time-to-first-remote-admission
                                           per topic, clipped at 0
    report.md                              Markdown report with embedded figures + tables
"""
import argparse
import collections
import json
import os
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

matplotlib.rcParams.update({
    "font.size": 12,
    "axes.titlesize": 13,
    "axes.labelsize": 12,
    "legend.fontsize": 10,
    "xtick.labelsize": 11,
    "ytick.labelsize": 11,
    "figure.dpi": 110,
    "savefig.bbox": "tight",
    "pdf.fonttype": 42,
    "ps.fonttype": 42,
})


def load(path):
    with open(path) as f:
        return json.load(f)


# Helper: searchers' foundIds use TerminalString (16-hex prefix); coverage byRegistrant
# uses full String() (64-hex). Truncate both to the first 16 chars to match.
def short_id(id_str):
    return id_str[:16]


def per_topic_fanout(topic_idx, cov_by_topic):
    cov_t = cov_by_topic.get(str(topic_idx), {})
    fan = list(cov_t.get("byRegistrant", {}).values())
    fan.sort()
    return fan


def per_topic_hostload(topic_idx, cov_by_topic):
    """How many distinct registrants of this topic each host holds in its
    topic table. Drives the (b) side of the fan-out figure."""
    cov_t = cov_by_topic.get(str(topic_idx), {})
    load = list(cov_t.get("byHost", {}).values())
    load.sort()
    return load


def per_registrant_discovery(results_for_topic, cov_by_topic, topic_idx):
    """Returns (sorted list of 'found-by-N-searchers' counts, list of registrant short IDs)."""
    fanout = {short_id(k): v for k, v in cov_by_topic.get(str(topic_idx), {}).get("byRegistrant", {}).items()}
    found_count = collections.Counter()
    for r in results_for_topic:
        for fid in set(r.get("foundIds") or []):
            found_count[fid] += 1
    counts = [found_count.get(rid, 0) for rid in fanout]
    counts.sort()
    return counts, list(fanout.keys())


def unique_recall_per_searcher(results_for_topic, cov_by_topic, topic_idx):
    fanout = {short_id(k): v for k, v in cov_by_topic.get(str(topic_idx), {}).get("byRegistrant", {}).items()}
    fanout_set = set(fanout.keys())
    target = len(fanout_set) - 1
    out = []
    for r in results_for_topic:
        uniq = len(set(r.get("foundIds") or []) & fanout_set)
        out.append(uniq)
    out.sort()
    return out, target


# ────────────────────────────────────────────────────────────────────────────
# Figure plotters
# ────────────────────────────────────────────────────────────────────────────


def plot_topic_distribution(per_topic, ax, label):
    """01: nodes per topic. Ordered by descending count for legibility."""
    topics = sorted(per_topic, key=lambda r: -r["target"])
    xs = list(range(len(topics)))
    ys = [r["numSearchers"] for r in topics]
    ax.bar(xs, ys, color="#3066BE")
    ax.set_xticks(xs)
    ax.set_xticklabels([f"topic {r['topic']}" for r in topics])
    ax.set_xlabel("topic")
    ax.set_ylabel("nodes")
    ax.set_title(f"{label}: nodes per topic (Zipf draw — each node both registers and searches)")
    for i, y in enumerate(ys):
        ax.text(i, y + max(ys) * 0.01, str(y), ha="center", fontsize=9)


def plot_time_to_first_cdf(results, ax, label):
    """02: CDF of time-to-first result across all searchers."""
    vals = [r["timeToFirstNs"] / 1e6 for r in results if r["timeToFirstNs"] > 0]
    if not vals:
        ax.set_title(f"{label}: no first-result data")
        return
    xs = np.sort(vals)
    ys = np.arange(1, len(xs) + 1) / len(xs)
    ax.plot(xs, ys, linewidth=1.8, color="#3066BE")
    ax.set_xlabel("time to first result (ms)")
    ax.set_ylabel("CDF over searchers")
    ax.set_title(f"{label}: time-to-first CDF (all searchers)")
    ax.grid(alpha=0.3)
    ax.set_ylim(0, 1.0)


def plot_unique_found_over_time(per_topic, results, ax, label):
    """03: per-topic mean ± std of unique-registrants-found over wall time.

    For each searcher, uniqueFoundAtMs is the timestamp (ms since search
    start) at which the i-th distinct registrant was first observed. We
    sample the per-searcher curves on a common time grid and plot the
    cross-searcher mean ± 1σ band per topic.
    """
    by_topic = collections.defaultdict(list)
    for r in results:
        by_topic[r["topic"]].append(r)
    # Cap the plotted window at 50s — discovery is essentially complete by
    # the first few seconds, the rest is plateau that visually hides the
    # early-growth dynamics. Per-searcher data beyond 50s is still counted
    # via cumulative-at-cap, it's just not rendered.
    x_limit_ms = 50_000
    grid = np.linspace(0, x_limit_ms, 200)
    cmap = plt.cm.tab10
    for i, rec in enumerate(sorted(per_topic, key=lambda r: -r["target"])):
        t = rec["topic"]
        rs = by_topic[t]
        per_search_curves = []
        for r in rs:
            ts = r.get("uniqueFoundAtMs") or []
            if not ts:
                per_search_curves.append(np.zeros_like(grid))
                continue
            ts_sorted = np.sort(ts)
            counts = np.searchsorted(ts_sorted, grid, side="right")
            per_search_curves.append(counts.astype(float))
        if not per_search_curves:
            continue
        arr = np.vstack(per_search_curves)
        mean = arr.mean(axis=0)
        std = arr.std(axis=0)
        color = cmap(i % 10)
        ax.plot(grid, mean, linewidth=1.6, color=color,
                label=f"topic {t} ({rec['target']} target)")
        ax.fill_between(grid, np.maximum(mean - std, 0), mean + std,
                        alpha=0.18, color=color)
    ax.set_xlabel("time since search start (ms)")
    ax.set_ylabel("unique registrants found (mean ± 1σ)")
    ax.set_title(f"{label}: unique registrants discovered over time, per topic (first 50 s)")
    ax.legend(loc="lower right", fontsize=9)
    ax.grid(alpha=0.3)
    ax.set_xlim(0, x_limit_ms)
    ax.set_ylim(bottom=0)


def plot_id_space_registrants(per_topic, cov_by_topic, ax, label):
    """04: strip plot of registrants present in ≥1 registrar's topic table.

    Each dot is one registrant placed at its (normalised) top-64-bit ID
    position on the x axis. Rows are topics, ordered ascending by topic
    index for consistency across runs.
    """
    topics = sorted(per_topic, key=lambda r: r["topic"])
    rows = []
    for r in topics:
        t = r["topic"]
        ids = list(cov_by_topic.get(str(t), {}).get("byRegistrant", {}).keys())
        rows.append((t, len(ids), ids))
    for row, (t, n, ids) in enumerate(rows):
        if not ids:
            continue
        xs = [int(i[:16], 16) / float(2**64) for i in ids]
        ax.scatter(xs, [row] * len(xs), s=14, alpha=0.7, color=plt.cm.tab10(row % 10))
    ax.set_yticks(range(len(rows)))
    ax.set_yticklabels([f"topic {t} ({n} regs admitted)" for t, n, _ in rows])
    ax.set_xlim(0, 1.0)
    ax.set_xlabel("ID position (top 64 bits, normalised 0..1)")
    ax.set_title(f"{label}: ID-space distribution of registrants admitted to ≥1 registrar")
    ax.grid(True, axis="x", alpha=0.3)


def plot_id_space_found_vs_missed(per_topic, results, cov_by_topic, fig, label):
    """05: per-topic discovery coverage across the ID space.

    One subplot per topic: x = registrant ID position, y = number of
    searchers in the simulation that returned that registrant via the
    iterator. Topics ordered ascending by index.
    """
    topics = sorted(per_topic, key=lambda r: r["topic"])
    n = len(topics)
    if n == 0:
        return
    axs = fig.subplots(nrows=n, ncols=1, sharex=True)
    if n == 1:
        axs = [axs]
    by_topic = collections.defaultdict(list)
    for r in results:
        by_topic[r["topic"]].append(r)
    for ax, rec in zip(axs, topics):
        t = rec["topic"]
        rs = by_topic[t]
        fanout = {short_id(k): v for k, v in cov_by_topic.get(str(t), {}).get("byRegistrant", {}).items()}
        found_count = collections.Counter()
        for r in rs:
            for fid in set(r.get("foundIds") or []):
                found_count[fid] += 1
        n_searchers = rec["numSearchers"]
        sorted_regs = sorted(fanout.keys(), key=lambda i: int(i[:16], 16))
        xs = [int(i[:16], 16) / float(2**64) for i in sorted_regs]
        ys = [found_count.get(i, 0) for i in sorted_regs]
        colors = ["#2A9D8F" if y >= n_searchers
                  else "#E76F51" if y < n_searchers * 0.9 else "#F4A261"
                  for y in ys]
        ax.scatter(xs, ys, c=colors, s=18, alpha=0.85)
        ax.axhline(n_searchers, color="black", linestyle="--", linewidth=1, alpha=0.4)
        ax.set_xlim(0, 1.0)
        ax.set_ylim(-1, n_searchers + 5)
        ax.set_ylabel(f"topic {t}\ntimes found", fontsize=9)
        ax.grid(True, axis="x", alpha=0.3)
    axs[-1].set_xlabel("registrant ID position (top 64 bits, normalised 0..1)")
    fig.suptitle(f"{label}: per-registrant discovery coverage across ID space", fontsize=12)


def plot_fanout_both_views(per_topic, cov_by_topic, fig, label, num_hosts):
    """06: per-topic side-by-side box plots — (a) per-registrant fan-out
    (how many registrars hold each registrant's ad), and (b) per-host load
    (how many registrants of this topic each host holds)."""
    topics = sorted(per_topic, key=lambda r: r["topic"])
    ax_a, ax_b = fig.subplots(nrows=1, ncols=2, sharey=False)

    # (a) per-registrant fan-out
    data_a = []
    labels_list = []
    for r in topics:
        t = r["topic"]
        fan = per_topic_fanout(t, cov_by_topic)
        if not fan:
            continue
        data_a.append(fan)
        labels_list.append(f"topic {t}\n({r['target']} target)")
    if data_a:
        bp = ax_a.boxplot(data_a, showfliers=True, patch_artist=True)
        for box in bp["boxes"]:
            box.set(facecolor="#3066BE", alpha=0.6)
        ax_a.set_xticks(range(1, len(labels_list) + 1))
        ax_a.set_xticklabels(labels_list)
    ax_a.set_ylabel(f"registrars holding each registrant (cap = {num_hosts - 1})")
    ax_a.set_title("(a) per-registrant fan-out")
    ax_a.grid(True, axis="y", alpha=0.3)

    # (b) per-host load
    data_b = []
    for r in topics:
        t = r["topic"]
        load = per_topic_hostload(t, cov_by_topic)
        if not load:
            continue
        data_b.append(load)
    if data_b:
        bp = ax_b.boxplot(data_b, showfliers=True, patch_artist=True)
        for box in bp["boxes"]:
            box.set(facecolor="#E76F51", alpha=0.6)
        ax_b.set_xticks(range(1, len(labels_list) + 1))
        ax_b.set_xticklabels(labels_list)
    ax_b.set_ylabel("registrants of this topic per host")
    ax_b.set_title("(b) per-host load")
    ax_b.grid(True, axis="y", alpha=0.3)

    fig.suptitle(f"{label}: fan-out two views — per registrant vs per host", fontsize=12)


def plot_registration_latency_bar(per_topic, reg_timing, ax, label):
    """07: mean ± std time-to-first-remote-admission per topic. Bars clipped
    at zero so the lower error bar never crosses zero."""
    if not reg_timing:
        ax.set_title(f"{label}: no registration timing data")
        return
    # Sort by topic index (ascending) to match other per-topic figures.
    topics = sorted(per_topic, key=lambda r: r["topic"])
    # Look up timing per topic by matching the topic hex prefix (makeTopic
    # encodes the index as a big-endian uint32 in bytes 3..6 = hex chars 6..14).
    def topic_to_hex(t):
        for hex_id in reg_timing:
            try:
                if int(hex_id[6:14], 16) == t:
                    return hex_id
            except (ValueError, IndexError):
                pass
        return None
    means_ms = []
    stds_ms = []
    counts = []
    xs_labels = []
    for r in topics:
        t = r["topic"]
        hex_id = topic_to_hex(t)
        if hex_id is None or not reg_timing[hex_id]:
            continue
        vals_ms = [d / 1e6 for d in reg_timing[hex_id].values()]
        means_ms.append(float(np.mean(vals_ms)))
        stds_ms.append(float(np.std(vals_ms)))
        counts.append(len(vals_ms))
        xs_labels.append(f"topic {t}\n({r['target']} target)")
    if not means_ms:
        ax.set_title(f"{label}: no registration timing data")
        return
    means_ms = np.array(means_ms)
    stds_ms = np.array(stds_ms)
    # Asymmetric error bars: lower error capped so the bar never dips below 0.
    lower_err = np.minimum(stds_ms, means_ms)
    upper_err = stds_ms
    xs = np.arange(len(means_ms))
    ax.bar(xs, means_ms, yerr=[lower_err, upper_err], capsize=6, color="#3066BE",
           edgecolor="black", alpha=0.85, error_kw={"linewidth": 1.2})
    ax.set_xticks(xs)
    ax.set_xticklabels(xs_labels)
    ax.set_ylabel("time to first remote admission (ms) — mean ± 1σ")
    ax.set_title(f"{label}: registration latency per topic")
    ax.grid(True, axis="y", alpha=0.3)
    ax.set_ylim(bottom=0)
    for i, (m, s, c) in enumerate(zip(means_ms, stds_ms, counts)):
        ax.text(i, m + s + max(means_ms) * 0.04, f"{m:.0f}±{s:.0f}\nn={c}",
                ha="center", fontsize=9)


# ────────────────────────────────────────────────────────────────────────────
# Markdown report
# ────────────────────────────────────────────────────────────────────────────


def write_report(out_dir, label, params, per_topic, results, cov_by_topic, reg_timing, num_hosts):
    path = os.path.join(out_dir, "report.md")
    lines = []
    lines.append(f"# Simnet experiment report — `{label}`\n")

    lines.append("## Simulation parameters\n")
    lines.append("| parameter | value |")
    lines.append("|---|---|")
    for k, v in params.items():
        lines.append(f"| {k} | {v} |")
    lines.append("")

    # Figure 1 lives in the setup section — it's a visual of the topic
    # distribution drawn by the Zipf process, i.e. another input parameter.
    if os.path.exists(os.path.join(out_dir, "01_topic_distribution.png")):
        lines.append("![01_topic_distribution](01_topic_distribution.png)\n")
        lines.append("*Nodes per topic (Zipf draw).*\n")

    # Aggregate
    n_searchers = sum(r["numSearchers"] for r in per_topic)
    full = sum(r["fullRecall"] for r in per_topic)
    lines.append("## Aggregate results\n")
    lines.append("| metric | value |")
    lines.append("|---|---|")
    lines.append(f"| total nodes (every node both registers and searches its topic) | {n_searchers} |")
    lines.append(f"| topics | {len(per_topic)} |")
    lines.append(f"| full-recall searches | {full} / {n_searchers} |")
    lines.append("")

    by_topic_results = collections.defaultdict(list)
    for r in results:
        by_topic_results[r["topic"]].append(r)

    # Coverage
    lines.append("## Post-register-wait coverage\n")
    lines.append("| topic | registrants visible | fan-out min | med | max |")
    lines.append("|---:|---:|---:|---:|---:|")
    for r in sorted(per_topic, key=lambda x: -x["target"]):
        t = r["topic"]
        fan = per_topic_fanout(t, cov_by_topic)
        if not fan:
            lines.append(f"| {t} | 0 | — | — | — |")
            continue
        lines.append(f"| {t} | {len(fan)} | {fan[0]} | {fan[len(fan)//2]} | {fan[-1]} |")
    lines.append("")
    lines.append("> *Fan-out is the number of distinct hosts that hold each registrant's ad in their topic table at the moment the registration phase ends.*\n")

    # Per-searcher unique recall
    lines.append("## Per-searcher unique recall\n")
    lines.append("| topic | min | med | max | target | ≥ target |")
    lines.append("|---:|---:|---:|---:|---:|---:|")
    for r in sorted(per_topic, key=lambda x: -x["target"]):
        t = r["topic"]
        uniq, target = unique_recall_per_searcher(by_topic_results[t], cov_by_topic, t)
        if not uniq:
            continue
        atTarget = sum(1 for u in uniq if u >= target) if target > 0 else 0
        lines.append(
            f"| {t} | {uniq[0]} | {uniq[len(uniq)//2]} | {uniq[-1]} | {target} | {atTarget}/{len(uniq)} |"
        )
    lines.append("")

    lines.append("## Figures\n")
    # Figure 01 (topic distribution) is embedded above in the Simulation
    # parameters section and intentionally omitted from this list.
    figs = [
        ("02_time_to_first_cdf", "CDF of time-to-first result across all searchers."),
        ("03_unique_found_over_time", "Per-topic mean ± 1σ of unique registrants discovered over time."),
        ("04_id_space_registrants", "ID-space distribution of registrants admitted to ≥1 registrar (one row per topic)."),
        ("05_id_space_found_vs_missed", "Per-topic discovery coverage across ID space — y is the number of searchers that returned each registrant. Green = found by all, orange = some misses, red = many misses."),
        ("06_fanout_both_views", "Per-topic fan-out two views: (a) per-registrant — how many registrars hold each registrant's ad; (b) per-host — how many registrants each host holds for this topic."),
        ("07_registration_latency_bar", "Mean ± 1σ time to first remote admission per topic (clipped at 0)."),
    ]
    for stem, caption in figs:
        if os.path.exists(os.path.join(out_dir, stem + ".png")):
            lines.append(f"### {stem}\n")
            lines.append(f"![{stem}]({stem}.png)\n")
            lines.append(f"*{caption}*\n")
    lines.append("")

    with open(path, "w") as f:
        f.write("\n".join(lines))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("metrics_json")
    ap.add_argument("--out-dir", default=None)
    ap.add_argument("--label", default=None)
    ap.add_argument("--params", nargs="*", default=[],
                    help="key=value pairs stamped into the report's parameters table")
    ap.add_argument("--max-node-idx", type=int, default=0,
                    help="if >0, drop searcher results with nodeIdx >= this (excludes mid-run churn joiners; keeps stable nodes)")
    args = ap.parse_args()

    label = args.label or os.path.splitext(os.path.basename(args.metrics_json))[0]
    out = args.out_dir or f"./figures-{label}"
    os.makedirs(out, exist_ok=True)

    data = load(args.metrics_json)
    per_topic = data["perTopic"]
    results = data["results"]
    if args.max_node_idx > 0:
        results = [r for r in results if r.get("nodeIdx", 0) < args.max_node_idx]
    cov_by_topic = data.get("registrationCoverage", {}).get("byTopic", {})
    reg_timing = data.get("registrationTimingNs", {})

    num_hosts = max(
        (len(cov_by_topic.get(str(r["topic"]), {}).get("byHost", {}))
         for r in per_topic),
        default=0,
    ) or sum(r["numSearchers"] for r in per_topic)

    params = collections.OrderedDict()
    for p in args.params:
        if "=" in p:
            k, v = p.split("=", 1)
            params[k.strip()] = v.strip()

    # 01 topic distribution
    fig, ax = plt.subplots(figsize=(8, 4))
    plot_topic_distribution(per_topic, ax, label)
    fig.savefig(os.path.join(out, "01_topic_distribution.png"))
    fig.savefig(os.path.join(out, "01_topic_distribution.pdf"))
    plt.close(fig)

    # 02 time-to-first CDF
    fig, ax = plt.subplots(figsize=(7, 4))
    plot_time_to_first_cdf(results, ax, label)
    fig.savefig(os.path.join(out, "02_time_to_first_cdf.png"))
    fig.savefig(os.path.join(out, "02_time_to_first_cdf.pdf"))
    plt.close(fig)

    # 03 unique-found over time (only if per-find timestamps were captured)
    has_unique_timestamps = any(r.get("uniqueFoundAtMs") for r in results)
    if has_unique_timestamps:
        fig, ax = plt.subplots(figsize=(8, 5))
        plot_unique_found_over_time(per_topic, results, ax, label)
        fig.savefig(os.path.join(out, "03_unique_found_over_time.png"))
        fig.savefig(os.path.join(out, "03_unique_found_over_time.pdf"))
        plt.close(fig)
    else:
        print(f"[{label}] skipping figure 03 (no uniqueFoundAtMs in metrics — predates instrumentation)")

    # 04 ID-space registrants
    fig, ax = plt.subplots(figsize=(10, 0.5 * len(per_topic) + 1.5))
    plot_id_space_registrants(per_topic, cov_by_topic, ax, label)
    fig.savefig(os.path.join(out, "04_id_space_registrants.png"))
    fig.savefig(os.path.join(out, "04_id_space_registrants.pdf"))
    plt.close(fig)

    # 05 ID-space found-vs-missed grid
    fig = plt.figure(figsize=(10, 1.7 * len(per_topic) + 1.5))
    plot_id_space_found_vs_missed(per_topic, results, cov_by_topic, fig, label)
    fig.tight_layout(rect=[0, 0, 1, 0.96])
    fig.savefig(os.path.join(out, "05_id_space_found_vs_missed.png"))
    fig.savefig(os.path.join(out, "05_id_space_found_vs_missed.pdf"))
    plt.close(fig)

    # 06 fan-out, both views
    fig = plt.figure(figsize=(13, 4.5))
    plot_fanout_both_views(per_topic, cov_by_topic, fig, label, num_hosts)
    fig.tight_layout(rect=[0, 0, 1, 0.95])
    fig.savefig(os.path.join(out, "06_fanout_both_views.png"))
    fig.savefig(os.path.join(out, "06_fanout_both_views.pdf"))
    plt.close(fig)

    # 07 registration latency bar (only if probe data is present)
    has_reg_timing = bool(reg_timing) and any(v for v in reg_timing.values())
    if has_reg_timing:
        fig, ax = plt.subplots(figsize=(8, 4.5))
        plot_registration_latency_bar(per_topic, reg_timing, ax, label)
        fig.savefig(os.path.join(out, "07_registration_latency_bar.png"))
        fig.savefig(os.path.join(out, "07_registration_latency_bar.pdf"))
        plt.close(fig)
    else:
        print(f"[{label}] skipping figure 07 (no registrationTimingNs in metrics — predates instrumentation)")

    # Markdown report
    write_report(out, label, params, per_topic, results, cov_by_topic, reg_timing, num_hosts)

    # Summary
    n_searchers = sum(r["numSearchers"] for r in per_topic)
    full = sum(r["fullRecall"] for r in per_topic)
    print(f"[{label}] {n_searchers} searchers, {full}/{n_searchers} full recall across {len(per_topic)} topics")
    print(f"figures + report in: {out}")


if __name__ == "__main__":
    main()
