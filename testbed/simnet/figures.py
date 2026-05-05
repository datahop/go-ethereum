#!/usr/bin/env python3
"""Generate paper-style figures from a simnet testbed metrics JSON file.

Usage:
    figures.py <metrics.json> [--out-dir DIR] [--label LABEL]

Produces in <DIR> (default ./figures-<label>):
    01_topic_distribution.{png,pdf}    bar chart of registrants per topic
    02_recall_histogram.{png,pdf}      histogram of per-searcher recall
    03_per_topic_recall.{png,pdf}      mean recall per topic with error bars
    04_found_missed_matrix.{png,pdf}   heatmap: which registrants found by which searchers (top topic only)
    05_time_to_first_cdf.{png,pdf}     CDF of time to first result, all searchers
    06_time_to_completion_cdf.{png,pdf} CDF of time to completion, by topic
    per_searcher.csv                   one row per searcher
"""
import argparse
import collections
import csv
import json
import os
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

# Paper-style font.
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


def topic_distribution(per_topic, ax, label):
    topics = sorted(per_topic, key=lambda r: -r["target"])
    xs = list(range(len(topics)))
    ys = [r["target"] for r in topics]
    ax.bar(xs, ys, color="#3066BE")
    ax.set_xticks(xs)
    ax.set_xticklabels([str(r["topic"]) for r in topics])
    ax.set_xlabel("topic id (sorted by population)")
    ax.set_ylabel("registrants")
    ax.set_title(f"{label}: topic distribution (Zipf draw)")
    for i, y in enumerate(ys):
        ax.text(i, y + max(ys) * 0.01, str(y), ha="center", fontsize=9)


def recall_histogram(results, ax, label):
    recs = [r["foundRegistrant"] / r["target"] for r in results if r["target"] > 0]
    bins = np.linspace(0, 1.0, 21)
    ax.hist(recs, bins=bins, color="#2A9D8F", edgecolor="white")
    ax.set_xlabel("recall (foundRegistrants / target)")
    ax.set_ylabel("# searchers")
    ax.set_title(f"{label}: per-searcher recall histogram")
    ax.set_xlim(0, 1.05)
    mean = np.mean(recs)
    ax.axvline(mean, color="black", linestyle="--", linewidth=1)
    ax.text(mean - 0.02, ax.get_ylim()[1] * 0.95, f"mean={mean:.4f}", ha="right", fontsize=10)


def per_topic_recall(per_topic, ax, label):
    topics = sorted(per_topic, key=lambda r: -r["target"])
    xs = list(range(len(topics)))
    means = [r["meanRecall"] for r in topics]
    full = [r["fullRecall"] / r["numSearchers"] if r["numSearchers"] else 0 for r in topics]
    width = 0.35
    ax.bar([x - width / 2 for x in xs], means, width=width,
           label="fraction of registrants found (per-search avg)", color="#264653")
    ax.bar([x + width / 2 for x in xs], full, width=width,
           label="fraction of complete searches (found all)", color="#E76F51")
    ax.set_xticks(xs)
    ax.set_xticklabels([f"{r['topic']}\n({r['target']} regs)" for r in topics])
    ax.set_xlabel("topic id (registrants)")
    ax.set_ylabel("fraction")
    ax.set_ylim(0, 1.05)
    ax.set_title(f"{label}: discovery success per topic")
    ax.legend(loc="lower right", fontsize=9)


def found_missed_matrix(results, registrant_ids_by_topic, ax, label, topic_idx):
    """Heatmap: rows = searchers (this topic), cols = registrants. Cell=1 if found."""
    # Pick the topic with the most searchers (the Zipf head) by default.
    rs = [r for r in results if r["topic"] == topic_idx]
    if not rs:
        ax.set_title(f"{label}: topic {topic_idx} has no searchers")
        return
    # The metrics file does not track *which specific* nodes a searcher found.
    # We approximate the heatmap by: for each searcher, mark `foundRegistrant` cells
    # as found and `target - foundRegistrant` cells as missed, sorted by completion time.
    # This shows the recall profile but not which specific registrants are missed.
    rs.sort(key=lambda r: r["timeToCompletionNs"])
    target = max(r["target"] for r in rs)
    grid = np.zeros((len(rs), target), dtype=int)
    for i, r in enumerate(rs):
        n = min(r["foundRegistrant"], target)
        grid[i, :n] = 1
    ax.imshow(grid, aspect="auto", cmap="RdYlGn", vmin=0, vmax=1, interpolation="nearest")
    ax.set_xlabel(f"registrant rank (0..{target - 1})")
    ax.set_ylabel("searcher (sorted by t_completion)")
    ax.set_title(f"{label}: topic {topic_idx} — found (green) vs missed (red), {target} regs × {len(rs)} searchers")


def cdf(values, ax, label, xlabel):
    xs = np.sort(values)
    ys = np.arange(1, len(xs) + 1) / len(xs)
    ax.plot(xs, ys, label=label, linewidth=1.8)
    ax.set_xlabel(xlabel)
    ax.set_ylabel("CDF")
    ax.set_ylim(0, 1.0)


def time_to_first_cdf(results, ax, label):
    vals = [r["timeToFirstNs"] / 1e6 for r in results if r["timeToFirstNs"] > 0]
    cdf(vals, ax, label, "time to first result (ms)")
    ax.set_title(f"{label}: time-to-first CDF")
    ax.grid(alpha=0.3)


def time_to_completion_per_topic(results, ax, label):
    by_topic = collections.defaultdict(list)
    for r in results:
        by_topic[r["topic"]].append(r["timeToCompletionNs"] / 1e6)
    topics_sorted = sorted(by_topic, key=lambda t: -len(by_topic[t]))
    for t in topics_sorted:
        vals = by_topic[t]
        if not vals:
            continue
        xs = np.sort(vals)
        ys = np.arange(1, len(xs) + 1) / len(xs)
        ax.plot(xs, ys, label=f"topic {t} ({len(vals)} src)", linewidth=1.5)
    ax.set_xlabel("time to completion (ms)")
    ax.set_ylabel("CDF")
    ax.set_title(f"{label}: time-to-completion CDF, by topic")
    ax.legend(loc="lower right", ncol=2)
    ax.grid(alpha=0.3)
    ax.set_ylim(0, 1.05)


def id_space_registrants(registrants_by_topic, ax, label, max_rows=None):
    """Strip plot: each row = topic, dots = registrants placed on a [0,1] axis."""
    topics = sorted(registrants_by_topic, key=lambda t: -len(registrants_by_topic[t]))
    if max_rows is not None:
        topics = topics[:max_rows]
    for row, t in enumerate(topics):
        ids = registrants_by_topic[t]
        xs = [int(i[:16], 16) / float(2**64) for i in ids]
        ax.scatter(xs, [row] * len(xs), s=14, alpha=0.7, color=plt.cm.tab10(row % 10))
    ax.set_yticks(range(len(topics)))
    ax.set_yticklabels([f"topic {t} ({len(registrants_by_topic[t])})" for t in topics])
    ax.set_xlim(0, 1.0)
    ax.set_xlabel("ID position (top 64 bits, normalized 0..1)")
    ax.set_title(f"{label}: registrant ID-space distribution per topic")
    ax.grid(True, axis="x", alpha=0.3)


def id_space_found_vs_missed(results, registrants_by_topic, ax, label, topic_idx):
    """For one topic: aggregate found-counts per registrant across all searchers.

    Shows for each registrant how many searchers found it. A perfectly-discovered
    registrant has count == numSearchers. Missed/sticky-tail registrants stand out
    with low counts.
    """
    rs = [r for r in results if r["topic"] == topic_idx]
    regs = registrants_by_topic.get(topic_idx, [])
    if not regs or not rs:
        ax.set_title(f"{label}: topic {topic_idx} — no data")
        return
    n_searchers = len(rs)
    found_count = collections.Counter()
    for r in rs:
        for fid in r.get("foundIds") or []:
            found_count[fid] += 1
    sorted_regs = sorted(regs, key=lambda i: int(i[:16], 16))
    xs = [int(i[:16], 16) / float(2**64) for i in sorted_regs]
    ys = [found_count.get(i, 0) for i in sorted_regs]
    colors = ["#2A9D8F" if y == n_searchers else "#E76F51" if y < n_searchers * 0.5 else "#F4A261"
              for y in ys]
    ax.scatter(xs, ys, c=colors, s=22, alpha=0.85)
    ax.axhline(n_searchers, color="black", linestyle="--", linewidth=1, alpha=0.5,
               label=f"all searchers ({n_searchers})")
    ax.set_xlim(0, 1.0)
    ax.set_ylim(-1, n_searchers + 5)
    ax.set_xlabel("registrant ID position (top 64 bits, normalized 0..1)")
    ax.set_ylabel("# searchers that found this registrant")
    ax.set_title(f"{label}: topic {topic_idx} — discovery coverage per registrant ({len(regs)} registrants × {n_searchers} searchers)")
    ax.grid(True, axis="x", alpha=0.3)
    ax.legend(loc="lower right")


def registration_time_per_topic(reg_timing, registrants_by_topic_hex, ax, label):
    """CDF of time-to-first-registration-on-any-remote, per topic."""
    if not reg_timing:
        ax.set_title(f"{label}: no registration timing data")
        return
    # Sort topics by registrant count.
    items = sorted(reg_timing.items(), key=lambda kv: -len(kv[1]))
    for topic_hex, m in items:
        if not m:
            continue
        ms = sorted(d / 1e6 for d in m.values())
        ys = np.arange(1, len(ms) + 1) / len(ms)
        ax.plot(ms, ys, label=f"topic {topic_hex[:8]}… ({len(m)} regs)", linewidth=1.5)
    ax.set_xlabel("time-to-first-remote-registration (ms)")
    ax.set_ylabel("CDF over registrants")
    ax.set_title(f"{label}: registration propagation time per topic")
    ax.legend(loc="lower right", ncol=2, fontsize=9)
    ax.grid(alpha=0.3)
    ax.set_ylim(0, 1.05)


def id_space_found_vs_missed_grid(results, registrants_by_topic, fig, label):
    """One subplot per topic showing per-registrant discovery coverage.

    For each registrant on the x axis, the y value is the number of searchers
    in the simulation that returned that registrant's ID via TopicSearch.
    """
    topics = sorted(registrants_by_topic, key=lambda t: -len(registrants_by_topic[t]))
    n = len(topics)
    if n == 0:
        return
    # Stacked vertical layout, one row per topic.
    axs = fig.subplots(nrows=n, ncols=1, sharex=True)
    if n == 1:
        axs = [axs]
    for ax, t in zip(axs, topics):
        rs = [r for r in results if r["topic"] == t]
        regs = registrants_by_topic[t]
        n_searchers = len(rs)
        found_count = collections.Counter()
        for r in rs:
            for fid in r.get("foundIds") or []:
                found_count[fid] += 1
        sorted_regs = sorted(regs, key=lambda i: int(i[:16], 16))
        xs = [int(i[:16], 16) / float(2**64) for i in sorted_regs]
        ys = [found_count.get(i, 0) for i in sorted_regs]
        colors = ["#2A9D8F" if y == n_searchers
                  else ("#E76F51" if y < n_searchers * 0.5 else "#F4A261")
                  for y in ys]
        ax.scatter(xs, ys, c=colors, s=18, alpha=0.85)
        ax.axhline(n_searchers, color="black", linestyle="--", linewidth=1, alpha=0.4)
        ax.set_xlim(0, 1.0)
        ax.set_ylim(-1, n_searchers + 5)
        ax.set_ylabel(f"topic {t}\ntimes found", fontsize=10)
        ax.grid(True, axis="x", alpha=0.3)
    axs[-1].set_xlabel("registrant ID position (top 64 bits, normalized 0..1)")
    fig.suptitle(
        f"{label}: times each registrant is found, by topic\n"
        "(dashed line = total searchers; green = found by all, orange = some misses, red = many misses)",
        fontsize=12)


def registration_time_bar(reg_timing, ax, label):
    """Bar plot of mean registration latency per topic, with std error bars
    clipped at 0 (latencies are non-negative, so the lower error bar is
    capped at the bar height)."""
    if not reg_timing:
        return
    items = sorted(reg_timing.items(), key=lambda kv: -len(kv[1]))
    # Decode topic index from the topic hex string. makeTopic(i) places i as
    # a big-endian uint32 in bytes 3..6 (hex chars 6..14).
    def topic_idx(hex_str):
        try:
            return int(hex_str[6:14], 16)
        except (ValueError, IndexError):
            return -1
    means_ms = np.array([np.mean([d / 1e6 for d in m.values()]) for _, m in items])
    stds_ms = np.array([np.std([d / 1e6 for d in m.values()]) for _, m in items])
    counts = [len(m) for _, m in items]
    indices = [topic_idx(hex_id) for hex_id, _ in items]
    # Asymmetric error bars: lower error capped so the bar never dips below 0.
    lower_err = np.minimum(stds_ms, means_ms)
    upper_err = stds_ms
    xs = np.arange(len(items))
    ax.bar(xs, means_ms, yerr=[lower_err, upper_err], capsize=6, color="#3066BE",
           edgecolor="black", alpha=0.85, error_kw={"linewidth": 1.2})
    ax.set_xticks(xs)
    ax.set_xticklabels([f"topic {idx}" for idx in indices])
    ax.set_xlabel("topic")
    ax.set_ylabel("registration latency (ms) — mean ± std")
    ax.set_title(f"{label}: registration time per topic (mean + std)")
    ax.grid(True, axis="y", alpha=0.3)
    ax.set_ylim(bottom=0)
    for i, (m, s, c) in enumerate(zip(means_ms, stds_ms, counts)):
        ax.text(i, m + s + max(means_ms) * 0.04, f"{m:.0f}±{s:.0f}",
                ha="center", fontsize=9)


def registration_time_box(reg_timing, ax, label):
    """Boxplot of registration latencies per topic."""
    if not reg_timing:
        return
    items = sorted(reg_timing.items(), key=lambda kv: -len(kv[1]))
    data = [[d / 1e6 for d in m.values()] for _, m in items]
    labels = [f"topic {hex_id[:8]}\n({len(m)} regs)" for hex_id, m in items]
    bp = ax.boxplot(data, labels=labels, showfliers=True, patch_artist=True)
    for box in bp["boxes"]:
        box.set(facecolor="#3066BE", alpha=0.6)
    ax.set_ylabel("time-to-first-remote-registration (ms)")
    ax.set_title(f"{label}: registration latency distribution per topic")
    ax.grid(True, axis="y", alpha=0.3)


def write_report(out_dir, label, params, per_topic, results, registrants_by_topic, reg_timing):
    """Render a Markdown report with simulation parameters, aggregate metrics
    and per-topic tables, and references to the figures in `out_dir`."""
    path = os.path.join(out_dir, "report.md")

    # Aggregate metrics (re-derived from the data so this is self-consistent).
    n = len(results)
    eligible = [r for r in results if r["target"] > 0]
    n_eligible = len(eligible)
    n_full = sum(1 for r in eligible if r["foundRegistrant"] >= r["target"])
    n_to = sum(1 for r in results if r["hitTimeout"])
    mean_recall = float(np.mean([r["foundRegistrant"] / r["target"] for r in eligible])) if eligible else 0.0
    t1 = sorted([r["timeToFirstNs"] / 1e6 for r in results if r["timeToFirstNs"] > 0])
    tc = sorted([r["timeToCompletionNs"] / 1e6 for r in results])

    def pct(arr, p):
        return arr[(p * (len(arr) - 1)) // 100] if arr else 0

    def fmt_dur_ns(ns):
        return f"{ns/1e9:g} s" if ns >= 1e9 else f"{ns/1e6:g} ms"

    p = params or {}

    lines = []
    lines.append(f"# Simnet experiment report — `{label}`\n")
    lines.append("## Simulation parameters\n")
    lines.append("| parameter | value |")
    lines.append("|---|---|")
    lines.append(f"| nodes | {p.get('nodes', '?')} |")
    lines.append(f"| DISC-NG nodes | {p.get('numDiscNG', '?')} (frac = {p.get('discngFrac', '?')}) |")
    lines.append(f"| topics | {p.get('topics', '?')} |")
    lines.append(f"| Zipf skew (s) | {p.get('zipfS', '?')} |")
    lines.append(f"| RNG seed | {p.get('seed', '?')} |")
    lines.append(f"| per-link latency | {p.get('latencyMs', '?')} ms |")
    lines.append(f"| per-link bandwidth | {p.get('bandwidthMibps', '?')} Mibps (each direction) |")
    lines.append(f"| max bootnodes per node | {p.get('maxBootnodes', '?')} |")
    if 'bootstrapWaitNs' in p:
        lines.append(f"| bootstrap wait | {fmt_dur_ns(p['bootstrapWaitNs'])} |")
    if 'registerWaitNs' in p:
        lines.append(f"| register wait | {fmt_dur_ns(p['registerWaitNs'])} |")
    if 'searchTimeoutNs' in p:
        lines.append(f"| per-search timeout | {fmt_dur_ns(p['searchTimeoutNs'])} |")
    if 'regProbePeriodNs' in p:
        lines.append(f"| registration probe period | {fmt_dur_ns(p['regProbePeriodNs'])} |")
    lines.append("")

    lines.append("## Workload\n")
    lines.append("Each DISC-NG node is assigned exactly one topic via a single Zipf draw, "
                 "and **both registers and searches the same topic**. Every node makes one search; "
                 "a search target is the set of *other* nodes that registered the same topic "
                 "(self-exclusion). Vanilla discv5 nodes participate in routing-table maintenance "
                 "(PING / FINDNODE) but neither register nor search.\n")
    lines.append("Recall metrics are computed per searcher as `foundRegistrant / target`. "
                 "A search terminates either when `foundRegistrant ≥ target` or when the "
                 "per-search timeout fires.\n")

    lines.append("## Aggregate results\n")
    lines.append("| metric | value |")
    lines.append("|---|---|")
    lines.append(f"| searchers | {n} |")
    lines.append(f"| full-recall searchers | {n_full} / {n_eligible} |")
    lines.append(f"| timeouts | {n_to} / {n} |")
    lines.append(f"| mean recall | {mean_recall:.4f} |")
    lines.append(f"| time to first result, p50 | {pct(t1, 50):.1f} ms |")
    lines.append(f"| time to first result, p95 | {pct(t1, 95):.1f} ms |")
    lines.append(f"| time to completion, p50 | {pct(tc, 50):.1f} ms |")
    lines.append(f"| time to completion, p95 | {pct(tc, 95):.1f} ms |")
    lines.append("")

    lines.append("## Per-topic results\n")
    lines.append("| topic | regs | searchers | full recall | timeouts | mean recall | t1st p50 (ms) | tc p50 (ms) | tc p95 (ms) |")
    lines.append("|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
    for tr in sorted(per_topic, key=lambda r: -r["target"]):
        lines.append(
            f"| {tr['topic']} | {tr['target']} | {tr['numSearchers']} | "
            f"{tr['fullRecall']} / {tr['numSearchers']} | {tr['hitTimeout']} | "
            f"{tr['meanRecall']:.4f} | {tr['medianTimeToFirstNs']/1e6:.1f} | "
            f"{tr['medianTimeToFinishNs']/1e6:.1f} | {tr['p95TimeToFinishNs']/1e6:.1f} |"
        )
    lines.append("")

    if reg_timing:
        lines.append("## Registration timing per topic\n")
        lines.append("Time from `RegisterTopic` call until the registrant first appears "
                     "in *any* remote DISC-NG node's local topic table. "
                     f"Sampled by polling every {fmt_dur_ns(p.get('regProbePeriodNs', 0))}; "
                     "values therefore have step granularity equal to the probe period.\n")
        lines.append("| topic | registered | mean (ms) | std (ms) | p50 (ms) | p90 (ms) | p99 (ms) |")
        lines.append("|---|---:|---:|---:|---:|---:|---:|")
        items = sorted(reg_timing.items(), key=lambda kv: -len(kv[1]))
        for hex_id, m in items:
            ms = sorted(d / 1e6 for d in m.values())
            mean = float(np.mean(ms))
            std = float(np.std(ms))
            try:
                idx = int(hex_id[6:14], 16)
                topic_label = f"topic {idx}"
            except (ValueError, IndexError):
                topic_label = hex_id[:16] + "…"
            lines.append(
                f"| {topic_label} | {len(m)} | {mean:.0f} | {std:.0f} | "
                f"{pct(ms, 50):.0f} | {pct(ms, 90):.0f} | {pct(ms, 99):.0f} |"
            )
        lines.append("")

    lines.append("## Figures\n")
    figs = [
        ("01_topic_distribution", "Topic distribution: registrants per topic, sorted by population (Zipf head on the left)."),
        ("02_per_topic_recall", "Per-topic discovery success: bars show the per-search average fraction of registrants found and the fraction of searches that found *all* registrants (complete searches)."),
        ("03_time_to_first_cdf", "CDF of time-to-first result across all searchers."),
        ("04_time_to_completion_cdf", "Per-topic CDF of time-to-completion."),
        ("05_id_space_registrants", "Strip plot showing registrants placed in the ID space (top 64 bits, normalized 0..1), one row per topic."),
        ("06_id_space_found_vs_missed", "Per-registrant discovery coverage, one subplot per topic. x = registrant ID position, y = number of times the registrant was found across all searches in the simulation. Green = found by every searcher, orange = some misses, red = many misses."),
        ("07_registration_time_bar", "Mean registration latency per topic with std error bars (clipped at 0)."),
    ]
    for stem, caption in figs:
        if os.path.exists(os.path.join(out_dir, stem + ".png")):
            lines.append(f"### {stem}\n")
            lines.append(f"![{stem}]({stem}.png)\n")
            lines.append(f"*{caption}*\n")
    lines.append("")

    with open(path, "w") as f:
        f.write("\n".join(lines))


def write_per_searcher_csv(results, path):
    fields = ["nodeIdx", "nodeId", "topic", "target", "found", "foundRegistrant",
              "foundExtra", "timeToFirstMs", "timeToCompletionMs", "hitTimeout", "recall"]
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for r in results:
            tgt = r["target"]
            recall = r["foundRegistrant"] / tgt if tgt else None
            w.writerow({
                "nodeIdx": r["nodeIdx"],
                "nodeId": r["nodeId"],
                "topic": r["topic"],
                "target": tgt,
                "found": r["found"],
                "foundRegistrant": r["foundRegistrant"],
                "foundExtra": r["foundExtra"],
                "timeToFirstMs": r["timeToFirstNs"] / 1e6,
                "timeToCompletionMs": r["timeToCompletionNs"] / 1e6,
                "hitTimeout": int(r["hitTimeout"]),
                "recall": recall if recall is not None else "",
            })


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("metrics_json")
    ap.add_argument("--out-dir", default=None)
    ap.add_argument("--label", default=None)
    ap.add_argument("--top-topic", type=int, default=None,
                    help="topic index for the found-missed matrix (default: most-populated)")
    args = ap.parse_args()

    label = args.label or os.path.splitext(os.path.basename(args.metrics_json))[0]
    out = args.out_dir or f"./figures-{label}"
    os.makedirs(out, exist_ok=True)

    data = load(args.metrics_json)
    per_topic = data["perTopic"]
    results = data["results"]
    registrants_by_topic = {int(k): v for k, v in (data.get("registrantsByTopic") or {}).items()}
    reg_timing = data.get("registrationTimingNs") or {}
    params = data.get("params") or {}

    # 01 topic distribution
    fig, ax = plt.subplots(figsize=(8, 4))
    topic_distribution(per_topic, ax, label)
    fig.savefig(os.path.join(out, "01_topic_distribution.png"))
    fig.savefig(os.path.join(out, "01_topic_distribution.pdf"))
    plt.close(fig)

    # 02 per-topic recall (mean and full-recall fraction)
    fig, ax = plt.subplots(figsize=(9, 4.5))
    per_topic_recall(per_topic, ax, label)
    fig.savefig(os.path.join(out, "02_per_topic_recall.png"))
    fig.savefig(os.path.join(out, "02_per_topic_recall.pdf"))
    plt.close(fig)

    # 03 time-to-first CDF
    fig, ax = plt.subplots(figsize=(7, 4))
    time_to_first_cdf(results, ax, label)
    fig.savefig(os.path.join(out, "03_time_to_first_cdf.png"))
    fig.savefig(os.path.join(out, "03_time_to_first_cdf.pdf"))
    plt.close(fig)

    # 04 time-to-completion CDF per topic
    fig, ax = plt.subplots(figsize=(8, 5))
    time_to_completion_per_topic(results, ax, label)
    fig.savefig(os.path.join(out, "04_time_to_completion_cdf.png"))
    fig.savefig(os.path.join(out, "04_time_to_completion_cdf.pdf"))
    plt.close(fig)

    # 05 ID-space distribution of registrants per topic
    if registrants_by_topic:
        fig, ax = plt.subplots(figsize=(10, 0.45 * len(registrants_by_topic) + 1.5))
        id_space_registrants(registrants_by_topic, ax, label)
        fig.savefig(os.path.join(out, "05_id_space_registrants.png"))
        fig.savefig(os.path.join(out, "05_id_space_registrants.pdf"))
        plt.close(fig)

    # 06 ID-space found vs missed for ALL topics (one subplot per topic)
    if registrants_by_topic and any(r.get("foundIds") for r in results):
        n_topics = len(registrants_by_topic)
        fig = plt.figure(figsize=(10, 1.6 * n_topics + 1.5))
        id_space_found_vs_missed_grid(results, registrants_by_topic, fig, label)
        fig.tight_layout(rect=[0, 0, 1, 0.96])
        fig.savefig(os.path.join(out, "06_id_space_found_vs_missed.png"))
        fig.savefig(os.path.join(out, "06_id_space_found_vs_missed.pdf"))
        plt.close(fig)

    # 07 registration time per topic (mean ± std bar)
    if reg_timing:
        fig, ax = plt.subplots(figsize=(10, 5))
        registration_time_bar(reg_timing, ax, label)
        plt.setp(ax.get_xticklabels(), rotation=15, ha="right")
        fig.savefig(os.path.join(out, "07_registration_time_bar.png"))
        fig.savefig(os.path.join(out, "07_registration_time_bar.pdf"))
        plt.close(fig)

    # CSV: one row per searcher
    write_per_searcher_csv(results, os.path.join(out, "per_searcher.csv"))

    # Markdown report
    write_report(out, label, params, per_topic, results, registrants_by_topic, reg_timing)

    # Summary
    n = len(results)
    n_full = sum(1 for r in results if r["target"] > 0 and r["foundRegistrant"] >= r["target"])
    n_to = sum(1 for r in results if r["hitTimeout"])
    mean = np.mean([r["foundRegistrant"] / r["target"] for r in results if r["target"] > 0])
    print(f"[{label}] {n} searchers, {n_full}/{n} full recall, {n_to} timeouts, mean recall = {mean:.4f}")
    print(f"figures + CSV in: {out}")


if __name__ == "__main__":
    main()
