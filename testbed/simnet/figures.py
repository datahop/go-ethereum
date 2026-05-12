#!/usr/bin/env python3
"""Generate figures and a Markdown report from a simnet testbed metrics JSON file
(multi-topic Zipf workload — registrationCoverage.byTopic, per-topic results).

Usage:
    figures.py <metrics.json> [--out-dir DIR] [--label LABEL] [--params KEY=VAL ...]

Each --params entry is stamped into the report's parameters table.

Produces in <DIR> (default ./figures-<label>):

    01_topic_distribution.{png,pdf}        registrants per topic
    02_per_topic_recall.{png,pdf}          per-topic full-recall and mean recall
    03_per_topic_fanout.{png,pdf}          per-topic fan-out (registrants known by N hosts)
    04_per_registrant_discovery.{png,pdf}  for each topic: how many searchers found each registrant
    05_unique_recall_distribution.{png,pdf} per-topic CDF of per-searcher unique recall
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


def per_registrant_discovery(results_for_topic, cov_by_topic, topic_idx):
    """Returns (sorted list of 'found-by-N-searchers' counts, target list of registrant short IDs)."""
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


def plot_topic_distribution(per_topic, ax, label):
    topics = sorted(per_topic, key=lambda r: -r["target"])
    xs = list(range(len(topics)))
    ys = [r["target"] for r in topics]
    ax.bar(xs, ys, color="#3066BE")
    ax.set_xticks(xs)
    ax.set_xticklabels([f"topic {r['topic']}" for r in topics])
    ax.set_xlabel("topic")
    ax.set_ylabel("registrants")
    ax.set_title(f"{label}: topic distribution (Zipf draw)")
    for i, y in enumerate(ys):
        ax.text(i, y + max(ys) * 0.01, str(y), ha="center", fontsize=9)


def plot_per_topic_recall(per_topic, results, cov_by_topic, ax, label):
    topics = sorted(per_topic, key=lambda r: -r["target"])
    xs = list(range(len(topics)))
    full = [r["fullRecall"] / r["numSearchers"] if r["numSearchers"] else 0 for r in topics]
    # Compute per-search avg unique recall (true fraction, dedup) per topic.
    by_topic = collections.defaultdict(list)
    for r in results:
        by_topic[r["topic"]].append(r)
    mean_unique = []
    for r in topics:
        t = r["topic"]
        rs = by_topic[t]
        uniq, target = unique_recall_per_searcher(rs, cov_by_topic, t)
        if not uniq or target == 0:
            mean_unique.append(0.0)
        else:
            mean_unique.append(np.mean([u / target for u in uniq]))
    width = 0.35
    ax.bar([x - width / 2 for x in xs], mean_unique, width=width,
           label="fraction of registrants found (unique, per-search avg)", color="#264653")
    ax.bar([x + width / 2 for x in xs], full, width=width,
           label="fraction of complete searches (found all)", color="#E76F51")
    ax.set_xticks(xs)
    ax.set_xticklabels([f"topic {r['topic']}\n({r['target']} regs)" for r in topics])
    ax.set_xlabel("topic")
    ax.set_ylabel("fraction")
    ax.set_ylim(0, 1.05)
    ax.set_title(f"{label}: discovery success per topic")
    ax.legend(loc="lower right", fontsize=9)


def plot_time_to_first_cdf(results, ax, label):
    vals = [r["timeToFirstNs"] / 1e6 for r in results if r["timeToFirstNs"] > 0]
    if not vals:
        ax.set_title(f"{label}: no first-result data")
        return
    xs = np.sort(vals)
    ys = np.arange(1, len(xs) + 1) / len(xs)
    ax.plot(xs, ys, linewidth=1.8, color="#3066BE")
    ax.set_xlabel("time to first result (ms)")
    ax.set_ylabel("CDF")
    ax.set_title(f"{label}: time-to-first CDF (all searchers)")
    ax.grid(alpha=0.3)
    ax.set_ylim(0, 1.0)


def plot_time_to_completion_cdf(per_topic, results, ax, label):
    by_topic = collections.defaultdict(list)
    for r in results:
        by_topic[r["topic"]].append(r["timeToCompletionNs"] / 1e6)
    for rec in sorted(per_topic, key=lambda r: -r["target"]):
        t = rec["topic"]
        vals = by_topic[t]
        if not vals:
            continue
        xs = np.sort(vals)
        ys = np.arange(1, len(xs) + 1) / len(xs)
        ax.plot(xs, ys, linewidth=1.5, label=f"topic {t} ({rec['target']} regs)")
    ax.set_xlabel("time to completion (ms)")
    ax.set_ylabel("CDF")
    ax.set_title(f"{label}: time-to-completion CDF, by topic")
    ax.legend(loc="lower right", fontsize=9)
    ax.grid(alpha=0.3)
    ax.set_ylim(0, 1.05)


def plot_id_space_registrants(per_topic, cov_by_topic, ax, label):
    """Strip plot: one row per topic, registrants dotted along [0,1] ID axis."""
    topics = sorted(per_topic, key=lambda r: -r["target"])
    rows = []
    for r in topics:
        t = r["topic"]
        ids = list(cov_by_topic.get(str(t), {}).get("byRegistrant", {}).keys())
        if not ids:
            continue
        rows.append((t, len(ids), ids))
    for row, (t, n, ids) in enumerate(rows):
        xs = [int(i[:16], 16) / float(2**64) for i in ids]
        ax.scatter(xs, [row] * len(xs), s=14, alpha=0.7, color=plt.cm.tab10(row % 10))
    ax.set_yticks(range(len(rows)))
    ax.set_yticklabels([f"topic {t} ({n} regs)" for t, n, _ in rows])
    ax.set_xlim(0, 1.0)
    ax.set_xlabel("ID position (top 64 bits, normalized 0..1)")
    ax.set_title(f"{label}: registrant ID-space distribution per topic")
    ax.grid(True, axis="x", alpha=0.3)


def plot_id_space_found_vs_missed(per_topic, results, cov_by_topic, fig, label):
    """One subplot per topic: x = registrant ID position, y = #searchers that found it."""
    topics = sorted(per_topic, key=lambda r: -r["target"])
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
    axs[-1].set_xlabel("registrant ID position (top 64 bits, normalized 0..1)")
    fig.suptitle(f"{label}: times each registrant is found, by ID-space position", fontsize=12)


def plot_per_topic_fanout(cov_by_topic, per_topic, ax, label, num_hosts):
    topics = sorted(per_topic, key=lambda r: -r["target"])
    data = []
    labels_list = []
    for r in topics:
        t = r["topic"]
        fan = per_topic_fanout(t, cov_by_topic)
        if not fan:
            continue
        data.append(fan)
        labels_list.append(f"topic {t}\n({r['target']} regs)")
    if not data:
        return
    bp = ax.boxplot(data, showfliers=True, patch_artist=True)
    for box in bp["boxes"]:
        box.set(facecolor="#3066BE", alpha=0.6)
    ax.set_xticks(range(1, len(labels_list) + 1))
    ax.set_xticklabels(labels_list)
    ax.set_ylabel(f"hosts holding each registrant's ad (cap = {num_hosts - 1})")
    ax.set_title(f"{label}: fan-out per topic at post-register-wait snapshot")
    ax.grid(True, axis="y", alpha=0.3)


def plot_per_registrant_discovery_grid(per_topic, results, cov_by_topic, fig, label):
    """One subplot per topic: x = registrant rank, y = #searchers that found that registrant."""
    topics = sorted(per_topic, key=lambda r: -r["target"])
    n = len(topics)
    if n == 0:
        return
    axs = fig.subplots(nrows=n, ncols=1, sharex=False)
    if n == 1:
        axs = [axs]
    by_topic = collections.defaultdict(list)
    for r in results:
        by_topic[r["topic"]].append(r)
    for ax, rec in zip(axs, topics):
        t = rec["topic"]
        rs = by_topic[t]
        counts, _ = per_registrant_discovery(rs, cov_by_topic, t)
        xs = list(range(len(counts)))
        n_searchers = rec["numSearchers"]
        colors = ["#2A9D8F" if c >= n_searchers else "#E76F51" if c < n_searchers * 0.9 else "#F4A261"
                  for c in counts]
        ax.scatter(xs, counts, c=colors, s=14, alpha=0.85)
        ax.axhline(n_searchers, color="black", linestyle="--", linewidth=1, alpha=0.4,
                   label=f"all searchers ({n_searchers})")
        ax.set_xlim(-1, len(counts) + 1)
        ax.set_ylim(-1, n_searchers + 5)
        ax.set_ylabel(f"topic {t}\n# searchers\nthat found it", fontsize=9)
        ax.grid(True, alpha=0.3)
    axs[-1].set_xlabel("registrant (rank-sorted by found count)")
    fig.suptitle(f"{label}: per-registrant discovery — searchers that found each registrant", fontsize=12)


def plot_unique_recall_cdf(per_topic, results, cov_by_topic, ax, label):
    topics = sorted(per_topic, key=lambda r: -r["target"])
    by_topic = collections.defaultdict(list)
    for r in results:
        by_topic[r["topic"]].append(r)
    for rec in topics:
        t = rec["topic"]
        rs = by_topic[t]
        uniq, target = unique_recall_per_searcher(rs, cov_by_topic, t)
        if not uniq:
            continue
        # Normalise to recall fraction
        rec_frac = [u / target if target > 0 else 1.0 for u in uniq]
        rec_frac.sort()
        ys = np.arange(1, len(rec_frac) + 1) / len(rec_frac)
        ax.plot(rec_frac, ys, label=f"topic {t} ({target} regs)", linewidth=1.6)
    ax.set_xlabel("per-searcher unique recall fraction")
    ax.set_ylabel("CDF over searchers")
    ax.set_title(f"{label}: per-searcher unique recall CDF per topic")
    ax.legend(loc="lower right", fontsize=9)
    ax.grid(alpha=0.3)
    ax.set_xlim(0, 1.05)
    ax.set_ylim(0, 1.02)


def write_report(out_dir, label, params, per_topic, results, cov_by_topic, num_hosts):
    path = os.path.join(out_dir, "report.md")
    lines = []
    lines.append(f"# Simnet experiment report — `{label}`\n")

    lines.append("## Simulation parameters\n")
    lines.append("| parameter | value |")
    lines.append("|---|---|")
    for k, v in params.items():
        lines.append(f"| {k} | {v} |")
    lines.append("")

    # Aggregate
    n_searchers = sum(r["numSearchers"] for r in per_topic)
    full = sum(r["fullRecall"] for r in per_topic)
    timeouts = sum(r["hitTimeout"] for r in per_topic)
    lines.append("## Aggregate results\n")
    lines.append("| metric | value |")
    lines.append("|---|---|")
    lines.append(f"| searchers | {n_searchers} |")
    lines.append(f"| topics | {len(per_topic)} |")
    lines.append(f"| full-recall searchers (across topics) | {full} / {n_searchers} |")
    lines.append(f"| timeouts (Go select-race flag) | {timeouts} / {n_searchers} |")
    lines.append("")

    # Per-topic
    lines.append("## Per-topic results\n")
    lines.append("| topic | registrants | searchers | full recall | mean recall (raw, with dupes) | timeouts |")
    lines.append("|---:|---:|---:|---:|---:|---:|")
    for r in sorted(per_topic, key=lambda x: -x["target"]):
        lines.append(
            f"| {r['topic']} | {r['target']} | {r['numSearchers']} | "
            f"{r['fullRecall']}/{r['numSearchers']} | {r['meanRecall']:.4f} | {r['hitTimeout']} |"
        )
    lines.append("")
    lines.append("> *Mean recall reported here is the raw `foundRegistrant / target` ratio averaged over searchers. Because the iterator is infinite and emits duplicates within the timeout window, this number can exceed 1.0 (the same registrant can be counted multiple times across the iterator's sessions). For a true per-searcher recall metric, see the unique-recall CDF (figure 05).*\n")

    # Coverage
    lines.append("## Post-register-wait coverage\n")
    lines.append("| topic | registrants visible | fan-out min | med | max | cap |")
    lines.append("|---:|---:|---:|---:|---:|---:|")
    for r in sorted(per_topic, key=lambda x: -x["target"]):
        t = r["topic"]
        fan = per_topic_fanout(t, cov_by_topic)
        if not fan:
            lines.append(f"| {t} | 0 | — | — | — | {num_hosts - 1} |")
            continue
        lines.append(f"| {t} | {len(fan)} | {fan[0]} | {fan[len(fan)//2]} | {fan[-1]} | {num_hosts - 1} |")
    lines.append("")
    lines.append("> *Fan-out is the number of distinct hosts that hold each registrant's ad in their topic table at the moment the registration phase ends. Cap = `numHosts - 1` (self-exclusion).*\n")

    # Per-searcher unique recall
    lines.append("## Per-searcher unique recall (true recall)\n")
    lines.append("| topic | min | med | max | target | ≥ target |")
    lines.append("|---:|---:|---:|---:|---:|---:|")
    by_topic = collections.defaultdict(list)
    for r in results:
        by_topic[r["topic"]].append(r)
    for r in sorted(per_topic, key=lambda x: -x["target"]):
        t = r["topic"]
        uniq, target = unique_recall_per_searcher(by_topic[t], cov_by_topic, t)
        if not uniq:
            continue
        atTarget = sum(1 for u in uniq if u >= target)
        lines.append(
            f"| {t} | {uniq[0]} | {uniq[len(uniq)//2]} | {uniq[-1]} | {target} | {atTarget}/{len(uniq)} |"
        )
    lines.append("")
    lines.append("> *Unique recall is the count of distinct registrant IDs each searcher's iterator yielded (deduplicated). Target = number of other registrants of the topic (excluding the searcher itself).*\n")

    lines.append("## Figures\n")
    figs = [
        ("01_topic_distribution", "Registrant count per topic (Zipf draw)."),
        ("02_per_topic_recall", "Per-topic discovery success: per-search avg fraction of registrants found (unique) and fraction of complete searches (found all)."),
        ("03_time_to_first_cdf", "CDF of time-to-first result across all searchers."),
        ("04_time_to_completion_cdf", "Per-topic CDF of time-to-completion (note: iterator is infinite; completion ≈ search-timeout for all searches)."),
        ("05_id_space_registrants", "Strip plot of registrants placed in ID-space (top 64 bits, normalized 0..1), one row per topic."),
        ("06_id_space_found_vs_missed", "Per-topic per-registrant view (one subplot per topic): x = registrant ID position, y = number of searchers that found that registrant. Green = found by all; orange = some missed; red = many missed."),
        ("07_per_topic_fanout", "Per-topic fan-out distribution at the end of register-wait — hosts holding each registrant's ad."),
        ("08_per_registrant_discovery", "Same per-registrant data as 06, rank-sorted on the x-axis instead of by ID-space position."),
        ("09_unique_recall_distribution", "Per-topic CDF of per-searcher unique recall fraction (distinct registrants found / target)."),
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
    args = ap.parse_args()

    label = args.label or os.path.splitext(os.path.basename(args.metrics_json))[0]
    out = args.out_dir or f"./figures-{label}"
    os.makedirs(out, exist_ok=True)

    data = load(args.metrics_json)
    per_topic = data["perTopic"]
    results = data["results"]
    cov_by_topic = data.get("registrationCoverage", {}).get("byTopic", {})

    # Derive number of hosts from the largest 'cap' visible in coverage data.
    # Each topic's fan-out cap = numHosts - 1.
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

    # 02 per-topic recall (mean unique + full)
    fig, ax = plt.subplots(figsize=(9, 4.5))
    plot_per_topic_recall(per_topic, results, cov_by_topic, ax, label)
    fig.savefig(os.path.join(out, "02_per_topic_recall.png"))
    fig.savefig(os.path.join(out, "02_per_topic_recall.pdf"))
    plt.close(fig)

    # 03 time-to-first CDF
    fig, ax = plt.subplots(figsize=(7, 4))
    plot_time_to_first_cdf(results, ax, label)
    fig.savefig(os.path.join(out, "03_time_to_first_cdf.png"))
    fig.savefig(os.path.join(out, "03_time_to_first_cdf.pdf"))
    plt.close(fig)

    # 04 time-to-completion CDF per topic
    fig, ax = plt.subplots(figsize=(8, 5))
    plot_time_to_completion_cdf(per_topic, results, ax, label)
    fig.savefig(os.path.join(out, "04_time_to_completion_cdf.png"))
    fig.savefig(os.path.join(out, "04_time_to_completion_cdf.pdf"))
    plt.close(fig)

    # 05 ID-space registrant distribution per topic
    fig, ax = plt.subplots(figsize=(10, 0.5 * len(per_topic) + 1.5))
    plot_id_space_registrants(per_topic, cov_by_topic, ax, label)
    fig.savefig(os.path.join(out, "05_id_space_registrants.png"))
    fig.savefig(os.path.join(out, "05_id_space_registrants.pdf"))
    plt.close(fig)

    # 06 ID-space found-vs-missed grid (one row per topic, x = ID position)
    fig = plt.figure(figsize=(10, 1.7 * len(per_topic) + 1.5))
    plot_id_space_found_vs_missed(per_topic, results, cov_by_topic, fig, label)
    fig.tight_layout(rect=[0, 0, 1, 0.96])
    fig.savefig(os.path.join(out, "06_id_space_found_vs_missed.png"))
    fig.savefig(os.path.join(out, "06_id_space_found_vs_missed.pdf"))
    plt.close(fig)

    # 07 per-topic fan-out box plot
    fig, ax = plt.subplots(figsize=(9, 4.5))
    plot_per_topic_fanout(cov_by_topic, per_topic, ax, label, num_hosts)
    fig.savefig(os.path.join(out, "07_per_topic_fanout.png"))
    fig.savefig(os.path.join(out, "07_per_topic_fanout.pdf"))
    plt.close(fig)

    # 08 per-registrant discovery grid (rank-sorted)
    fig = plt.figure(figsize=(10, 1.6 * len(per_topic) + 1.5))
    plot_per_registrant_discovery_grid(per_topic, results, cov_by_topic, fig, label)
    fig.tight_layout(rect=[0, 0, 1, 0.96])
    fig.savefig(os.path.join(out, "08_per_registrant_discovery.png"))
    fig.savefig(os.path.join(out, "08_per_registrant_discovery.pdf"))
    plt.close(fig)

    # 09 unique recall CDF
    fig, ax = plt.subplots(figsize=(8, 5))
    plot_unique_recall_cdf(per_topic, results, cov_by_topic, ax, label)
    fig.savefig(os.path.join(out, "09_unique_recall_distribution.png"))
    fig.savefig(os.path.join(out, "09_unique_recall_distribution.pdf"))
    plt.close(fig)

    # Markdown report
    write_report(out, label, params, per_topic, results, cov_by_topic, num_hosts)

    # Summary
    n_searchers = sum(r["numSearchers"] for r in per_topic)
    full = sum(r["fullRecall"] for r in per_topic)
    print(f"[{label}] {n_searchers} searchers, {full}/{n_searchers} full recall across {len(per_topic)} topics")
    print(f"figures + report in: {out}")


if __name__ == "__main__":
    main()
