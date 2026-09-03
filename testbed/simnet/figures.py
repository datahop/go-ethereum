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
from matplotlib.lines import Line2D
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


def emit(fig, out, stem, ok=True):
    """Save a figure, or discard it when the plotter had nothing to draw.

    A figure written with empty axes reads as a real result that happens to be
    flat. Several measurements here are optional or only present in newer runs,
    so a missing one must show up as a missing figure.
    """
    if ok:
        fig.savefig(os.path.join(out, stem + ".png"))
        fig.savefig(os.path.join(out, stem + ".pdf"))
    plt.close(fig)


def load(path):
    with open(path) as f:
        return json.load(f)


# Helper: searchers' foundIds use TerminalString (16-hex prefix); coverage byRegistrant
# uses full String() (64-hex). Truncate both to the first 16 chars to match.
def short_id(id_str):
    return id_str[:16]


# The multi-topic search path records only the deduplicated registrant set
# (foundRegistrantIds); foundIds is populated by the single-topic path. Every
# consumer here intersects with the registrant set anyway, so prefer the former
# and fall back to the latter.
def found_reg_ids(r):
    return set(r.get("foundRegistrantIds") or r.get("foundIds") or [])


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
        for fid in found_reg_ids(r):
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
        uniq = len(found_reg_ids(r) & fanout_set)
        out.append(uniq)
    out.sort()
    return out, target


# ────────────────────────────────────────────────────────────────────────────
# Figure plotters
# ────────────────────────────────────────────────────────────────────────────


def topic_index_map(metrics):
    """Map a topic's hex ID to its topic index.

    Topic IDs are spread across the keyspace and carry no index, so the run
    emits the mapping. Metrics files written before that existed are recovered
    by matching each timing block's registrants against the per-topic
    registrant sets in registrationCoverage -- the legacy trick of reading an
    index out of the hex silently produced garbage for every topic.
    """
    ids = metrics.get("topicIds")
    if ids:
        return {k: int(v) for k, v in ids.items()}
    cov = (metrics.get("registrationCoverage") or {}).get("byTopic") or {}
    members = {int(t): set(c.get("byRegistrant", {})) for t, c in cov.items()}
    out = {}
    for topic_hex, regs in (metrics.get("registrationTimingNs") or {}).items():
        sample = set(list(regs)[:25])
        if not sample:
            continue
        best, score = None, 0
        for t, ms in members.items():
            hit = len(sample & ms)
            if hit > score:
                best, score = t, hit
        if best is not None:
            out[topic_hex] = best
    return out


def topic_positions(metrics):
    """Topic index -> the topic ID's own position in the normalised ID space.

    Every ID-space figure plots node IDs on 0..1; without the topic's own
    position on the same axis there is no way to see whether an effect is
    centred on the topic or spread across the keyspace.
    """
    return {idx: int(hex_id[:16], 16) / float(2 ** 64)
            for hex_id, idx in topic_index_map(metrics).items()}


def mark_topic(ax, pos):
    """Draw the topic ID's position as a reference line on an ID-space axis."""
    if pos is None:
        return
    ax.axvline(pos, color="#7B1FA2", ls=":", linewidth=1.8, zorder=5,
               label=f"topic ID position ({pos:.3f})")


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


def plot_recall_reached(per_topic, results, fig, label):
    """02: does discovery actually reach every registrant?

    Left: distinct registrants found over time as a fraction of the topic's
    registrant population (median across searchers, inter-quartile band), with
    100% marked. Right: CDF over searchers of the fraction each one ended up
    reaching, so the spread behind the median is visible and the share of
    searchers achieving full recall can be read directly off the axis.
    """
    by_topic = {}
    for r in results:
        ts = r.get("uniqueFoundAtMs") or []
        tgt = r.get("target", 0)
        if tgt <= 0 or not ts:
            continue
        by_topic.setdefault(r["topic"], []).append((np.asarray(ts, dtype=float), tgt))
    if not by_topic:
        return False

    axl, axr = fig.subplots(1, 2)
    topics = sorted(by_topic)
    horizon = max(float(ts[-1]) for v in by_topic.values() for ts, _ in v) / 1000.0
    grid = np.linspace(0, horizon, 220)

    for t in topics:
        colour = plt.cm.tab10(t % 10)
        curves = np.empty((len(by_topic[t]), grid.size))
        finals = np.empty(len(by_topic[t]))
        for i, (ts, tgt) in enumerate(by_topic[t]):
            curves[i] = np.searchsorted(ts, grid * 1000.0, side="right") / tgt
            finals[i] = len(ts) / tgt
        med = np.median(curves, axis=0)
        axl.plot(grid, med, color=colour, linewidth=1.8, label=f"topic {t} (n={len(finals)})")
        axl.fill_between(grid, np.percentile(curves, 25, axis=0),
                         np.percentile(curves, 75, axis=0), color=colour, alpha=0.15, linewidth=0)
        f = np.sort(finals)
        axr.plot(f, np.arange(1, f.size + 1) / f.size, color=colour, linewidth=1.8,
                 label=f"topic {t}: median {np.median(f):.0%}")

    for ax in (axl, axr):
        ax.grid(alpha=0.3)
        ax.set_ylim(0, 1.02)
    axl.axhline(1.0, ls="--", color="#666", linewidth=1)
    axl.set_xlabel("search time (s)")
    axl.set_ylabel("registrants found / registrants in topic")
    axl.set_title("Distinct peers found over time (median, IQR)", fontsize=10)
    axl.legend(fontsize=8, loc="lower right")
    axr.axvline(1.0, ls="--", color="#666", linewidth=1)
    axr.set_xlim(0, 1.02)
    axr.set_xlabel("fraction of registrants found by end of search")
    axr.set_ylabel("CDF over searchers")
    axr.set_title("Where each searcher finished", fontsize=10)
    axr.legend(fontsize=8, loc="upper left")
    fig.suptitle(f"{label}: discovery completeness (1.0 = every registrant in the topic)")
    return True


def plot_time_to_first_cdf(results, ax, label):
    """02b: CDF of time-to-first result, one line per topic.

    Split by topic because a searcher's first result depends on how densely
    its topic is registered: a crowded topic answers sooner than a sparse one,
    and a single pooled curve hides that.
    """
    by_topic = {}
    for r in results:
        if r.get("timeToFirstNs", 0) > 0:
            by_topic.setdefault(r["topic"], []).append(r["timeToFirstNs"] / 1e9)
    if not by_topic:
        return False
    for t in sorted(by_topic):
        xs = np.sort(by_topic[t])
        ys = np.arange(1, xs.size + 1) / xs.size
        ax.plot(xs, ys, linewidth=1.8, color=plt.cm.tab10(t % 10),
                label=f"topic {t} (n={xs.size}, median {np.median(xs):.2f}s)")
    ax.set_xlabel("time to first result (s)")
    ax.set_ylabel("CDF over searchers")
    ax.set_title(f"{label}: time to first result, by topic")
    ax.grid(alpha=0.3)
    ax.set_ylim(0, 1.0)
    ax.set_xlim(left=0)
    ax.legend(fontsize=8, loc="lower right")
    return True


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


def plot_id_space_registrants(per_topic, cov_by_topic, fig, label, tpos=None):
    """04: admitted registrants across the ID space, by how widely they placed.

    One panel per topic. Each dot is a registrant at its ID position; height,
    size and colour all encode how many distinct registrars accepted its ad,
    so a node that only got one registrar is visibly different from one that
    reached thirty. The dotted line is the topic ID's own position.
    """
    topics = sorted(per_topic, key=lambda r: r["topic"])
    if not topics:
        return False
    axes = fig.subplots(len(topics), 1, sharex=True, squeeze=False)[:, 0]
    tpos = tpos or {}
    any_row = False
    for ax, r in zip(axes, topics):
        t = r["topic"]
        fan = cov_by_topic.get(str(t), {}).get("byRegistrant", {})
        if not fan:
            ax.set_ylabel(f"topic {t}\n(no data)", fontsize=8)
            continue
        any_row = True
        xs = [int(k[:16], 16) / float(2 ** 64) for k in fan]
        ys = list(fan.values())
        sizes = [6 + 2.2 * v for v in ys]
        sc = ax.scatter(xs, ys, s=sizes, c=ys, cmap="viridis", alpha=0.75, linewidths=0)
        fig.colorbar(sc, ax=ax, pad=0.01, label="registrars")
        mark_topic(ax, tpos.get(t))
        ax.set_ylabel(f"topic {t}\nregistrars holding", fontsize=8)
        ax.set_ylim(bottom=0)
        ax.grid(True, alpha=0.3)
        ax.legend(fontsize=7, loc="upper right")
    axes[-1].set_xlim(0, 1.0)
    axes[-1].set_xlabel("registrant ID position (top 64 bits, normalised 0..1)")
    axes[0].set_title(f"{label}: how widely each registrant placed its ad, across the ID space")
    return any_row


def plot_id_space_registrars(per_topic, cov_by_topic, fig, label, tpos=None):
    """04b: registrars across the ID space, by how many ads they hold.

    The mirror of figure 04: each dot is a host that accepted at least one ad
    for the topic, placed at its own ID position, with height and colour showing
    how many of that topic's ads it stores. Marker size is fixed -- ad load spans
    two orders of magnitude, and scaling the markers by it produces blobs that
    hide both the baseline population and the topic line.
    """
    topics = sorted(per_topic, key=lambda r: r["topic"])
    if not topics:
        return False
    axes = fig.subplots(len(topics), 1, sharex=True, squeeze=False)[:, 0]
    tpos = tpos or {}
    any_row = False
    for ax, r in zip(axes, topics):
        t = r["topic"]
        load = cov_by_topic.get(str(t), {}).get("byHost", {})
        if not load:
            ax.set_ylabel(f"topic {t}\n(no data)", fontsize=8)
            continue
        any_row = True
        xs = [int(k[:16], 16) / float(2 ** 64) for k in load]
        ys = list(load.values())
        sc = ax.scatter(xs, ys, s=9, c=ys, cmap="plasma", alpha=0.7, linewidths=0)
        fig.colorbar(sc, ax=ax, pad=0.01, label="ads held")
        mark_topic(ax, tpos.get(t))
        ax.set_ylabel(f"topic {t}\nads held", fontsize=8)
        ax.set_ylim(bottom=0)
        ax.grid(True, alpha=0.3)
        ax.legend(fontsize=7, loc="upper right")
    axes[-1].set_xlim(0, 1.0)
    axes[-1].set_xlabel("registrar ID position (top 64 bits, normalised 0..1)")
    axes[0].set_title(f"{label}: how many ads each registrar holds, across the ID space")
    return any_row


# Discovery-coverage bands for figure 05: an ordered green-to-yellow ramp for
# registrants most searchers reached, and a deliberately off-ramp magenta for
# the under-80% tail so a coverage problem is impossible to mistake for the
# merely-imperfect end of the scale. (label, colour, lower bound as a fraction
# of the searcher count); evaluated top-down.
COVERAGE_BANDS = [
    ("found by every searcher", "#14532D", 1.0),
    ("95-99%", "#3F8A4F", 0.95),
    ("90-94%", "#7FB069", 0.90),
    ("85-89%", "#C3D17B", 0.85),
    ("80-84%", "#EFD469", 0.80),
    ("under 80%", "#C2185B", 0.0),
]


def coverage_color(found, n_searchers):
    """Colour for a registrant found by `found` of `n_searchers` searchers."""
    if n_searchers <= 0:
        return COVERAGE_BANDS[-1][1]
    frac = found / n_searchers
    for _, colour, lo in COVERAGE_BANDS:
        if frac >= lo and (lo < 1.0 or found >= n_searchers):
            return colour
    return COVERAGE_BANDS[-1][1]


def plot_id_space_found_vs_missed(per_topic, results, cov_by_topic, fig, label, tpos=None):
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
            for fid in found_reg_ids(r):
                found_count[fid] += 1
        n_searchers = rec["numSearchers"]
        sorted_regs = sorted(fanout.keys(), key=lambda i: int(i[:16], 16))
        xs = [int(i[:16], 16) / float(2**64) for i in sorted_regs]
        ys = [found_count.get(i, 0) for i in sorted_regs]
        colors = [coverage_color(y, n_searchers) for y in ys]
        # Never-found registrants are the real failures, not just the low end
        # of the ramp, so they get their own marker rather than a colour.
        miss = [i for i, y in enumerate(ys) if y == 0]
        keep = [i for i, y in enumerate(ys) if y > 0]
        ax.scatter([xs[i] for i in keep], [ys[i] for i in keep],
                   c=[colors[i] for i in keep], s=18, alpha=0.85)
        if miss:
            ax.scatter([xs[i] for i in miss], [0] * len(miss), marker="x", s=34,
                       c=COVERAGE_BANDS[-1][1], linewidths=1.4)
        ax.axhline(n_searchers, color="black", linestyle="--", linewidth=1, alpha=0.4)
        if ax is axs[0]:
            # The dot colour bands how completely a registrant was discovered;
            # without a key the three colours are unreadable.
            handles = [Line2D([], [], marker="o", ls="", color=c, label=lab)
                       for lab, c, _ in COVERAGE_BANDS]
            handles.append(Line2D([], [], marker="x", ls="", color=COVERAGE_BANDS[-1][1],
                                  label="never found"))
            handles.append(Line2D([], [], ls="--", color="black", alpha=0.4,
                                  label="all searchers (max possible)"))
            ax.legend(handles=handles, fontsize=7.5, loc="lower right", ncol=3, framealpha=0.92)
        mark_topic(ax, (tpos or {}).get(t))
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


def plot_placement_time_idspace(per_topic, reg_timing, ax_data, fig, label, tpos=None,
                                start_ns=None, placements=None, mode="min"):
    """07 / 07b: how long placing an ad took, per node, across the ID space.

    mode="min"  -- time until the ad was admitted *anywhere*: how long before a
                   registrant is findable at all.
    mode="mean" -- mean time across every registrar that accepted the ad: how
                   long the full placement took, which is the cost a registrant
                   actually pays to spread itself.

    Both are measured from the node's own registration start where that was
    recorded, so the harness's staggered start does not leak into the numbers.
    """
    topics = sorted(per_topic, key=lambda r: r["topic"])
    if not topics:
        return False
    by_hex = ax_data or {}
    sn = start_ns or {}
    axes = fig.subplots(len(topics), 1, sharex=True, squeeze=False)[:, 0]
    any_data = False
    for ax, r in zip(axes, topics):
        t = r["topic"]
        hex_id = next((h for h, i in by_hex.items() if i == t and h in reg_timing), None)
        xs, ys = [], []
        if hex_id:
            place = (placements or {}).get(hex_id, {})
            for rid, first in reg_timing[hex_id].items():
                began = sn.get(rid, 0)
                if mode == "mean":
                    a = place.get(rid)
                    if not a or not a.get("count"):
                        continue
                    val = (a["sumNs"] / a["count"] - began) / 1e9
                else:
                    val = (first - began) / 1e9
                xs.append(int(rid[:16], 16) / float(2 ** 64))
                ys.append(max(val, 0.0))
        if xs:
            any_data = True
            ax.scatter(xs, ys, s=9, c=ys, cmap="viridis", alpha=0.7, linewidths=0)
            ax.axhline(float(np.median(ys)), color="#444", ls="--", linewidth=1,
                       label=f"median {np.median(ys):.1f}s")
        mark_topic(ax, (tpos or {}).get(t))
        ax.set_ylabel(f"topic {t}\ntime (s)", fontsize=8)
        ax.set_ylim(bottom=0)
        ax.grid(alpha=0.3)
        ax.legend(fontsize=7, loc="upper right")
    axes[-1].set_xlim(0, 1)
    axes[-1].set_xlabel("registrant ID position (top 64 bits, normalised 0..1)")
    axes[0].set_title(
        f"{label}: time to place an ad anywhere, per registrant" if mode == "min"
        else f"{label}: mean time to place an ad across all its registrars")
    return any_data


def plot_registration_latency_bar(per_topic, reg_timing, ax, label, topic_ids=None,
                                  start_ns=None):
    """07: mean ± std time-to-first-remote-admission per topic. Bars clipped
    at zero so the lower error bar never crosses zero."""
    if not reg_timing:
        return False
    # Sort by topic index (ascending) to match other per-topic figures.
    topics = sorted(per_topic, key=lambda r: r["topic"])
    # Topic IDs carry no index, so the caller supplies the resolved mapping.
    by_hex = topic_ids or {}

    def topic_to_hex(t):
        for hex_id, idx in by_hex.items():
            if idx == t and hex_id in reg_timing:
                return hex_id
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
        # Measure from each registrant's own registration start: nodes are
        # staggered across minutes at 10k, so timing them from the start of the
        # sweep reports the schedule, not the protocol.
        sn = start_ns or {}
        vals_ms = []
        for rid, admitted in reg_timing[hex_id].items():
            began = sn.get(rid, 0)
            vals_ms.append(max(admitted - began, 0) / 1e6)
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
    ax.set_ylabel("time from own registration start\nto first remote admission (ms) — mean ± 1σ")
    ax.set_title(f"{label}: registration latency per topic")
    ax.grid(True, axis="y", alpha=0.3)
    ax.set_ylim(bottom=0)
    for i, (m, s, c) in enumerate(zip(means_ms, stds_ms, counts)):
        ax.text(i, m + s + max(means_ms) * 0.04, f"{m:.0f}±{s:.0f}\nn={c}",
                ha="center", fontsize=9)
    return True


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

    # 02 discovery completeness: distinct peers over time + where searchers finish
    fig = plt.figure(figsize=(13, 4.6), constrained_layout=True)
    ok = plot_recall_reached(per_topic, results, fig, label)
    emit(fig, out, "02_recall_reached", ok)

    # 02b time-to-first CDF
    fig, ax = plt.subplots(figsize=(7.5, 4.4), constrained_layout=True)
    ok = plot_time_to_first_cdf(results, ax, label)
    emit(fig, out, "02b_time_to_first_cdf", ok)

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

    tpos = topic_positions(data)

    # 04 ID-space registrants, by how widely each placed its ad
    fig = plt.figure(figsize=(10, 2.0 * len(per_topic) + 1.5), constrained_layout=True)
    ok = plot_id_space_registrants(per_topic, cov_by_topic, fig, label, tpos)
    emit(fig, out, "04_id_space_registrants", ok)

    # 04b ID-space registrars, by how many ads each holds
    fig = plt.figure(figsize=(10, 2.0 * len(per_topic) + 1.5), constrained_layout=True)
    ok = plot_id_space_registrars(per_topic, cov_by_topic, fig, label, tpos)
    emit(fig, out, "04b_id_space_registrars", ok)

    # 07 / 07b placement time across the ID space
    tmap = topic_index_map(data)
    for mode, stem in (("min", "07_placement_time_idspace"),
                       ("mean", "07b_placement_mean_idspace")):
        fig = plt.figure(figsize=(10, 2.0 * len(per_topic) + 1.5), constrained_layout=True)
        ok = plot_placement_time_idspace(per_topic, reg_timing, tmap, fig, label, tpos,
                                         data.get("registrationStartNs"),
                                         data.get("registrationPlacements"), mode)
        if ok:
            fig.savefig(os.path.join(out, stem + ".png"))
            fig.savefig(os.path.join(out, stem + ".pdf"))
        plt.close(fig)

    # 05 ID-space found-vs-missed grid
    fig = plt.figure(figsize=(10, 1.7 * len(per_topic) + 1.5))
    plot_id_space_found_vs_missed(per_topic, results, cov_by_topic, fig, label, tpos)
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
        ok = plot_registration_latency_bar(per_topic, reg_timing, ax, label,
                                           topic_index_map(data),
                                           data.get("registrationStartNs"))
        emit(fig, out, "07_registration_latency_bar", ok)
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
