#!/usr/bin/env python3
"""Traffic breakdown plots vs ID-space distance to topic — bytes AND packet
counts, with real protocol message names.
Usage: plot_traffic.py <traffic.csv> <label> <out-prefix> [DUR_seconds]
Rates are per-node per-second.
"""
import sys, csv
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

path, label, prefix = sys.argv[1], sys.argv[2], sys.argv[3]
DUR = float(sys.argv[4]) if len(sys.argv) > 4 else 4080.0

rows = list(csv.DictReader(open(path)))
xs = [int(r["logdist"]) for r in rows]

# internal key -> real protocol message name (topic messages only; the aux NODES
# are labelled by the response they ride on).
NAME = {
    "regtopic": "REGTOPIC", "regconfirm": "REGCONFIRMATION", "regaux": "NODES (REGTOPIC resp)",
    "topicquery": "TOPICQUERY", "topicnodes": "TOPICNODES", "searchaux": "NODES (TOPICQUERY resp)",
}
TYPES = list(NAME.keys())
OTHER = ["findnode", "nodes", "ping", "pong"]  # DHT + liveness (uniform background)
REG = ["regtopic", "regconfirm", "regaux"]
SEA = ["topicquery", "topicnodes", "searchaux"]

def val(r, key, unit):  # per-node per-second; unit "B"->KB/s, "P"->pkts/s
    v = float(r.get(key, 0) or 0) / DUR
    return v / 1024.0 if unit == "B" else v

def base(ax, ylabel):
    ax.set_xlabel("logdist to topic  (lower = closer →)")
    ax.set_ylabel(ylabel); ax.grid(True, alpha=0.3); ax.invert_xaxis()

# ── Aggregate: all / registration / search, bytes and packets ──
for unit, ylabel, tag in (("B", "KB per node / s", "bytes"), ("P", "packets per node / s", "packets")):
    fig, axes = plt.subplots(1, 2, figsize=(13, 4.6))
    for ax, d, dtag in ((axes[0], "_in" + unit, "received"), (axes[1], "_out" + unit, "sent")):
        tot = "rx" + unit if d.startswith("_in") else "tx" + unit
        ax.plot(xs, [val(r, tot, unit) for r in rows], "o-", color="#333", ms=4, label="all")
        ax.plot(xs, [sum(val(r, p + d, unit) for p in REG) for r in rows], "s-", color="#E76F51", ms=4, label="registration")
        ax.plot(xs, [sum(val(r, p + d, unit) for p in SEA) for r in rows], "^-", color="#2A9D8F", ms=4, label="search")
        ax.set_title(dtag); base(ax, ylabel); ax.legend()
    fig.suptitle(f"{label}: {tag} per node vs ID-space distance (all / registration / search)", fontweight="bold")
    fig.tight_layout(rect=[0, 0, 1, 0.95]); fig.savefig(f"{prefix}_agg_{tag}.png", dpi=130); plt.close(fig)

# ── Per-message-type (real names), bytes and packets ──
cmap = plt.cm.tab10
for unit, ylabel, tag in (("B", "KB per node / s", "bytes"), ("P", "packets per node / s", "packets")):
    fig, axes = plt.subplots(1, 2, figsize=(14, 4.8))
    for ax, d, dtag in ((axes[0], "_in" + unit, "received"), (axes[1], "_out" + unit, "sent")):
        for i, t in enumerate(TYPES):
            ax.plot(xs, [val(r, t + d, unit) for r in rows], "o-", color=cmap(i % 10), ms=3, label=NAME[t])
        ax.plot(xs, [sum(val(r, o + d, unit) for o in OTHER) for r in rows], "--", color="#999",
                ms=0, label="other (DHT/liveness)")
        ax.set_title(dtag); base(ax, ylabel); ax.legend(fontsize=8, ncol=2)
    fig.suptitle(f"{label}: {tag} per node by message type vs ID-space distance", fontweight="bold")
    fig.tight_layout(rect=[0, 0, 1, 0.94]); fig.savefig(f"{prefix}_types_{tag}.png", dpi=130); plt.close(fig)

print("wrote", prefix + "_{agg_bytes,agg_packets,types_bytes,types_packets}.png")
