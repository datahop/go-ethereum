#!/usr/bin/env python3
"""Traffic breakdown plots vs ID-space distance to topic.
Usage: plot_traffic.py <traffic.csv> <label> <out-prefix> [DUR_seconds]
Rates are per-node KB/s (cumulative bytes / DUR).
"""
import sys, csv
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

path, label, prefix = sys.argv[1], sys.argv[2], sys.argv[3]
DUR = float(sys.argv[4]) if len(sys.argv) > 4 else 4080.0

rows = list(csv.DictReader(open(path)))
xs = [int(r["logdist"]) for r in rows]

def col(r, name):  # KB/s
    return float(r.get(name, 0) or 0) / 1024.0 / DUR

def series(name):
    return [col(r, name) for r in rows]

def agg(r, parts, dirn):  # sum of parts, in KB/s
    return sum(col(r, p + dirn) for p in parts)

REG = ["regtopic", "regconfirm", "regaux"]
SEA = ["topicquery", "topicnodes", "searchaux"]

def base(ax):
    ax.set_xlabel("logdist to topic  (lower = closer to topic →)")
    ax.set_ylabel("KB per node per second")
    ax.grid(True, alpha=0.3); ax.invert_xaxis()

# Fig 1+2: all / reg / search, received and sent
for dirn, tot, tag in (("_inB", "rxB", "received"), ("_outB", "txB", "sent")):
    fig, ax = plt.subplots(figsize=(7.5, 4.4))
    ax.plot(xs, series(tot), "o-", color="#333333", markersize=4, label="all")
    ax.plot(xs, [agg(r, REG, dirn) for r in rows], "s-", color="#E76F51", markersize=4, label="registration")
    ax.plot(xs, [agg(r, SEA, dirn) for r in rows], "^-", color="#2A9D8F", markersize=4, label="search")
    ax.set_title(f"{label}: bytes {tag} per node vs ID-space distance (reg / search / all)")
    base(ax); ax.legend()
    fig.tight_layout(); fig.savefig(f"{prefix}_{tag}.png", dpi=130); plt.close(fig)

# Fig 3+4: per-message-type within reg and within search (received + sent)
def type_fig(parts, name, colors):
    fig, axes = plt.subplots(1, 2, figsize=(13, 4.4))
    for ax, dirn, tag in ((axes[0], "_inB", "received"), (axes[1], "_outB", "sent")):
        for p, c in zip(parts, colors):
            ax.plot(xs, series(p + dirn), "o-", color=c, markersize=3, label=p)
        ax.set_title(f"{name} {tag}")
        base(ax); ax.legend(fontsize=9)
    fig.suptitle(f"{label}: {name} traffic by message type vs ID-space distance", fontweight="bold")
    fig.tight_layout(rect=[0, 0, 1, 0.95])
    fig.savefig(f"{prefix}_{name}_types.png", dpi=130); plt.close(fig)

type_fig(REG, "registration", ["#E76F51", "#F4A261", "#9B2226"])
type_fig(SEA, "search", ["#2A9D8F", "#264653", "#8AB17D"])
print("wrote", prefix + "_{received,sent,registration_types,search_types}.png")
