# Topic discovery evaluation — 5 topics, Zipf (10k nodes)

**Scenario:** 10,000 nodes across 5 topics; each node draws one topic via a Zipf
distribution (`s = 1.07`) and both *registers* and *searches* it (`-topics 5`).
Covers registration performance, search performance (recall and found-vs-missed
across the ID space), and per-node overhead across the ID space.

Integration build = `topdisc` + PRs #71 (eviction) + #81 (waiting-time lower bound
+ G floor) + #83 (`collectOnePerDist` topic-dist fix) + #84 (iptree depth
normalization), plus the simnet measurement hooks.

> **Status:** overhead + per-topic performance tables below are final. The
> per-topic registration/search **figures** (via `figures.py`) are being generated
> from a longer metrics-out re-run and will be added here.

## Setup

```
nodes=10000  topics=5  zipf-s=1.07  reg-bucket-size=3  search-bucket-size=20
ad-lifetime=15m  reg-attempt-timeout=22.5m  max-nodes-per-source-per-bucket=1
bootstrap-wait=3m  register-wait=5m  search-timeout=2h
register-stagger=30ms  search-stagger=10ms  search-pause-max=400ms  refresh-interval=10m
latency=30ms  bw=100Mibps  router-shards=96  link-no-aqm  max-bootnodes=5  seed=42
```

Zipf membership (registrants per topic): t0 = 4544, t1 = 2117, t2 = 1424,
t3 = 1057, t4 = 858.

## Registration performance

Admission latency is essentially topic-independent (~303–306 s) — governed by the
shared reg-bucket dynamics, not per-topic membership.

| topic | registrants | mean time to first remote admission |
|---:|---:|---:|
| 0 | 4544 | 306.5 s |
| 1 | 2117 | 304.3 s |
| 2 | 1424 | 303.2 s |
| 3 | 1057 | 302.8 s |
| 4 | 858 | 303.3 s |

*(Fan-out / per-host-load / ID-space registration figures pending the metrics-out
re-run.)*

## Search performance

Recall rises as topic membership shrinks: smaller topics are fully covered by most
searchers (topic 4: 383 of 858 searchers saw everyone), the largest plateaus near
90%. Collective coverage is **100% on every topic**.

| topic | registrants | coverage | mean recall (`meanUniq`) | full recall | mean dup |
|---:|---:|---:|---:|---:|---:|
| 0 | 4544 | 100% | 0.8965 | 0 / 4544 | 3.52 |
| 1 | 2117 | 100% | 0.9789 | 0 / 2117 | 7.54 |
| 2 | 1424 | 100% | 0.9953 | 50 / 1424 | 11.10 |
| 3 | 1057 | 100% | 0.9975 | 169 / 1057 | 14.70 |
| 4 | 858 | 100% | 0.9981 | 383 / 858 | 17.49 |

*(Recall-over-time / found-vs-missed-across-ID-space figures pending the
metrics-out re-run.)*

## Overhead across the ID space

Per-node overhead binned by `logdist(own-topic, node)` — each node mapped to its
own topic via the registrant manifest (all 10,000 mapped).

| logdist | nNodes | tx MB | rx MB | TQ rcv |
|---:|---:|---:|---:|---:|
| 256 (far) | 4990 | 10.3 | 10.0 | 2,929 |
| 252 | 284 | 11.3 | 10.1 | 3,225 |
| 250 | 100 | 25.6 | 11.2 | 8,031 |
| 249 | 40 | 53.3 | 15.1 | 17,151 |
| 247 | 13 | 71.5 | 18.5 | 20,412 |
| 242 (closest) | 1 | 94.3 | 22.2 | 21,621 |
| **ALL** | **10000** | **10.1** | **10.0** | **2,867** |

![overhead vs ID-space distance](figures-5top/overhead_compare.png)

**Multi-topic spreads the load.** A node far from its own topic is still close to
some *other* topic, so the periphery is not idle: far nodes send ~10 MB (vs ~3.7 MB
in the single-topic run) and the near-topic peak is lower (~94 MB vs ~170 MB). The
far/near byte skew flattens from ~45× (single topic) to **~9×**, at a ~35% higher
average per-node load.

## Comparison with the single-topic run

See [`report-single-topic-10k.md`](report-single-topic-10k.md).

| | far node (ld 256) tx | closest node tx | mean tx / node | skew |
|---|---:|---:|---:|---:|
| single topic | 3.7 MB | ~170 MB | 7.6 MB | ~45× |
| 5 topics | 10.3 MB | ~94 MB | 10.1 MB | ~9× |

Run dirs on London: `~/eval-overhead-5top-10k-20260713-210832/` (overhead + tables);
metrics-out re-run in progress for the figures.
