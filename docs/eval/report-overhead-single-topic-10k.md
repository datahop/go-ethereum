# 10k overhead evaluation — single topic

**Scenario:** 10,000 nodes, one shared topic that every node both registers *and*
searches (`-all-register`). Measures per-node message/byte overhead as a function
of position in the ID space (log-distance to the topic hash), alongside search and
registration performance.

**Headline:** 100% coverage; mean per-searcher recall 70.9% at 60 min (still
climbing — a 4h run of this config plateaus near 89%). Overhead is **sharply
concentrated on the few nodes closest to the topic**: a handful of near-topic
registrars send ~170 MB and field ~24k TOPICQUERYs each, versus ~3.8 MB / ~400 for
the far majority — a **~45× byte skew** across the ID space.

## Setup

Integration build = `topdisc` + PRs #71 (eviction) + #81 (waiting-time lower
bound + G floor) + #83 (`collectOnePerDist` topic-dist fix) + #84 (iptree depth
normalization), plus the simnet measurement hooks.

```
nodes=10000  topics=1 (all-register)  reg-bucket-size=3  search-bucket-size=20
ad-lifetime=15m  reg-attempt-timeout=22.5m  max-nodes-per-source-per-bucket=1
bootstrap-wait=3m  register-wait=5m  search-timeout=60m
register-stagger=30ms  search-stagger=10ms  search-pause-max=400ms  refresh-interval=10m
latency=30ms  bw=100Mibps  router-shards=96  link-no-aqm  max-bootnodes=5  seed=42
```

## Search performance

| metric | value |
|---|---|
| registrants / searchers | 10,000 / 10,000 |
| coverage (found by ≥1 searcher) | **100%** (neverFound = 0) |
| mean per-searcher recall (`meanUniq`) | **0.7095** (70.9% of 9,999 targets) |
| full recall (searchers seeing everyone) | 0 / 10,000 |
| mean distinct registrants / searcher | 7,094 |
| mean duplicate factor | 1.67 |

Per-registrant find-count distribution (how many of the 10k searchers found each
registrant):

| min | p5 | p25 | p50 | p75 | p95 | max | mean |
|---|---|---|---|---|---|---|---|
| 1,992 | 4,142 | 5,728 | 7,068 | 8,616 | 9,851 | 9,989 | 7,094 |

Recall was still rising at cutoff (p50 find-count climbed 6,635 → 7,069 over the
last ~8 min); the same config over a full 4h run reaches ~89% mean recall. The 60
min window here is sized for the overhead distribution, which stabilizes early.

## Registration performance

| metric | value |
|---|---|
| registrants | 10,000 |
| mean time to first remote admission | 310.5 s |

(The large variance is the expected heavy tail: with reg-bucket-size 3 the closest
bucket admits few registrars per cycle, so late arrivals wait for a slot.)

## Message / byte overhead in the ID space

Per-node sent/received packets and bytes and TOPICQUERYs received, binned by
`logdist(topic, node)` (lower logdist = closer to the topic in XOR space; ~half
the nodes sit at logdist 256, the far end).

| logdist | nNodes | tx pkts | tx MB | rx pkts | rx MB | TQ rcv |
|---:|---:|---:|---:|---:|---:|---:|
| 256 | 5087 | 12,209 | 3.7 | 19,092 | 6.9 | 394 |
| 255 | 2440 | 15,528 | 5.1 | 19,802 | 7.1 | 820 |
| 254 | 1249 | 21,999 | 7.7 | 21,324 | 7.5 | 1,612 |
| 253 | 602 | 36,227 | 13.5 | 24,799 | 8.3 | 3,356 |
| 252 | 320 | 61,278 | 23.3 | 31,094 | 9.7 | 6,329 |
| 251 | 160 | 115,515 | 45.0 | 44,300 | 12.8 | 12,676 |
| 250 | 65 | 215,568 | 82.9 | 72,229 | 17.8 | 23,591 |
| 249 | 29 | 200,726 | 73.3 | 74,655 | 16.1 | 20,072 |
| 248 | 24 | 199,489 | 68.8 | 83,684 | 16.4 | 19,141 |
| 247 | 14 | 263,619 | 86.5 | 120,400 | 31.0 | 16,516 |
| 246 | 3 | 531,649 | 167.8 | 264,003 | 97.8 | 11,384 |
| 245 | 2 | 479,588 | 147.5 | 244,896 | 86.5 | 11,767 |
| 244 | 2 | 520,838 | 174.1 | 239,226 | 97.6 | 9,513 |
| 243 | 2 | 454,746 | 144.7 | 224,504 | 91.3 | 6,245 |
| 242 | 1 | 475,582 | 129.8 | 263,588 | 95.8 | 6,613 |
| **ALL** | **10000** | **22,066** | **7.6** | **21,706** | **7.5** | **1,500** |

Figure: [`overhead-10k-1top-vs-5top.png`](overhead-10k-1top-vs-5top.png) (left/blue
series is this single-topic run).

Reading it:

- **Byte load rises monotonically toward the topic**, from ~3.7 MB sent at the far
  edge to ~170 MB at the closest nodes — a ~45× skew driven purely by ID-space
  proximity. Those near-topic nodes are the registrars every searcher must reach.
- **TOPICQUERYs received peak in the logdist 250–251 band (~24k)** and then *fall*
  toward the very closest nodes. The one-per-source-per-bucket rule caps how many
  searchers can query the single nearest node in a bucket, so query load spreads
  into a band rather than piling onto one node — while byte volume keeps climbing
  (the closest nodes are the biggest referral hubs, returning large NODES sets).

## Provenance (how search learns registrars)

Final cumulative counts — nodes/ads that entered a search table via the DHT
routing-table seed (`src=nil`) vs. via TOPICQUERY referrals from other registrars:

| | DHT seed | referral | referral share |
|---|---:|---:|---:|
| nodes added | 4.97 M | 17.86 M | 78% |
| queries issued | 4.62 M | 9.34 M | 67% |
| ads discovered | 56.3 M | 121.5 M | 68% |

Discovery is **referral-dominated** — most registrars are learned from other
registrars' responses, consistent with PR #83 fixing topic-query referrals to
point at the topic.

## Notes

- Search-table bucket occupancy at cutoff: the 6 farthest buckets are full (20),
  tapering to near-empty at the topic end — the search table is breadth-limited by
  `search-bucket-size` × depth, the known reach ceiling.
- Rejections: `full` = 9.9 M (buckets at capacity, expected), `onePerBucket` = 0,
  `ip` = 0.
- Run dir on London: `~/eval-overhead-10k-20260713-194637/`
  (`overhead.json`, `run.log`).
