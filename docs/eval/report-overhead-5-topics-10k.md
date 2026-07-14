# 10k overhead evaluation — 5 topics (Zipf)

**Scenario:** 10,000 nodes across 5 topics, each node drawing one topic via a Zipf
distribution (`s = 1.07`) and both registering *and* searching it. Measures per-node
message/byte overhead as a function of position in the ID space (log-distance to the
node's *own* topic hash), alongside per-topic search and registration performance.

**Headline:** 100% coverage on every topic; per-topic mean recall 89.6% (largest
topic) up to 99.8% (smallest). Overhead is **more evenly spread than the
single-topic case**: because a node far from its own topic is still close to some
*other* topic, the periphery is not idle — the far/near byte skew flattens from
~45× (single topic) to **~9×**, at a ~35% higher average per-node load.

## Setup

Integration build = `topdisc` + PRs #71 (eviction) + #81 (waiting-time lower
bound + G floor) + #83 (`collectOnePerDist` topic-dist fix) + #84 (iptree depth
normalization), plus the simnet measurement hooks.

```
nodes=10000  topics=5  zipf-s=1.07  reg-bucket-size=3  search-bucket-size=20
ad-lifetime=15m  reg-attempt-timeout=22.5m  max-nodes-per-source-per-bucket=1
bootstrap-wait=3m  register-wait=5m  search-timeout=60m
register-stagger=30ms  search-stagger=10ms  search-pause-max=400ms  refresh-interval=10m
latency=30ms  bw=100Mibps  router-shards=96  link-no-aqm  max-bootnodes=5  seed=42
```

Zipf membership (registrants per topic): t0 = 4544, t1 = 2117, t2 = 1424,
t3 = 1057, t4 = 858.

## Search performance (per topic)

| topic | registrants | coverage | mean recall (`meanUniq`) | full recall | mean dup |
|---:|---:|---:|---:|---:|---:|
| 0 | 4544 | 100% | 0.8965 | 0 / 4544 | 3.52 |
| 1 | 2117 | 100% | 0.9789 | 0 / 2117 | 7.54 |
| 2 | 1424 | 100% | 0.9953 | 50 / 1424 | 11.10 |
| 3 | 1057 | 100% | 0.9975 | 169 / 1057 | 14.70 |
| 4 | 858 | 100% | 0.9981 | 383 / 858 | 17.49 |

Recall rises as topic membership shrinks: smaller topics are fully covered by most
searchers (topic 4: 383 of 858 searchers saw *everyone*), while the largest topic
plateaus near 90% within the 60 min window. Duplicate factor grows for small topics
(fewer distinct registrars, so the same ones are seen repeatedly).

Per-registrant find-count median (p50) / registrants: t0 4241/4543, t1 2106/2117,
t2 1422/1424, t3 1056/1057, t4 856/858 — i.e. ~93% to ~99.9%.

## Registration performance (per topic)

| topic | registrants | mean time to first remote admission |
|---:|---:|---:|
| 0 | 4544 | 306.5 s |
| 1 | 2117 | 304.3 s |
| 2 | 1424 | 303.2 s |
| 3 | 1057 | 302.8 s |
| 4 | 858 | 303.3 s |

Admission latency is essentially topic-independent (~303–306 s) — registration
timing is governed by the shared reg-bucket dynamics, not per-topic membership.

## Message / byte overhead in the ID space

Per-node overhead binned by `logdist(own-topic, node)` — each node mapped to *its
own* topic via the registrant manifest (all 10,000 mapped).

| logdist | nNodes | tx pkts | tx MB | rx pkts | rx MB | TQ rcv |
|---:|---:|---:|---:|---:|---:|---:|
| 256 | 4990 | 29,649 | 10.3 | 28,874 | 10.0 | 2,929 |
| 255 | 2555 | 25,522 | 8.6 | 27,907 | 9.9 | 2,411 |
| 254 | 1222 | 21,853 | 7.2 | 26,884 | 9.7 | 1,870 |
| 253 | 606 | 24,635 | 8.2 | 27,574 | 9.8 | 2,171 |
| 252 | 284 | 32,107 | 11.3 | 28,938 | 10.1 | 3,225 |
| 251 | 158 | 54,492 | 20.2 | 34,186 | 11.2 | 6,214 |
| 250 | 100 | 68,304 | 25.6 | 36,567 | 11.2 | 8,031 |
| 249 | 40 | 139,464 | 53.3 | 57,301 | 15.1 | 17,151 |
| 248 | 20 | 164,668 | 61.4 | 68,775 | 16.5 | 21,836 |
| 247 | 13 | 189,922 | 71.5 | 72,452 | 18.5 | 20,412 |
| 246 | 9 | 187,759 | 61.4 | 83,037 | 21.6 | 16,430 |
| 245 | 2 | 238,118 | 77.8 | 112,490 | 33.4 | 19,025 |
| 242 | 1 | 223,473 | 94.3 | 71,117 | 22.2 | 21,621 |
| **ALL** | **10000** | **29,308** | **10.1** | **28,788** | **10.0** | **2,867** |

Figure: [`overhead-10k-1top-vs-5top.png`](overhead-10k-1top-vs-5top.png) (right/orange
series is this 5-topic run).

Reading it:

- The **periphery is busy**: nodes far from their own topic still send ~10 MB
  (vs. ~3.7 MB in the single-topic run) because they are close to — and serve as
  registrars for — *other* topics. The whole keyspace is load-bearing.
- The **near-topic peak is lower**: ~94 MB at the closest node vs. ~170 MB
  single-topic, so no single node is as hot. Net effect: average per-node load is
  ~35% higher (10.1 vs 7.6 MB tx) but the skew flattens to ~9× (94 / 10).
- **TOPICQUERYs received keep rising toward the topic** (~22k at the closest),
  without the single-topic run's fall-off at the very closest bin.

## Provenance (how search learns registrars)

Final cumulative counts (DHT seed vs. referral):

| | DHT seed | referral | referral share |
|---|---:|---:|---:|
| nodes added | 7.81 M | 27.10 M | 78% |
| queries issued | 7.24 M | 18.98 M | 72% |
| ads discovered | 44.8 M | 118.9 M | 73% |

As in the single-topic run, discovery is referral-dominated.

## Comparison with the single-topic run

Both runs use identical parameters and node population; only the topic count differs.
See [`report-overhead-single-topic-10k.md`](report-overhead-single-topic-10k.md).

| | far node (ld 256) tx | closest node tx | mean tx / node | skew (close/far) |
|---|---:|---:|---:|---:|
| single topic | 3.7 MB | ~170 MB | 7.6 MB | ~45× |
| 5 topics | 10.3 MB | ~94 MB | 10.1 MB | ~9× |

**Multi-topic spreads the load.** Concentrating all demand on one topic overloads a
few close-by registrars and leaves the periphery idle; splitting demand across topics
raises the baseline (everyone is close to *some* topic) and lowers the peak.

## Notes

- Search-table bucket occupancy at cutoff runs deeper than the single-topic case
  (`[20 20 20 20 20 19.9 19.3 16.3 11.1 5.7 …]`) — with fewer registrars per topic,
  buckets closer to the topic fill.
- Rejections: `full` = 14.9 M, `onePerBucket` = 0, `ip` = 0.
- Run dir on London: `~/eval-overhead-5top-10k-20260713-210832/`
  (`overhead.json`, `snaps/registrants-t*.json`, `run.log`).
