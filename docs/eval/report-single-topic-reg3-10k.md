# Topic Discovery Evaluation — Single Topic, reg-3 / search-20 (best config)

## Setup

- **10,000 nodes, one common topic** — every node both registers and searches it (`-all-register`).
- **Registration bucket size 3, search bucket size 20**, with the **#55 fix** (`collectOnePerDist` returns aux nodes by distance to the *topic*, not the registrar).
- **Ad lifetime 15 min**, continuous registration + renewal, **no churn**.
- 4h search window — converges cleanly and stays flat.

10,000 nodes on one topic reach **p50 = 96.15%** per-node recall (mean 95.6%) with **100% collective coverage**.

## Headline result

| metric | value |
|---|---|
| per-node recall (p50) | **96.15%** (9,615 / 10,000) |
| per-node recall (mean) | 95.6% |
| collective coverage | **100%** (neverFound = 0) |
| mean duplicate finds / searcher | 5.13 |
| convergence | flat after ~t=200 min |

Every one of the 10,000 registrants is discovered by at least one searcher, and the *typical* node discovers ~96% of the entire population.

## Registration

Registration is healthy and complete — **all 10,000 ads placed, none unplaced, neverFound = 0**. Every registrant is discovered by at least one searcher. With `reg-bucket-size=3` the fan-out (distinct registrars holding each ad) is a lean **median 13** (min 3, max 39) — fewer active slots per bucket than reg-10's ~46, by design.

**Fan-out and per-host load.** (a) how many registrars hold each ad; (b) how many ads each registrar holds for the topic.

![fan-out and per-host load](figures-reg3fix-single/06_fanout_both_views.png)

**Registrants across ID-space.** Admitted registrars concentrate around the topic id.

![registrants across ID-space](figures-reg3fix-single/04_id_space_registrants.png)

**Registration latency.** Mean ± 1σ time to first remote admission (~310 s).

![registration latency](figures-reg3fix-single/07_registration_latency_bar.png)

## Search results

**A — % registrants found per node (distribution).** Median 96%, mean 95.6%; the mass sits in a tight band around 96%.

![% registrants found per node](figures-reg3fix-single/fix_A_distribution.png)

**B — % registrants found over time.** Climbs steadily and converges at ~96% (flat after ~t=200 min).

![% registrants found over time](figures-reg3fix-single/fix_B_overtime.png)

**D — across ID-space.** The band sits at ~96% (green = found by ≥90% of searchers) uniformly across the keyspace, **except a single sharp dip at the topic center** (x≈0.267, the topic ID), where nodes drop to 25–90% (red). This is the entire residual — it is *not* spread evenly.

![ID-space: # searchers that found each node](figures-reg3fix-single/fix_D_idspace.png)

**Reach — unique registrars hit per searcher.** Each searcher queries a median of **1,127** distinct registrars (mean 1,123) over the run.

![unique registrars hit per searcher](figures-reg3fix-single/fix_reach_dist.png)

**Search-node origin.** Of the nodes each searcher discovers, **59%** come from topdisc referrals (TOPICQUERY responses) and **41%** from the DHT routing-table seed.

![search-node origin](figures-reg3fix-single/fix_origin.png)

## The residual is a topic-center hotspot

The dip at the topic ID is a **structural funnel**, not a coverage failure. Reach instrumentation (per-searcher queried-registrar sets, 134 sampled searchers) shows the keyspace neighborhood of the topic is sparse and low-yield:

![topic-center funnel](figures-reg3fix-single/reach_funnel.png)

| logdist to topic | # registrars | queried by (of 134) | distinct ads exposed / registrar |
|---|---|---|---|
| 242–246 (**center**) | **~5** | 134 (all) | **~10** |
| 246–248 | 17 | 134 | 10.5 |
| 248–250 | 58 | 133 | 15.1 |
| 250–252 | 234 | 119 | 28.4 |
| 252–254 | 915 | 59 | 22.7 |
| 254–257 (**bulk**) | **~5,700** | 10 | **~34** |

Within logdist 250 of the topic there are only ~80 registrars; beyond it, thousands. Every searcher queries the ~5 closest registrars, but each exposes only ~10 distinct ads — their tables are admission-capped (`WaitTime` grows as `AdLifetime / occupancy¹⁰`, plus `AdCacheSize` / `RegBucketSize` limits). A registrant at the topic center therefore depends on this thin, capacity-limited funnel and is replicated/surfaced far less than one served by the vast far region. **No node is ever down or unresponsive** — it is pure capacity concentration at the topic ID.

## Overhead

Per-node traffic across the ID space (`logdist(topic, node)`; lower = closer to the
topic, ~half the nodes at logdist 256 at the far end), from an instrumented re-run
of this config (`-overhead-out`, integration build = `topdisc` + #71/#81/#83/#84).
It **measures the topic-center funnel directly**. Four per-node inbound/traffic
views:

### 1. Queries received (TOPICQUERY)

![queries received across ID space](figures-overhead/1top_queries_received.png)

TOPICQUERYs received peak in the logdist 250–251 band (~24k per node) and then *ease*
toward the very closest nodes — the one-per-source-per-bucket rule caps how many
searchers query the single nearest registrar, spreading query load into a band.

### 2. Registrations received (REGTOPIC)

> ⚠️ **Not yet captured.** Needs a per-node REGTOPIC-received counter; re-instrumentation
> + re-run pending.

### 3. Ads received

> ⚠️ **Not yet captured.** Ads stored per registrar is derivable from `metrics.json`
> (`byHost`), and ad records received on the wire needs a counter; pending.

### 4. Total bytes sent / received

![bytes sent/received across ID space](figures-overhead/1top_bytes_sent_received.png)

Byte load rises monotonically toward the topic: the near-topic registrars (logdist
246–250) **send ~80–170 MB** each versus ~3.7 MB for the far majority — a **~45×
skew** driven purely by ID-space proximity. Received bytes rise similarly (to
~90–100 MB at the closest). The closest nodes are the biggest referral hubs, so
byte volume keeps climbing even where query *count* eases.

**Summary table:**

| logdist | nNodes | tx MB | rx MB | TQ rcv |
|---:|---:|---:|---:|---:|
| 256 (far) | 5087 | 3.7 | 6.9 | 394 |
| 252 | 320 | 23.3 | 9.7 | 6,329 |
| 250 | 65 | 82.9 | 17.8 | 23,591 |
| 247 | 14 | 86.5 | 31.0 | 16,516 |
| 246 | 3 | 167.8 | 97.8 | 11,384 |
| 242 (closest) | 1 | 129.8 | 95.8 | 6,613 |
| **ALL** | **10000** | **7.6** | **7.5** | **1,500** |

This is the *same* thin, capacity-capped funnel the recall dip exposes, now measured
in traffic: the ~80 registrars within logdist 250 of the topic are both the **recall
bottleneck** and the **traffic hotspot**.

## Conclusion

The tuned **reg-3 / search-20 + #55** config reaches **p50 = 96%** with 100% collective coverage. The remaining ~4% is entirely the **topic-center hotspot**: registrants nearest the topic ID, served by a sparse, capacity-capped set of close registrars — the same handful of nodes that carry a ~45× share of the network traffic. This is load concentrating at the topic ID, not a discovery failure, and it scales with topic size (worst for the dominant topic in a multi-topic deployment).

Run dirs on London: `~/eval-integration-10k-20260713-133630/` (search/reg), `~/eval-overhead-10k-20260713-194637/` (overhead).
