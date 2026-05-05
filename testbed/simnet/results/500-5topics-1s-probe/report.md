# Simnet experiment report — `500-5topics`

## Simulation parameters

| parameter | value |
|---|---|
| nodes | 500 |
| DISC-NG nodes | 500 (frac = 1) |
| topics | 5 |
| Zipf skew (s) | 1.07 |
| RNG seed | 1 |
| per-link latency | 30 ms |
| per-link bandwidth | 100 Mibps (each direction) |
| max bootnodes per node | 20 |
| bootstrap wait | 60 s |
| register wait | 90 s |
| per-search timeout | 90 s |
| registration probe period | 1 s |

## Workload

Each DISC-NG node is assigned exactly one topic via a single Zipf draw, and **both registers and searches the same topic**. Every node makes one search; a search target is the set of *other* nodes that registered the same topic (self-exclusion). Vanilla discv5 nodes participate in routing-table maintenance (PING / FINDNODE) but neither register nor search.

Recall metrics are computed per searcher as `foundRegistrant / target`. A search terminates either when `foundRegistrant ≥ target` or when the per-search timeout fires.

## Aggregate results

| metric | value |
|---|---|
| searchers | 500 |
| full-recall searchers | 269 / 500 |
| timeouts | 231 / 500 |
| mean recall | 0.9805 |
| time to first result, p50 | 82.2 ms |
| time to first result, p95 | 127.7 ms |
| time to completion, p50 | 3103.3 ms |
| time to completion, p95 | 90000.5 ms |

## Per-topic results

| topic | regs | searchers | full recall | timeouts | mean recall | t1st p50 (ms) | tc p50 (ms) | tc p95 (ms) |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 0 | 212 | 212 | 0 / 212 | 212 | 0.9565 | 82.7 | 90000.0 | 90000.8 |
| 1 | 106 | 106 | 89 / 106 | 17 | 0.9953 | 82.6 | 2763.5 | 90000.0 |
| 2 | 74 | 74 | 72 / 74 | 2 | 0.9996 | 80.0 | 2078.7 | 2982.3 |
| 3 | 68 | 68 | 68 / 68 | 0 | 1.0000 | 81.0 | 1906.9 | 2798.5 |
| 4 | 40 | 40 | 40 / 40 | 0 | 1.0000 | 81.1 | 1167.1 | 1605.4 |

## Registration timing per topic

Time from `RegisterTopic` call until the registrant first appears in *any* remote DISC-NG node's local topic table. Sampled by polling every 1 s; values therefore have step granularity equal to the probe period.

| topic | registered | mean (ms) | std (ms) | p50 (ms) | p90 (ms) | p99 (ms) |
|---|---:|---:|---:|---:|---:|---:|
| topic 0 | 212 | 2039 | 3999 | 1011 | 1011 | 19010 |
| topic 1 | 106 | 1153 | 1450 | 1011 | 1011 | 1011 |
| topic 2 | 74 | 1011 | 0 | 1011 | 1011 | 1011 |
| topic 3 | 68 | 1011 | 0 | 1011 | 1011 | 1011 |
| topic 4 | 40 | 1011 | 0 | 1011 | 1011 | 1011 |

## Figures

### 01_topic_distribution

![01_topic_distribution](01_topic_distribution.png)

*Topic distribution: registrants per topic, sorted by population (Zipf head on the left).*

### 02_per_topic_recall

![02_per_topic_recall](02_per_topic_recall.png)

*Per-topic discovery success: bars show the per-search average fraction of registrants found and the fraction of searches that found *all* registrants (complete searches).*

### 03_time_to_first_cdf

![03_time_to_first_cdf](03_time_to_first_cdf.png)

*CDF of time-to-first result across all searchers.*

### 04_time_to_completion_cdf

![04_time_to_completion_cdf](04_time_to_completion_cdf.png)

*Per-topic CDF of time-to-completion.*

### 05_id_space_registrants

![05_id_space_registrants](05_id_space_registrants.png)

*Strip plot showing registrants placed in the ID space (top 64 bits, normalized 0..1), one row per topic.*

### 06_id_space_found_vs_missed

![06_id_space_found_vs_missed](06_id_space_found_vs_missed.png)

*Per-registrant discovery coverage, one subplot per topic. x = registrant ID position, y = number of times the registrant was found across all searches in the simulation. Green = found by every searcher, orange = some misses, red = many misses.*

### 07_registration_time_bar

![07_registration_time_bar](07_registration_time_bar.png)

*Mean registration latency per topic with std error bars (clipped at 0).*

