# Simnet testbed: traces and figures

What the testbed writes, what is inside each file, and which figure each field
drives. Every figure below is produced by one of the two scripts in this
directory; nothing here needs manual post-processing.

## Producing a run and its figures

```sh
# Build (nested module; the vanilla replace only matters for -vanilla-frac runs)
cd testbed/simnet && CGO_ENABLED=0 go build -o ~/simnet .

# A run with every trace enabled
~/simnet -nodes 5000 -topics 5 -zipf-s 1.07 -seed 1 \
    -bootstrap-wait 60s -register-wait 3m -search-timeout 20m \
    -metrics-out out/r.json \
    -overhead-series-out out/r-overhead.json \
    -overhead-out out/r-oh.json \
    -checkpoint-interval 60s -overhead-series-period 30s

# Figures
python3 figures.py out/r.json --out-dir figs --label myrun
python3 figures_overhead.py out/r-overhead.json \
    --metrics out/r.json --overhead out/r-oh.json --out-dir figs --label myrun

# Cross-run churn/deployment report (needs several runs)
python3 report_plots.py <logdir> <outdir>
```

Search-phase duration is a first-class parameter. Discovery converges, but the
time it needs scales with the number of registrants in a topic, so a search
window that is too short reports an unfinished process rather than a limit.

## Trace files

### `-metrics-out` — the run's search and registration record (JSON)

The large one; hundreds of MB at 10k nodes.

| Field | Contents | Figures |
|---|---|---|
| `perTopic[]` | per topic: `topic`, `numSearchers`, `target`, `fullRecall`, `meanRecall`, `meanUniqueCount`, `meanFoundDup`, `hitTimeout` | 8 |
| `results[]` | one record per searcher (below) | 2, 3, 4, 14, 15 |
| `registrationCoverage.byTopic{}` | `byRegistrant` (registrars holding each ad) and `byHost` (ads held per host) | 10, 11, 12, 14 |
| `registrationTimingNs{}` | topic hex → registrant → ns to first *remote* admission | 9, 13, 15 |
| `registrationStartNs{}` | registrant → ns offset when that node began registering | 9, 13 |
| `registrationPlacements{}` | topic hex → registrant → `{sumNs, count}` over every observed placement | 13 (mean) |
| `findCountByTopic[]` | per topic: `neverFound`, min/p5/p25/p50/p75/p95/max, `counts[]` | tables |
| `topicIds{}` | topic hex → topic index | all ID-space figures |

Per-searcher record in `results[]`:

| Field | Meaning |
|---|---|
| `nodeIdx`, `nodeId`, `topic`, `target` | who searched, for what, and how many registrants existed |
| `uniqueRegistrant`, `foundRegistrantIds[]` | distinct registrants found (the set) |
| `uniqueFoundAtMs[]`, `uniqueFoundIds[]` | index-aligned: when each distinct registrant was first seen, and which |
| `searchStartMs` | this searcher's offset from the run's common epoch (registration start) |
| `timeToFirstNs`, `timeToCompletionNs`, `hitTimeout` | latency and outcome |
| `connectedAtStart`, `alreadyConnectedReg`, `newRegistrant`, `newFoundAtMs[]` | how much was already in the routing table |

`uniqueFoundAtMs` is relative to *that searcher's* start, which is not
comparable across searchers; add `searchStartMs` to put it on the same clock as
`registrationTimingNs`.

### `-overhead-series-out` — periodic samples (JSON)

Sampled every `-overhead-series-period`. Small; a few MB.

| Field | Contents | Figures |
|---|---|---|
| `samples[].tSec` | seconds since spawn | 1, 17, 19 |
| `samples[].txBytes/rxBytes/txMsgs/rxMsgs` | cumulative totals per ID-space bucket (50 buckets) | 17 |
| `samples[].byType{}` | the same, split by discv5 message type | 19 |
| `samples[].nodes[]` | live nodes per bucket, so bucket totals convert to per-node | 17, 19 |
| `samples[].cacheHeld` / `cacheCap` / `cacheByTopic{}` | ad-cache occupancy network-wide and per topic | 1 |
| `waitTime[]` | per topic: `quotedMs[]` (every quote issued) and `admittedMs[]` (cumulative wait per successful registration) | 2 |

Totals are cumulative, so rates come from differencing consecutive samples.
`quotedMs` holds several samples per registration because registrants retry;
`admittedMs` holds exactly one per successful registration and is the answer to
"how long did registering actually take".

### `-overhead-out` — per-node totals at teardown (JSON)

One record per node: `idx`, `id`, `txPkts`, `txBytes`, `rxPkts`, `rxBytes`,
`tqRcv`, and `byType{}` giving the same split per discv5 message type.
Drives figures 5, 6, 7, 16, 18.

### `-reach-out` — search reach sampling (JSON, optional)

Per-searcher queried-registrar sets plus each registrar's topic-table contents,
compacted to `{fanout, load, sample}` so the dump does not stall teardown at
10k. Diagnostic; no standard figure.

### `-snapshot-dir` — periodic recall snapshots (JSON, optional)

`registrants-t<N>.json` manifests plus `snap-<seq>.json` per checkpoint.
Diagnostic.

### Run log (stdout)

Machine-readable lines worth parsing:

| Line | Contents |
|---|---|
| `PARAMS: k=v ...` | every flag value, so a report never has to hard-code the configuration |
| `[checkpoint t=Ns] topic N: ...` | live per-topic coverage, never-found count, find-count percentiles |
| `[checkpoint t=Ns] search-provenance: ...` | nodes and ads sourced from the DHT vs from referrals |
| `[checkpoint t=Ns] search-buckets: occ[...] reject(...)` | search-table occupancy per bucket and why candidates were rejected |
| `[buf] ... peak ... now ...` | router and link buffer occupancy — **check this before trusting a run** |
| `=== dead results in searches ===` | stale-result rate and dead-age percentiles (churn runs) |
| `=== churn summary ===` | joins, kills, alive-at-end (churn runs) |
| `=== cross-stack interop (DHT merge) ===` | routing-table cross-population (mixed-binary runs) |

The `[buf]` line matters: this workload saturates whatever link capacity it is
given, and sustained saturation eventually freezes discovery progress, though
only some minutes after onset. A run whose `final peak` shows 100% on links or
router produced timing numbers that include queueing delay.

## Figures

`figures.py` (needs `-metrics-out`):

| # | Stem | Question |
|---|---|---|
| 8 | `01_topic_distribution` | registrants per topic — the Zipf popularity draw itself |
| 4 | `02_recall_reached` | distinct peers found over time, and where each searcher finished |
| 3 | `02b_time_to_first_cdf` | time to a searcher's first result, per topic |
| — | `03_unique_found_over_time` | absolute unique-found over time (superseded by 4) |
| 11 | `04_id_space_registrants` | where registrants sit in ID space, by how widely they placed |
| 12 | `04b_id_space_registrars` | where registrars sit, by how many ads they hold |
| 14 | `05_id_space_found_vs_missed` | how many searchers found each registrant, banded by completeness |
| 10 | `06_fanout_both_views` | fan-out and ads-per-host distributions per topic |
| 9 | `07_registration_latency_bar` | time to first remote admission, per topic |
| 13 | `07_placement_time_idspace` | time to place an ad *anywhere*, across ID space |
| 13 | `07b_placement_mean_idspace` | mean time to place across *all* its registrars |

`figures_overhead.py` (needs `-overhead-series-out`; `--overhead` and
`--metrics` unlock the rest):

| # | Stem | Question |
|---|---|---|
| 16 | `oh_01_idspace_traffic` | per-node total sent/received across ID space |
| 17 | `oh_02_idspace_peak_rate` | peak sustained per-node rate across ID space |
| 18 | `oh_03_idspace_msgtype` | per-node bytes by message type across ID space |
| 19 | `oh_04_idspace_peak_msgtype` | peak rate by message type across ID space |
| 2 | `oh_05_wait_time_cdf` | registrar-quoted waiting times, per topic |
| 15 | `oh_06_idspace_found_time` | ad-placed → first-discovered latency on the common clock |
| 7 | `oh_07_load_vs_topic_distance` | load against XOR distance to the topic ID |
| 1 | `oh_08_cache_utilisation` | ad-cache utilisation over time, network-wide and per topic |
| 5 | `oh_09_cost_per_lookup` | lookup traffic per searcher vs topic popularity |
| 6 | `oh_10_reg_vs_lookup` | registration vs lookup traffic per node, across ID space |

`report_plots.py` (cross-run; needs several run logs plus their JSON):

| # | Stem | Question |
|---|---|---|
| 20 | `churn_deadresult` | fraction of search results that are already-dead nodes, by churn rate |
| 21 | `churn_deadage` | how stale those dead results were |
| 22 | `search_discovery_by_rate` | discovery over time across churn rates |
| 23 | `reg_fanout_by_rate`, `reg_hostload_by_rate`, `search_ttf_by_rate` | fan-out, ads-per-host and time-to-first CDFs overlaid by rate |

A figure is written only when its data is present: a measurement the run did not
record produces no file rather than an empty plot.

## Instrumentation switches

Counters that cost something on the hot path are off unless the corresponding
flag is set: `-overhead-out` enables per-message-type wire counting and
TOPICQUERY-received counting, `-overhead-series-out` additionally enables
registrar wait-time sampling, and `-reach-out` enables per-searcher reach
sampling. Runs without those flags carry no measurement overhead.
