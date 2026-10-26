# Registrar-core benchmark

`registrarbench` drives a single `topicindex.TopicTable` (the TopDisc registrar ad
cache and admission control) with a synthetic advertiser population and query load.
Only `TopicTable.Register`, `RandomNodes` and `Expire` do registrar work. There is no
transport, session crypto, ticket MAC or packet encoding. Production registrar code is
used unmodified.

Build natively. On Apple Silicon with an amd64 Go toolchain, `GOARCH=arm64` is
required; the harness records and rejects Rosetta-translated runs (see Validity).

```sh
# from p2p/discover
GOARCH=arm64 go build -o /tmp/registrarbench ./topicindex/cmd/registrarbench
go test ./topicindex ./topicindex/cmd/registrarbench
/tmp/registrarbench --mode simulated --cache-size 1000 --fresh-rate 2 --query-rate 20 \
  --advertisers 100000 --duration 24h --warmup 12h --sample-interval 10s \
  --raw-ops=false --output /tmp/run.jsonl
```

## Modes

| mode        | clock                                   | valid for |
|-------------|-----------------------------------------|-----------|
| `simulated` | `mclock.Simulated`, jumps to each event | cache state, admission dynamics, waits, memory; per-call latency distributions only as descriptive data |
| `realtime`  | system clock, arrivals in wall time     | throughput, CPU, queue delay, latency under load |
| `hybrid`    | simulated during warm-up, then real time from the simulated time base | as `realtime`, without spending hours of real warm-up |
| `memfill`   | none (`TopicTable.Add` to fixed occupancy levels) | bytes per cached ad (slope of post-GC heap) |

The hybrid clock is continuous and monotone at the switch:
`Now() = simNow(switch) + (real.Now() - real.Now(switch))`. All absolute times stored by
`TopicTable` (ad expiry, lower-bound timestamps) keep their meaning; ads admitted during
the simulated warm-up expire on schedule in real time. `TestHybridTableEquivalence`
checks that expiry and lower bounds evolve identically to a purely simulated table.

Per-call latencies are measured with `time.Now`, whose resolution is recorded as
`platform.timer_resolution_ns` (41.67 ns on Apple Silicon). Sub-microsecond operations
are only a few ticks long; use the `testing.B` benchmarks in `topicindex` for per-op cost.

## Workload model

* **Population.** `--advertisers` identities are generated before timing, each with a
  signed ENR (IPv4, UDP 30303) and one topic drawn from Zipf(`--zipf-alpha`) over
  `--topics` topics. IP addresses come from `--ip-dist`: `empirical` (rows of
  `--ip-file` sampled with replacement), `diverse` (unique random public IPv4) or
  `concentrated` (random hosts in `--ip-prefixes` random /16 prefixes).
* **Fresh arrivals.** Registration attempts start at rate `--fresh-rate`
  (`--arrivals deterministic|poisson`). Each takes the next idle advertiser from a FIFO.
  If none is idle, the arrival is lost and counted as `population_scarcity`; it is
  never banked as credit, and any scarcity during measurement sets `load_valid=false`.
* **Retries.** A quoted wait is received as ceil-milliseconds (as on the wire). A
  continuing advertiser retries exactly at issue time + wait, as its own arrival,
  independent of the fresh-arrival rate. The registrar credits `now - first ticket
  time`. In real time, a retry served 5 s or more after its due time is outside the ticket
  window (`[due, due+5s)`, `ticket.go`); production drops it, and the harness counts it as
  `expired_retries` and returns the advertiser to the idle FIFO.
* **Abandonment** (`--abandon`):
  * `production` (default): `topicindex.Registration.HandleTicketResponse`. The
    attempt is dropped if a **single** quote exceeds `RegAttemptTimeout`, or if the
    **sum of all quotes** in the attempt exceeds it. Both comparisons are strict;
    `RegAttemptTimeout` defaults to 1.5·E (`--attempt-timeout` overrides it).
    `TestAbandonmentMatchesProductionClient` checks equivalence against the
    production state machine.
  * `spec`: drop if a single quote exceeds E ("a single waiting interval SHOULD NOT
    exceed E"); no cumulative limit.
  * `none`: retry after any wait (the original harness behaviour).

  An abandoning advertiser leaves this registrar: it returns to the end of the idle
  FIFO and can come back only through a later fresh arrival, which starts a new
  attempt with no credit.
* **Admission and expiry.** An admitted advertiser is busy until its ad has expired
  and been removed by `Expire`, then it becomes idle again. No renewals are modelled.
* **Queries.** At `--query-rate`, topic drawn from the same Zipf distribution,
  `RandomNodes(topic, 16, check)` with the production `TOPICQUERY` callback
  `netutil.CheckRelayAddr(requester, n.IPAddr())` and a fixed public requester.
* **ENR copies.** With `--independent-enr` (default), each request passes a freshly
  RLP-decoded `enode.Node` to the registrar, as `handleRegtopic` does. The decode is
  outside the timed region. With `--independent-enr=false`, the population's node is
  shared and its ENR storage is not charged to the registrar.

## Output (JSONL, schema 6)

* `metadata`: resolved config, dataset (source, SHA-256, rows, distinct IPs),
  `platform` (revision, dirty flag, Go version, GOOS/GOARCH, CPU model, P/E core counts,
  Rosetta flag, timer resolution), topic populations.
* `operation` (only with `--raw-ops`, measurement phase only): one record per method call.
* `sample`: occupancy and admission-state structure counts, ads per topic, memory, CPU,
  cumulative counters.
* `summary`: measured counters and rates, `time_avg_occupancy` (exact time integral),
  Little's-law ratio `time_avg_occupancy / (admission_rate·E)`, per-half statistics for
  stationarity, wait fraction, abandonment rate/fraction, quoted waits, cumulative wait
  at admission (all admissions, and admissions after waiting), latency and queue-delay
  histograms, CPU per operation (real time only), validity flags.
* `memfill` / `memfill_bounds`: post-GC heap at each occupancy level per variant
  (`shared`, `independent`) and repetition, and the heap cost of topic lower bounds.

Histogram percentiles are bucket upper bounds (≤3.125% wide).

## Validity

* `performance_valid` is false for allocation audits, simulated runs, an unknown
  revision, or a Rosetta-translated process (unless `--allow-translated`).
* `load_valid` is false if any fresh arrival found no idle advertiser during measurement.
* macOS does not expose the core type a thread ran on. `--pacing spin` busy-waits
  between arrivals so that the worker's core does not idle.

## Analysis

`study.py` (in this directory) aggregates schema-6 runs across seeds (mean ± 95% CI),
checks Little's law and stationarity, and writes the tables and figures for
`docs/registrar-overhead`. `analyze.py` is the original single-run plotting tool for
the earlier schema-5 bundles. Those bundles were all produced by an amd64 binary under
Rosetta 2 and are kept for reference only.
