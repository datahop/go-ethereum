# Registrar-core benchmark

Build from the repository root (Go 1.24 or newer):

```sh
go build -o /tmp/registrarbench ./p2p/discover/topicindex/cmd/registrarbench
go test ./p2p/discover/topicindex ./p2p/discover/topicindex/cmd/registrarbench
/tmp/registrarbench --ip-file "" --advertisers 100 --topics 30 --reg-rate 200 --query-rate 300 \
  --duration 4s --warmup-fraction .5 --cache-size 500 --ad-lifetime 1s \
  --sample-interval 500ms --output /tmp/registrarbench.jsonl
```

This short synthetic example checks the harness, not steady-state production performance.
Omit capacity/lifetime overrides to use production defaults (currently 5000 and 15m).
Default fixed population is 10000, topics 300, Zipf alpha 1, seed 1, rates 100/s each,
duration 1h, warm-up fraction .5 and sampling interval 1s. `--help` lists flags.
The query result limit is 16, mirrored from discover.topicNodesResultLimit.
One invocation is one offered-load point. Linux and macOS are supported.

Only TopicTable.Register, RandomNodes and Expire perform registrar work. No
transport, ticket authentication/encoding, routing table or
Ethereum node is used. ENRs are deterministically signed before timing starts.
No production registrar behaviour is changed.

## IP input

Use `--ip-file addresses.txt` for one IPv4 per nonblank line (`#` comments allowed),
or a `.csv` file with an `ip` column (`--ip-column` overrides the header).
Whitespace and IPv4-mapped IPv6 representations are normalized. Other IPv6 and
malformed rows fail. Duplicate rows and input order are preserved. Each advertiser
independently samples one row uniformly **with replacement**, using a separate
PRNG seeded with `seed + 2`. Thus A,A,B gives A twice B's sampling probability;
there is no deduplication, deterministic cycling, or synthetic replacement.
The fixed advertiser population can exceed the row count. Empty usable input fails.
The same seed, population and file contents/order reproduce the mapping; increasing
the population preserves the mapping prefix. Topic and runtime-query RNG streams
are separate. Metadata records `dataset.ip_assignment_mode` as
`empirical_rows_with_replacement` (or `synthetic_unique_ipv4` without an input file),
the original-file hash, row count and source/assigned distinct-IP counts. Population
and seed remain in the existing configuration/metadata fields. The default file is `results/data/dnslist-ipv4.txt` relative
to the working directory (normally `p2p/discover`). Pass `--ip-file ""` explicitly
for development without the dataset: unique synthetic non-LAN/non-special IPv4
addresses are generated. These do not reproduce the crawl's prefix distribution.

## Scheduling and lifecycle

One serial engine maintains fresh-advertiser FIFO state, a retry deadline heap,
and an active-advertisement expiry heap. Query deadlines and registration pacing deadlines
are implicit; all are merged with table expiry, sampling and warm-up deadlines.
`--mode realtime` (default) supplies `mclock.System` to TopicTable and sleeps until
needed. `--mode simulated` supplies `mclock.Simulated` to both table and lifecycle,
advancing directly to each event. Thus admission lower-bound decay, waiting and
expiry all see logical time. `time.Now/Since` always measures real method execution.

`--reg-rate` schedules **total registration opportunities** at `i / rate`
seconds from experiment start (`i = 0, 1, ...`), sharing capacity between fresh
attempts and retries. At 20/s, scheduled times are 0, 50, 100, 150ms, independent
of Register completion. Nanosecond timestamps use the same rounding as queries.
Rate zero disables registration opportunities. Simulated time jumps between events;
method execution consumes wall time but does not advance the logical clock.

In realtime, a serial worker services late opportunities with their original
arrival timestamps. Overload creates backlog and scheduling lag; it never shifts
future arrivals. Consequently the cap governs **scheduled demand**, not a sliding
window of actual dispatches: recovery from overload may dispatch queued arrivals
closer together. These are outstanding arrivals, not unused positive credit.
Queries already use the same origin/index schedule and retain their original times.
At a registration dispatch, valid due retries take priority; otherwise a fresh
advertiser is selected. There is no idle-capacity bank or deficit accounting.
If neither a valid retry nor a fresh advertiser is available, the opportunity
is consumed without a Register call and recorded as population scarcity. Queueing delay is actual start minus the ORIGINAL scheduled
opportunity for both fresh and retry attempts. `retry_due_ns` separately records
the waiting-time deadline, used for retry validity, not arrival queueing delay.

A positive wait schedules a retry at **method entry time + returned wait**, matching
`v5_udp.go:handleRegtopic`'s `LastUsed` timestamp. Credited waiting passed to Register
is elapsed time since the first wait-producing attempt, including valid queueing.
Production `topicindex/ticket.go:Ticket.isValidAt` accepts
`[LastUsed + WaitTimeIssued, due + 5s)`. Both modes obey this window. Expiry is an
independent event, so queued retries expire even when the registration cap is zero.
At/after the exclusive deadline they emit `retry_expired`, increment
`expired_retries`, and end that registration lifecycle. The advertiser returns
exactly once to the normal fresh FIFO with waiting credit cleared. Expiry itself
does not call Register, consume capacity, or create a restart event; the advertiser
can only be selected through an ordinary registration opportunity. There is no
client timeout or registrar-reselection simulation. Ticket crypto, wire rounding,
and the client `RegAttemptTimeout` attempt budget remain excluded. Tests compare
the mirrored grace boundary against production TicketSealer decoding.

Active advertisers are released at method completion + AdLifetime (exact table
expiry in simulation, conservatively later in realtime). Expire runs before releasing
advertisers and before requests whenever NextExpiryTime is due. No active refreshes
are generated; inconsistent admission/occupancy results still fail immediately.
At duration the worker stops without draining outstanding work; pending/overdue
retries, retry queue length, query backlog and ready population are reported.

Both modes pre-generate **exactly `--advertisers` identities** before timing.
There is no population expansion based on rates, duration, lifetime or retries.
An admitted advertiser remains unavailable until its advertisement expires and
Expire has removed it; it then returns to the fresh FIFO. Waiting advertisers
stay exclusively in the retry lifecycle. An expired retry is dropped, but its
advertiser becomes fresh-eligible again. No keys, ENRs or new identities are
generated during timing. The population model is recorded as `fixed-reusable-v2`
so the analyzer separates these runs from earlier runs that permanently retired
advertisers after retry expiry.
`--saturation` is a realtime marker and does not alter population size.

If no valid due retry or fresh advertiser exists at a scheduled opportunity,
`population_scarcity_events` increments, a `population_scarcity` event is emitted
in measurement, and the schedule advances normally. No call or latency observation
is fabricated, and this unused opportunity can never be recovered as credit.
Eligibility is evaluated by the serial worker at dispatch, including when realtime
arrivals are late. Runs continue by default. Summary `population_limited` is true if **any** opportunity
was missed, including warm-up. `measured_population_limited` and measured scarcity
counts are separate. Use the flag in summaries to reject/re-run scientific load
points; the analyzer retains their output and adds an explicit note.

Summary requested/actual/pregenerated populations are equal; the legacy
`population_auto_sized` field is always false. `population_scarcity_fraction` divides
whole-run missed opportunities by all scheduled registration arrivals.
`population_scarcity_interval_ns` sums the scheduled intervals of missed opportunities
(clipped at duration), not physical continuous stall time. `fresh_supply_limited_ns`
tracks time with no ready advertiser or due retry. Samples include cumulative
scarcity counts; registration-composition CSV/plots include scarcity events/s.
`handled_registration_opportunities` (also the legacy
`dispatched_registration_arrivals`) includes calls and scarcity misses; unserved
arrivals count only the backlog left at termination, not handled scarcity misses.

The IPv4 file must contain at least one usable row. Sampling with replacement
allows any fixed advertiser count while preserving empirical row weights. Empty or
invalid input fails before population construction/timing; no synthetic fallback occurs.
Use the same fixed population and IP input across load points, checking scarcity
flags to determine whether each point achieved its requested demand.

Warm-up uses workload time: `--warmup 3h` overrides `--warmup-fraction .5`, so
`--mode simulated --duration 6h --warmup 3h` advances six logical hours. State is
retained across the boundary. Samples cover both phases and contain cumulative
counts; raw method operations cover measurement only. CPU utilisation, real queue
delay and sustainable throughput are realtime-only. Simulated rates use logical
seconds; its wall runtime is execution cost, not workload CPU utilisation. Optional
CPU profiles in simulation describe execution only. Audit timings remain invalid.

Short simulated development example (from `p2p/discover`):

```sh
/tmp/registrarbench --mode simulated --ip-file "" --advertisers 100 --topics 30 \
  --reg-rate 20 --query-rate 30 --duration 3m --warmup 90s \
  --cache-size 50 --ad-lifetime 30s --sample-interval 10s --output /tmp/simulated.jsonl
```

## JSONL and metric interpretation

* `metadata`: resolved configuration, dataset, realized topic populations, baseline,
  runtime/build details and metric definitions. Config durations are nanoseconds.
* `operation`: method latency (excluding queue delay and harness lifecycle work),
  topic rank/population, pre-operation topic size and occupancy, outcome or result
  count, fresh/retry status and credited waiting. Scarcity events are separate from methods. Admission means a new cache entry;
  an inconsistent return/occupancy delta fails the run with a diagnostic.
* `sample`: time, phase, cache and admission structural counts, ads per topic
  (array index = rank minus one), heap, RSS and cumulative process CPU seconds.
* `summary`: fresh/retry/total/admitted/wait/queued/expired-retry counts and
  rates, outstanding demand, logical and wall durations, latency distributions,
  realtime CPU and process allocations. Schema 5 records fixed, reusable population semantics and the unchanged arrival schedule. Summaries include scheduled,
  dispatched and unserved registration arrivals. Samples and summaries include cumulative
  queued retries, maximum due queue length and current queue length. Queued counters
  include retries served immediately; queue length counts due, not future, retries.

Immediate-admission fraction divides fresh immediate admissions by fresh attempts.
Wait-response fraction divides waits by all processed registrations. An admission
following earlier waiting is counted separately, even if waiting began in warm-up.
Live heap is runtime's **last completed GC** estimate. Incremental live heap subtracts
a forced-GC baseline taken after population and empty-table construction. Samples
include GC cycle counts: with no intervening GC, live heap has not been refreshed.
Current HeapAlloc is reported separately and can include uncollected garbage.
A final forced-GC snapshot is taken outside measurement, keeping population and table
alive. Negative incremental values are retained rather than clamped.

Heap and CPU include harness work, JSON encoding, runtime/GC, scheduling and sampling;
RSS is whole-process supplementary memory, not registrar-owned memory. Node objects
are already retained by the population at baseline, so incremental heap primarily
captures cache/index structures, not the cost of owning another copy of every ENR.
On macOS RSS sampling invokes ps, which adds sampling delay; errors are emitted as
rss_error, not silently interpreted as zero memory. CPU is getrusage self user+system:
100% means one occupied core and excludes ps child CPU. GC environment settings are
recorded. Default empty GOGC/GOMEMLIMIT environment values imply Go runtime defaults.

No per-operation MemStats reads occur in the primary mode. Total allocation rate is
process-wide. In `--allocation-audit`, MemStats brackets each registrar method and
reports bytes/allocations per operation and expiry. Those runs are marked
performance_valid=false: instrumentation distorts CPU, latency and throughput.
Primary-run per-method allocation counters are not measurements.

Latency means are exact. Summary percentile fields end in `_upper`: fixed logarithmic
histogram bucket upper bounds (at most about 3.125% width above 32ns), without retaining
a growing in-memory latency array. Raw latency_ns permits exact offline quantiles.
Buffered JSONL output is intentionally retained as a visible harness cost; a slow disk
can bottleneck the worker. Use local fast storage and optional `--cpu-profile` to
assess attribution before interpreting saturation as solely TopicTable throughput.

The harness sets GODEBUG=randseednop=0 and seeds the global math/rand source
used by production reservoir sampling, in addition to separate workload RNGs.
This uses Go's seeded locked PRNG rather than the default runtime random source;
its small locking cost is included in query timing. The resolved GODEBUG is recorded.

Seeds reproduce population and simulated lifecycle decisions (tested by replay).
Realtime timing/GC and near-boundary admission outcomes remain variable. Repeat runs. Production's full-cache duration arithmetic and
history-dependent trie behaviour are intentionally not corrected by this harness.

## Offline analysis and plots

The Python script `analyze.py` reads completed JSONL files and does not run the
benchmark. It needs Python 3.9+ and matplotlib (which installs NumPy); pandas is
not required. From the repository root:

```sh
python3 -m venv /tmp/registrarbench-analysis-venv
/tmp/registrarbench-analysis-venv/bin/python -m pip install matplotlib

# Inspect syntax and command-line usage without running any experiment.
PYTHONPYCACHEPREFIX=/tmp/registrarbench-pycache \
  /tmp/registrarbench-analysis-venv/bin/python -m py_compile \
  p2p/discover/topicindex/cmd/registrarbench/analyze.py
/tmp/registrarbench-analysis-venv/bin/python \
  p2p/discover/topicindex/cmd/registrarbench/analyze.py --help

# Analyze an existing single run.
/tmp/registrarbench-analysis-venv/bin/python \
  p2p/discover/topicindex/cmd/registrarbench/analyze.py \
  /tmp/registrarbench.jsonl --output-dir /tmp/registrarbench-plots

# Analyze separately collected load points and allocation audits together.
/tmp/registrarbench-analysis-venv/bin/python \
  p2p/discover/topicindex/cmd/registrarbench/analyze.py \
  results/reg100-query100.jsonl results/reg200-query100.jsonl \
  results/audit-reg100-query100.jsonl results/audit-reg200-query100.jsonl \
  --output-dir results/plots
```

To collect an allocation audit separately from a primary run, use the same workload
flags and add `--allocation-audit`, with a different output path. For example, after
building as above:

```sh
/tmp/registrarbench --ip-file "" --advertisers 100 --topics 30 --reg-rate 200 --query-rate 300 \
  --duration 4s --warmup-fraction .5 --cache-size 500 --ad-lifetime 1s \
  --sample-interval 500ms --allocation-audit --output /tmp/registrarbench-audit.jsonl
```

These are development examples. For experimental collection, supply the crawl
using `--ip-file`, retain production parameters where appropriate, and allow enough
time for warm-up/expiry. This tooling does not implement a sweep or launch runs.

### Output and statistical conventions

Every figure is saved as a 300-dpi PNG and a vector PDF. Each input gets a separate
subdirectory containing its metadata, summary CSV, resource figures and (for valid
primary runs) latency figures. `runs.csv` summarizes all inputs; `analysis.txt`
records assumptions, warnings and explicitly skipped plots. Compatibility-group
subdirectories contain cross-run figures and `configuration.json` listing the
compared settings and runs. Use a fresh output directory to avoid retaining plots
from an earlier invocation.

* Warm-up and measurement samples have separate markers/line styles. The actual
  recorded measurement boundary is used; operation records are also filtered by
  that boundary. Method latency comes only from `latency_ns`, never queue delay.
* Latency curves show median/p95/p99 computed from raw observations with NumPy's
  linear-interpolation quantiles. At most 20 x bins are used (`--bins` changes this).
  Unique x values are kept when possible; otherwise equal-frequency boundaries
  group observations without splitting ties. Each point is plotted at the median
  observed x in its bin. Per-operation-type CSVs contain bin ranges and counts.
  Sparse-bin tail percentiles are descriptive, not confidence intervals. Overall
  exact raw-data quantiles and means are included in `runs.csv`.
* Popularity has two explicit views: topic rank (1 is most popular) and realized
  advertiser population for the topic. Current topic-cache size is plotted separately.
  Quantiles are request-weighted, including both fresh and retry registrations and
  both admitted and waiting outcomes; topics are not given artificial equal weight.
* Incremental live heap uses the recorded baseline-subtracted value, preserving
  negative values. It is a last-GC estimate paired with sampling-time occupancy,
  not necessarily live memory at that exact occupancy. The post-measurement
  forced-GC endpoint is marked separately. RSS has its own whole-process figure;
  error/missing RSS observations are skipped with a diagnostic. Admission-state
  figures show structural counts, not invented byte estimates.
* Compatibility groups require equality of recorded configuration, dataset hash
  and cardinalities, seed, realized population, revision, Go/runtime/GC settings,
  duration, sampling settings and audit/profile mode. Only the two offered rates
  and irrelevant file paths are ignored. Files are never silently pooled. Repeated
  runs appear as independent points. The benchmark does not record machine identity,
  so check host/hardware equivalence yourself when comparing runs.
* One-dimensional offered-rate plots hold the other request rate fixed. CPU and
  process-allocation load maps can also show mixed-rate points without implying
  that one request type alone caused the difference. Cross-run comparisons require
  at least two distinct compatible offered-load points; otherwise a skip is recorded.
* Registration plots distinguish the configured total-attempt target, processed
  fresh/retry attempts and new admissions. Legacy slot-based inputs retain their
  recorded summary fields but are assigned separate compatibility groups.
* Allocation-audit runs get separate per-operation allocation figures and CSVs.
  Audit and `performance_valid=false` runs never enter primary latency, throughput
  or CPU figures. Primary allocation rates are process-wide (including harness and
  output); audit operation allocations come from the separately instrumented run.

The reader streams JSONL but retains compact numeric raw-operation columns for exact
quantiles; memory therefore grows with measured request count. Truncated runs lacking
metadata/final summary and malformed JSON fail with an explicit error. Missing raw
operations do not fall back silently to approximate summary histogram percentiles.

Mode and registration-rate semantics are compatibility keys in the analyzer: legacy
slot-based, credit-based, completion-paced and fixed-arrival runs are not pooled together. Simulated runs retain method
latency, state and memory plots, but never enter CPU/saturation comparisons.
`registration_composition.{png,pdf,csv}` shows interval total/fresh/retry/admission/
wait/expired-retry/scarcity rates with warm-up shaded; CSV marks intervals wholly
inside measurement. Expiry method latency is plotted against cache utilisation.

Focused validation (from `p2p/discover`):

```sh
go test -count=1 ./topicindex ./topicindex/cmd/registrarbench
python3 -m unittest discover -s topicindex/cmd/registrarbench -p 'test_analyze.py'
```
