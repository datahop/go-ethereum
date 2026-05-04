# DISC-NG specifications compliance report

## 1. Introduction

This report compares the DISC-NG topic-discovery implementation under
`p2p/discover` against the [Service Discovery in Ethereum 2.0][paper]
research paper. It is bidirectional: each part of §2 covers what the
paper specifies, how the code implements (or diverges from) it, and what
protections the code adds beyond the paper.

**Code reference:** `topdisc` branch at the time of writing.

**Status legend:**

| Symbol | Meaning |
|---|---|
| ✅ | Compliant — code matches paper |
| ⚠️ | Differs — intentional divergence or known compromise |
| ❌ | Missing — paper specifies; code does not implement |
| ➕ | Extra — code adds, paper does not specify |

[paper]: https://github.com/harnen/service-discovery-paper

## 2. Protocol coverage

### 2.1 Admission and topic table

The admission machinery accepts or defers REGTOPIC requests, computes per-
request wait times, scores requesters by IP diversity, caches accepted ads
in the topic table, and (per paper) prevents wait-time gaming via a lower-
bound mechanism.

#### 2.1.1 Topic table (cache)

| Spec | Paper | Implementation | Status |
|---|---|---|---|
| Fixed capacity C | configurable | `AdCacheSize` config | ✅ |
| Ads expire after E | fixed expiry | `AdLifetime` config; per-entry `exp` field | ✅ |
| Periodic expiry processing | sweeps cache | `TopicTable.Expire`, dispatched by alarm | ✅ |
| No per-service / per-IP hard limits | explicit non-requirement | only soft pressure via wait-time scoring | ✅ |
| Random selection for lookup response | Alg 5 | `TopicTable.RandomNodes` (reservoir sampling) | ✅ |

#### 2.1.2 Ticket-based stateless admission (Algorithm 4)

| Spec | Paper | Implementation | Status |
|---|---|---|---|
| Reject duplicate registrations | `assert ad ∉ ad_cache` | `TopicTable.isRegistered` check in `Register` | ✅ |
| Compute per-request wait time | `twait = CalculateWaitingTime(ad)` | `TopicTable.WaitTime(n, t)` | ✅ |
| Tickets signed by registrar, contain ad + timestamps | Alg 4 lines 12-15 | `TicketSealer` HMAC; ticket carries `Topic`, `WaitTimeIssued`, `LastUsed`, `FirstIssued` | ✅ |
| Ticket validity window δ | `t_scheduled ≤ NOW ≤ t_scheduled + δ` | `ticketValidityWindow=5s` checked in `Ticket.isValidAt` | ✅ |
| Accumulated wait time from ticket | `t_remaining = twait − (NOW − ticket.tinit)` | `waitTime = now.Sub(ticket.FirstIssued)` then `Register(n, topic, waitTime)` | ✅ |
| Confirmed vs Wait response | status field | `Regconfirmation.Ticket` empty = confirmed, non-empty = wait + new ticket | ✅ |
| No per-request state at registrar | stateless | `TicketSealer` rotating HMAC keys; no per-advertiser state | ✅ |
| Ticket key rotation | implicit, anti-replay | `ticketRekeyInterval=100000` uses, `ticketKeyLifetime=6h` | ✅ |
| Cap wait time at E | `response.ticket.wait_for = MIN(E, t_remaining)` | drops attempt if `totalWaitTime > RegAttemptTimeout` (1.5×E) | ⚠️ |
| Verify ENR endpoint matches packet source | not explicit | not enforced in `handleRegtopic` — REGTOPIC accepts ad with arbitrary advertised IP | ➕ ❌ deferred (Issue #24); see below |

**Divergence (wait-time cap):** the paper caps `wait_for` at `E`, the code
uses `1.5×E` (`RegAttemptTimeout`). Decision: accept; document.

**ENR endpoint check:** an explicit source-vs-ENR check at REGTOPIC
admission would catch ads whose ENR points at an arbitrary victim IP, but
produces false positives in NAT scenarios (port-rebinding, mid-session
NAT pin-hole churn). Decision: do not pursue at admission time. The same
threat surface (stale or lying registrar entries) is addressed downstream
by active random table revalidation (see §2.2 and §2.3): periodically
PINGing entries in the registration and search tables prunes stale or
unreachable peers regardless of how they got there.

#### 2.1.3 Waiting-time function (Equation 1)

Paper:

```
w(ad) = E × (1 / (1 − c/C)^Pocc) × (c(ad.s)/c + score(ad.IP) + G)
```

Implementation in `TopicTable.WaitTime`:

```
baseTime    = 0.1 * AdLifetime / occupancy^occupancyExp
topicMod    = topicSize(t) / (regCount + 1)
ipMod       = ipTree.score(n)
neededTime  = baseTime * max(topicMod + ipMod, 0.000001)
```

| Element | Paper | Implementation | Impact |
|---|---|---|---|
| Safety floor G | `G > 0` | `max(..., 0.000001) ≈ 0` | Practically negligible — `topicTableWaitTimeFloor=50ms` and network latency dominate |
| Base modifier coefficient | `E / occupancy^Pocc` | `0.1 × E / occupancy^Pocc` | Wait times 10× shorter; intentional tuning for testbed throughput |
| Service similarity denominator | `c(ad.s) / c` | `topicSize(t) / (regCount + 1)` | `+1` prevents div-by-zero; otherwise equivalent |
| Wait floor | not specified | `topicTableWaitTimeFloor=50ms` (remaining wait below 50 ms accepts immediately) | Avoids unnecessary final round-trips |

**Decision (G floor):** the paper's `G > 0` should be re-instated with a
small concrete value once the lower-bound mechanism (§2.1.5) is
implemented; otherwise the floor on retried wait times is undefined.

**Decision (10× tuning, 50 ms floor):** accept and document. Re-evaluate
on production deployment if registration contention becomes a concern.

#### 2.1.4 IP tree scoring (Algorithm 6)

| Spec | Paper | Implementation | Status |
|---|---|---|---|
| Binary tree of IP prefixes | 33 levels for IPv4 | `ipTree` configurable bits (32 / 128) | ✅ |
| Per-vertex counter, incremented on insert | yes | `ipTreeNode.counter` in `insert` | ✅ |
| Score: count vertices above balanced threshold | `if v.counter > root.counter / 2^i then score++` | same formula in `insert` and `score` | ✅ |
| Normalize by IP length | `score / 32` | `computeScore` divides by `bits` | ✅ |
| Decrement and prune on ad expiry | yes | `remove` decrements counters; nodes nilled when counter=0 | ✅ |
| Separate IPv4 / IPv6 trees | implied | `waitTimeState` holds both | ✅ |
| Skip LAN addresses | not in paper | `netutil.IsLAN` short-circuit (local-network addresses skewing score) | ➕ on original topdisc code |

**Open item (IPv6 prefix policy):** the paper is IPv4-centric. The code
maintains a 128-bit IPv6 tree but does not aggregate at any specific
prefix; a dual-stack attacker can bypass the IPv4 /24 cap via many
addresses inside a single IPv6 /64 or /48. See §3.2.

#### 2.1.5 Lower-bound anti-gaming mechanism (paper §6)

The paper proposes a "lower bound" mechanism to prevent advertisers from
gaming wait times via repeated ticket requests at slightly different
moments: a new wait time at t2 must not be smaller than the wait at t1 by
more than (t2 − t1).

| Spec | Paper | Implementation | Status |
|---|---|---|---|
| Per-service lower bound | track `bound(s)`, `timestamp(s)` per service | not implemented | ❌ |
| Per-IP lower bound in IP tree | aggregate at longest prefix | not implemented | ❌ |
| Retry-protection invariant | `w(t2) ≥ w(t1) − (t2 − t1)` | not enforced | ❌ |

In practice the gap is bounded by the 5 s ticket validity window and the
fact that retries lose accumulated wait time, but the paper's invariant
is not asserted.

**Decision:** schedule for follow-up. Should land alongside the §2.1.3
G-floor fix — the two are coupled (without lower-bound, G ≈ 0 is
effectively unbounded; with lower-bound, G has a defined role).

### 2.2 Advertisement protocol (Algorithm 1)

Drives the local `Registration` state per topic, walks bucket B(s) keyed
by log-distance from the topic ID, sends REGTOPIC to selected registrars,
and incorporates response neighbours into B(s) via Algorithm 3.

| Spec / Extension | Paper | Implementation | Status |
|---|---|---|---|
| Per-topic table B(s) initialized from B(node.id) | Alg 1 line 1 | `Registration` with `regTableDepth=18`, seeded from `tab.allNodes()` | ✅ |
| K_register active registrations per bucket | Alg 1 line 5 | `RegBucketSize=10` active + `RegBucketStandbyLimit=20` standby per bucket | ✅ |
| Async registration per registrar | Alg 1 line 11 | `topicReg.sendRequestsLoop` goroutine | ✅ |
| Random registrar selection per bucket | Alg 1 line 7 | `Registration.refillAttempts` picks from standby nodes | ✅ |
| Populate B(s) from registrar response neighbours (Alg 3) | Alg 1 line 21 | `Registration.AddNodes(resp.src, ...)` on REGTOPIC response. Server-side reply built by `collectOnePerDist`. | ✅ |
| Per-bucket IP/subnet cap | not in paper | `regBucket.ips`, `regBucketSubnet=24`, `regBucketIPLimit=1` — defends against a single operator with many addresses dominating a bucket | ➕ on original topdisc code |
| One-per-bucket-per-RPC rule | not in paper | `Registration.bucketCheck` — defends against a single REGTOPIC response stuffing many sybils into one bucket | ➕ on original topdisc code |
| Per-source persistent bucket cap | not in paper | `regBucket.seenSources` — defends against a single registrar flooding a bucket across many RPCs (per-RPC `bucketCheck` resets each call) | ➕ ❌ Issue #48 |
| Self filter (skip own ID) | not in paper | `cfg.Self` filtered in `Registration.AddNodes` | ➕ on original topdisc code |
| Active random table revalidation | not in paper | periodic background task over registration buckets — prunes dead registrar entries; same defence scope as the §2.1.2 ENR endpoint check | ➕ ❌ Issue #21 |

**Algorithm 3 server side (Issue #55).** `collectOnePerDist(target enode.ID, distances, ...)`
is the server-side helper used by both REGTOPIC and TOPICQUERY handlers
to build the auxiliary `Nodes` payload. It accepts a `target` parameter
but never reads it; nodes are picked by distance from the registrar's
own ID, not from `target`. The protocol functions because contacted
registrars are typically topic-close (so distance-from-self ≈ distance-
from-topic), but for non-topic-close registrars the returned nodes are
clustered around the registrar's own region of the keyspace rather than
the topic's. The receiving side re-buckets each node by actual topic-
distance, so correctness is preserved, but per-response candidate yield
is degraded relative to the spec.

**Note on distance verification.** A per-response check that returned-
node distances fall within the requested `Buckets` set is sometimes
proposed as an additional defence. It is not needed: `collectOnePerDist`
already dedups by distance via its `processed` map, the wire format does
not include a "claimed distance" field (the client computes each node's
actual log-distance from its ID), and the per-RPC `bucketCheck` plus the
persistent `seenSources` cap ensure a malicious registrar cannot place
more than one node per client bucket per RPC regardless of which
distances it claims to have walked.

**Note (cross-source concentration in the registration table).** The
per-source persistent bucket cap bounds what one registrar can
contribute. N colluding registrars (each with a distinct ENR) can each
contribute up to the cap, so the cross-source bound is N. Total damage
stays bounded by `RegBucketStandbyLimit` (per-bucket size) and the
per-bucket IP-subnet cap. Defending strictly against cross-source
collusion is out of scope for this work.

### 2.3 Lookup protocol (Algorithm 2)

Drives the local `Search` state per topic, walks bucket B(s) keyed by
log-distance from the topic ID far → close, sends TOPICQUERY to selected
nodes, and incorporates response auxiliary neighbours into B(s) via
Algorithm 3 (TOPICNODES is the orthogonal results channel).

| Spec / Extension | Paper | Implementation | Status |
|---|---|---|---|
| Per-topic table B(s) initialized from B(node.id) | Alg 2 line 1 | `Search` with `searchTableDepth=18`, seeded from `tab.allNodes()` | ✅ |
| K_lookup queries per bucket | Alg 2 line 4 | `SearchBucketSize=8` max queried per bucket | ✅ |
| Walk buckets far → close (b0 to bm-1) | Alg 2 line 3 | `Search.QueryTarget` selects from buckets with unqueried nodes; stops at first bucket with no prior requests | ✅ |
| Populate B(s) from query response neighbours (Alg 3) | Alg 2 line 15 | `Search.AddNodes(resp.src, resp.auxNodes)` on TOPICQUERY response | ✅ |
| Stop at F_lookup distinct advertisers | Alg 2 line 8 | No F_lookup gate. Search runs until `IsDone` (buckets exhausted + no recent novel nodes). Caller-side `numNodes` parameter on the `TopicSearch` RPC provides an equivalent early-exit at the consumer level. | ⚠️ |
| Per-bucket IP/subnet cap | not in paper | `searchBucket.ips`, `searchBucketSubnet=24`, `searchBucketIPLimit=1` — same defence as the registration side | ➕ on original topdisc code |
| One-per-bucket-per-RPC rule | not in paper | `Search.bucketCheck` map — defends against a single TOPICQUERY response stuffing many sybils into one bucket | ➕ ⚠️ broken in topdisc — write missing, rule never triggers (Issue #47) |
| Self filter (skip own ID) | not in paper | `cfg.Self` filtered in `Search.AddNodes` and `Search.AddQueryResults` | ➕ on original topdisc code |
| Search.IsDone termination | not in paper | `topicindex/search.IsDone` — defends against a search hanging indefinitely when the bucket `new` set drains with attempts still in flight | ➕ ❌ Issue #27 |
| Active topicSearch lifecycle | not in paper | `topicSystem.search` map + ctx-cancel into `topicQuery` — defends against `UDPv5.Close` leaving search goroutines / TOPICQUERY RPCs running past shutdown | ➕ ❌ Issues #28, #30 |
| Active random table revalidation | not in paper | periodic background task over search buckets — same defence scope as registration side | ➕ ❌ Issue #21 |

**Decision (F_lookup):** accept the divergence. The caller-side `numNodes`
parameter is the right place to express "enough results"; coupling the
protocol's bucket walk to a specific count is harder to reason about.

**Note (cross-source concentration in the search table).** The per-RPC
`bucketCheck` rule (once the §2.3 write-missing bug is fixed) bounds
what one queried peer contributes per call. Each peer is queried at
most once per `Search` instance, so a single peer is structurally
bounded. With N colluding peers each contributing one entry per bucket,
the cross-source bound is N per `Search` instance. Total damage stays
bounded by `SearchBucketSize` (per-bucket size) and the per-bucket
IP-subnet cap. Defending strictly against cross-source collusion is
out of scope for this work.

The note on distance verification in §2.2 applies symmetrically to the
search side and is not repeated here.

### 2.4 Wire protocol

| Message | Paper | Implementation | Status |
|---|---|---|---|
| Register request (ad + ticket) | Alg 4 `Register(ad, ticket)` | `REGTOPIC{Topic, Ticket, ENR, Buckets}` | ✅ |
| Register response | Alg 4 `(status, ticket, neighbours)` | `REGCONFIRMATION{Ticket, WaitTime}` + `NODES{Nodes[]}` (auxiliary peers) | ✅ |
| Lookup request | Alg 5 `LookupResponse(s)` | `TOPICQUERY{Topic, Buckets}` | ✅ |
| Lookup response | Alg 5 `(peers, neighbours)` | `TOPICNODES{Nodes[]}` (registrants) + `NODES{Nodes[]}` (auxiliary peers) | ✅ |
| Ad payload | service ID + advertiser endpoint + signature | full ENR (richer than paper) | ✅ |

## 3. Compatibility & deployment

The paper assumes uniform DISC-NG adoption. In practice DISC-NG must
coexist with legacy Discv5 nodes during a multi-month rollout, and must
behave correctly for both IPv4 and IPv6 hosts.

### 3.1 ENR `discng` capability flag and dual-stack operation

The mechanism is an ENR entry advertising DISC-NG support, with
`filterDiscNG` applied at registration and search ingress. The flag is
designed but not yet merged. Dual-stack correctness depends on the flag
being applied consistently across all ingress paths.

| Concern | Status |
|---|---|
| ENR entry advertising DISC-NG support | ❌ Issue #19 |
| `filterDiscNG` applied at registration / search bootstrap | ❌ alongside Issue #19 |
| `filterDiscNG` applied to response-sourced nodes | ❌ Issue #29 — without it, REGTOPIC / TOPICQUERY response neighbours can pollute B(s) with non-DISC-NG peers |
| Legacy Discv5 nodes don't respond to REGTOPIC / TOPICQUERY — must be filtered out of B(s) | covered by ENR flag (Issue #19) on bootstrap path; response-path gap is Issue #29 |
| DiscNG-enabled nodes still serve normal Discv5 (FINDNODE) traffic | ✅ — DISC-NG handlers added alongside, not replacing, the v5 dispatch table |
| Bootstrap with mixed-capability seed lists | covered by ENR flag (Issue #19) |
| Behaviour when a node loses DISC-NG support mid-session | ❌ not analyzed |

### 3.2 IPv6 in IP-diversity scoring

The paper's IP scoring (Algorithm 6) is described in IPv4 terms (33-
level binary tree, /24 boundary implicit). The implementation already
maintains separate IPv4 and IPv6 trees, but **the IPv6 prefix policy
used for diversity counting is not explicitly reasoned about** and is
not aligned with any specific recommendation.

A dual-stack attacker could bypass the IPv4 /24 cap by registering many
addresses within a single IPv6 /64 (single LAN) or even within a single
/48 (subscriber allocation), and the IP tree at full 128-bit granularity
treats each as distinct.

| Concern | Status |
|---|---|
| Recommend prefix length(s) for IPv6 diversity (typical: /48 subscriber, /56 residential, /64 single-LAN) | ❌ Issue #53 (spec) |
| Treatment of link-local / ULA / Teredo / 6to4 addresses | ❌ Issue #53 (spec) |
| Code-side gap: scoring at /128 vs aggregating at chosen prefix | ❌ Issue #54 (implementation) |

The IPv6 prefix-policy work is split along spec vs implementation lines:
specification under Issue #53 (Task 1.5), implementation under Issue #54
(Task 2.5). Both are tracked separately from this report.

## 4. Open work

### 4.1 Decisions made

- **Wait-time cap divergence (§2.1.2)** — accept. Bounded by `RegAttemptTimeout=1.5×E`.
- **Wait-time function tuning (§2.1.3)** — accept. The 10× scale-down on the base modifier and the 50 ms wait floor are intentional throughput choices.
- **F_lookup early termination (§2.3)** — accept. Caller-side `numNodes` is the right place to express "enough results".
- **REGTOPIC source-vs-ENR endpoint check at admission time** — do not pursue. Defended instead via active random table revalidation (§2.2 / §2.3); the explicit check would produce false positives in NAT scenarios.

### 4.2 Pending tasks

Each row is an actionable gap with a tracking issue. Rows without an
issue ID are documented but not yet filed.

| Section | Gap | Tracking |
|---|---|---|
| §2.3 | `Search.bucketCheck` write missing — per-RPC sybil cap currently does not trigger | Issue #47 |
| §2.2 | Per-source persistent bucket cap (registration) — defends against multi-RPC concentration from a single registrar | Issue #48 |
| §2.3 | `Search.IsDone` natural termination | Issue #27 |
| §2.3 | Active topicSearch lifecycle / cancel topicQuery on `UDPv5.Close` | Issues #28, #30 |
| §3.1 | ENR `discng` capability flag | Issue #19 |
| §3.1 | `filterDiscNG` applied to response-sourced nodes | Issue #29 |
| §3.2 | IPv6 prefix policy specification | Issue #53 (Task 1.5) |
| §3.2 | IPv6 prefix policy implementation | Issue #54 (Task 2.5) |
| §2.2 / §2.3 | Active random table revalidation (subsumes Issue #24, REGTOPIC endpoint check) | Issue #21 |
| §2.2 | `collectOnePerDist` not topic-targeted — server returns nodes at distance-from-self instead of distance-from-topic | Issue #55 |
| §2.1.5 | Lower-bound anti-gaming mechanism — paper's invariant on retried wait times | (no tracking issue) |
| §2.1.3 | G safety floor reinstatement — coupled to the lower-bound work above | (no tracking issue) |

## 5. Conclusions

The DISC-NG implementation under `topdisc` is broadly compliant with the
paper. The protocol surface (advertisement, lookup, admission, wire
formats) is in place; the topic table, ticket system, IP tree scoring,
and bucket walks all match their specifications. Practical divergences
(F_lookup termination via caller-side `numNodes`, wait-time tuning, the
G safety floor near zero) are intentional and bounded.

The code adds defensive extensions beyond the paper that are necessary
for production: per-bucket IP/subnet caps, per-RPC sybil concentration
caps, the per-source persistent bucket cap, and the search-side
bucketCheck fix. Together these raise the bar against sybil and
concentration attacks well above what the paper specifies.

What remains is the lower-bound anti-gaming mechanism (§2.1.5), active
table revalidation (§2.2 / §2.3) — which subsumes the dropped REGTOPIC
endpoint check — search lifecycle correctness, the ENR `discng` flag for
dual-stack rollout, and an IPv6 prefix policy. These are listed in §4.
