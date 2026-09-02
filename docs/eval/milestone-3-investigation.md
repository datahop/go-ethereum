# Milestone 3 — Investigation Plan

Robustness and performance of topic discovery under realistic dynamics. Each
area lists the question, what to measure, and the lever or hypothesis to test.
Grounded in the 10k single-topic / 5-topic evaluations (see the reg-3 reports in
this directory) and the per-message traffic instrumentation.

## 1. Load imbalance across the ID space

**Question.** How concentrated is per-node traffic, and how does the
concentration scale with network size `N` and topic count?

**What we already know.** Registration ingress fans in on the ~16 registrars
closest to a topic (geometric ID distribution: only ~1–2 nodes exist inside
logdist 244). Received traffic at the topic center rises ~400× over the far
field (0.03 → 12.4 KB/s per node, ~60% of it REGTOPIC), while search traffic is
a roughly uniform background. The imbalance is registration-driven; total
volume is search-driven.

**Measure.**
- Peak/median and coefficient-of-variation (or Gini) of per-node received bytes
  vs logdist to topic.
- Number of nodes above a load threshold; peak KB/s at the center.
- Scaling of the peak with `N` (10k → 50k → 100k) and with topic count.

**Levers to evaluate.**
- `RegAttemptTimeout` — registrants abandon a congested registrar and relocate
  the ad to a farther, free-er one. Default is 1.5×AdLifetime (≈22.5 min at
  AdLifetime 15 min), so backoff effectively never fires. A/B at 1×AdLifetime.
- Per-registrant registration cap (bound total placements).
- Near-topic bucket sizing.
- Wait-time conservativeness (upper end), balanced against recall.

## 2. Coverage dips in the ID space (registration and search)

**Question.** Are there logdist bands that are under-registered, or that
searchers never reach?

**Hypothesis.** Scarce close buckets stay under-filled (few nodes exist there)
*and* are reached by few searchers — a double dip that caps recall. This is the
mechanism behind the observed reach-limited recall.

**Measure.**
- Per-logdist registrant count (ad-placement histogram).
- Per-logdist search-reach probability (fraction of searchers that query a node
  at each distance).
- Overlay the two to locate coverage holes and correlate with recall loss.

## 3. Load balance under high churn

**Question.** Does churn spread load (registrars rotate) or concentrate it
(survivors get hammered, plus renewal storms)?

**What we already know.** A steady-state 50/50 leave/join churn model exists;
dead-result rate is AdLifetime-bounded. Register-stagger fixed one renewal storm
(synchronized AdLifetime expiries).

**Measure.**
- Hotspot stability over time under churn (does the peak move or stay pinned?).
- Dead-result % and re-registration traffic volume.
- Whether ad relocation (lever from area 1) amplifies churn-driven traffic.
- Re-verification that register-stagger still suppresses renewal storms under
  churn.

## 4. Search effectiveness — new-node discovery rate (novelty, not duplicates)

**Question.** What is the novelty yield per query, and how fast does a searcher
converge to the full registrant set?

**What we already know.** Within a search session, results are de-duplicated
(`resultSeen`) and candidate registrars are de-duplicated (`contains`/`asked`);
`IsDone` stops the session once no unasked registrars remain. Recall therefore
climbs across *rollovers* (fresh sessions re-seeded from the routing table),
not by re-querying within a session.

**Measure.**
- Unique-found vs queries-issued curve; duplicate fraction per TOPICNODES.
- Marginal new registrants discovered per rollover.
- Time-to-90%-recall.

**Lever.** Intra-session persistence — keep querying farther-out registrars when
a round yields only duplicates, instead of stopping and rolling over. Compare
against the current stop-and-rollover behavior.

## 5. Wait-time formula validation under load

Does the paper's Eq. 1 produce sane wait times across the full occupancy /
contention range? Study the interplay of the lower bound (waiting-time floor)
and the upper bound (`RegAttemptTimeout`) — the window between them determines
how aggressively load sheds from hot registrars.

## 6. Adversarial load / Sybil

The ticket weaknesses (missing advertiser binding, no single-use enforcement)
let a Sybil inflate near-topic ingress. The imbalance study should include an
adversarial-registrant scenario to quantify how much a small Sybil set can
amplify the hotspot and degrade honest recall.

## 7. Multi-topic coupling

Under Zipf-distributed topic popularity, do popular topics starve unpopular ones
for the scarce near-center registrar slots? Measure cross-topic load
interference and per-topic recall as a function of topic popularity.

## 8. Overhead scaling

How do peak/median traffic and recall scale from 10k to 50k/100k nodes? Confirms
whether the imbalance and reach limits are constant-factor or grow with `N`.
