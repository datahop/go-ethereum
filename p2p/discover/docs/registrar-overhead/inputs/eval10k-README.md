# eval10k.csv: per-registrar load from the 10k-node network evaluation

Source branch: `origin/docs/overhead-eval-10k`
Commit: `16e7360fd456f2b94ed1fd32530d96be8e047e6a` ("docs/eval: add all-PRs 1-topic and 5-topic run reports", 2026-07-21)

## Files read

- `docs/eval/report-single-topic-reg3-10k.md`: single topic, reg-3/search-20 + #55. The overhead table comes from run `eval-overhead-10k-20260713-194637`.
- `docs/eval/report-5-topics-reg3-10k.md`: 5 topics with Zipf s=1.07 and spread topic IDs. The overhead table comes from run `eval-overhead-5top-10k-20260713-210832`.
- `docs/eval/report-allprs-1topic.md`: run `eval-allprs-1top-regto15m-20260720-140640`.
- `docs/eval/report-allprs-5topic.md`: run `eval-allprs-5top-regto15m-20260720-140640`.
- `docs/eval/milestone-3-investigation.md`: investigation plan. It gives a few summary numbers but does not say which run they come from.
- Two figures were viewed because no report states a per-registrar REGTOPIC request rate in numbers:
  `docs/eval/figures-allprs-1top/traf_allprs1_p1s_types_packets.png` and
  `docs/eval/figures-allprs-5top/traf_allprs5_p1s_types_packets.png`.
  Values in rows that cite these figures were **read by eye and are approximate**. Their note column marks them "APPROXIMATE".

`docs/eval` contains no CSV or JSON data files, only `.md` and `.png` files. The reports refer to `overhead.json` and `metrics.json` and to a traffic CSV ("CSV adds txP/rxP" in a commit on `integration/simnet-all-prs`). Those files sit in run directories on the "London" host and are not in the repo.

## Code revision used by the runs

- reg3 overhead runs (2026-07-13): "integration build = `topdisc` + #71/#81/#83/#84". No commit hash is given.
- all-PRs runs (2026-07-20 14:06:40): binary `integration/simnet-all-prs` = `topdisc` + #71, #81, #91, #92, #94 + testbed + per-message traffic instrumentation. No commit hash is given. The current head of `origin/integration/simnet-all-prs` is `7d61c6f8d15ad9a47d7055a2489e16c00613a80c` (2026-07-20 09:00 +0200), which predates the run. It is plausibly the build that was used, but the reports do not confirm this.

## Conventions

- `per_second` for cumulative counts = count / 4080 s. The reg3 reports say the run lasted "~68 min", so these conversions are approximate. The all-PRs reports state a 4 080 s window explicitly.
- Byte-rate rows give `per_second` in the row's own unit (KB/s or MB/s), not in requests/s.
- Overhead tables are binned by logdist to the topic. The 5-topic runs use each node's own topic. A row value is per node in that bin. The reports do not say whether it is a bin mean.
- No report gives an explicit median. The far bin (ld 256) holds about half the nodes, so it is the closest available stand-in for a "typical" registrar.

## MISSING (not in any source)

1. **No per-registrar REGTOPIC request rate in numbers.** Both reg3 reports say "Registrations received (REGTOPIC): Not yet captured." The all-PRs reports give only byte rates: the peak node receives 26.9 KB/s (1 topic) or 111.2 KB/s (5 topics), described as "registration-dominated" or "REGTOPIC-received ~111 KB/s". The packets/s values in the CSV were read off the figures and are approximate.
2. No p50/p90/p99/max distribution of REGTOPIC or TOPICQUERY rate across registrars. Only logdist-bin values are given, plus a network mean for TOPICQUERY and bytes.
3. No exact per-registrar TOPICQUERY rate for the all-PRs runs. Those reports give only a per-searcher send rate (0.381/s and 0.547/s) and the figures. Exact TOPICQUERY-received counts exist only for the reg3 overhead runs, as cumulative counts.
4. **No ad-cache occupancy** (ads cached per registrar or fraction of capacity). The reports say "Ads received: Not yet captured" and "derivable from `metrics.json` (`byHost`)". The only proxy is "distinct ads exposed / registrar" seen by 134 sampled searchers.
5. **No ad-cache capacity value.** `AdCacheSize` is named but never given a number.
6. No CPU, memory or latency figures per registrar, or anywhere else.
7. No commit hashes for the run builds.
8. No explicit reg-attempt-timeout for the reg3 runs. They presumably used the default of 1.5×AdLifetime.
9. The milestone-3 figures (center 12.4 KB/s, ~60% REGTOPIC, ~16 registrars) do not name the run they come from.
