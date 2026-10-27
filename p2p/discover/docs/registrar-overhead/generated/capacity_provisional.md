| source | point | registrations/s | queries/s | mean ns/registration | mean ns/query | registrar busy (one core) | implied requests/s at 100% busy (same mix) |
|---|---|---|---|---|---|---|---|
| simulated | C1000-fresh100 | 137.1 | 20.0 | 1,188 | 1,149 | 0.0186% | 845,603 |
| simulated | C1000-fresh5-query1000 | 7.1 | 1,000.0 | 3,069 | 840 | 0.0862% | 1,168,259 |
| realtime-spin | C1000-fresh100-query1000-spin | 128.9 | 1,001.3 | 1,429 | 791 | 0.0976% | 1,157,693 |
| realtime-spin | C1000-fresh5-query20-spin | 6.9 | 20.0 | 9,746 | 2,878 | 0.0125% | 215,628 |
