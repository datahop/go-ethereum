| run | operation | calls | min µs | p50 µs | p99 µs | mean µs | queue delay p50 µs (all ops) | process CPU % of one core |
|---|---|---|---|---|---|---|---|---|
| C1000-fresh100-query1000-sleep | registration | 115997 | 0.33 | 6.29 | 26.83 | 6.62 | 162.80 | 3.5 |
| C1000-fresh100-query1000-sleep | query | 901189 | 0.00 | 1.83 | 23.42 | 3.82 | 162.80 | 3.5 |
| C1000-fresh100-query1000-spin | registration | 115999 | 0.33 | 1.17 | 7.25 | 1.43 | 0.16 | 99.9 |
| C1000-fresh100-query1000-spin | query | 901189 | 0.00 | 0.29 | 5.25 | 0.79 | 0.16 | 99.9 |
| C1000-fresh5-query20-sleep | registration | 6195 | 0.50 | 14.00 | 77.32 | 19.65 | 2,010.88 | 0.3 |
| C1000-fresh5-query20-sleep | query | 17980 | 0.00 | 2.46 | 47.34 | 6.41 | 2,010.88 | 0.3 |
| C1000-fresh5-query20-spin | registration | 6195 | 0.50 | 8.46 | 27.42 | 9.75 | 1.21 | 100.0 |
| C1000-fresh5-query20-spin | query | 17980 | 0.00 | 1.38 | 15.88 | 2.88 | 1.21 | 100.0 |
