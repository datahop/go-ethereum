| stage | point | clock | C | fresh/s | offered query/s | served req/s | mean ns/REGTOPIC | mean ns/TOPICQUERY | registrar busy (one core) | queue delay p50 µs | queue delay p99 µs | occupancy N/C | window min | contamination |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| a-query | C1000-fresh100-query1000 | hybrid | 1000 | 100 | 1,000 | 1,130 | 1,075 | 583 | 0.07% | 0.2 | 1.3 | 26.3% | 10.0 | load 3.5 |
| a-query | C1000-fresh100-query16000 | hybrid | 1000 | 100 | 16,000 | 16,137 | 1,020 | 362 | 0.59% | 0.1 | 0.8 | 26.3% | 10.0 | load 3.3 |
| a-query | C1000-fresh100-query160000 | hybrid | 1000 | 100 | 160,000 | 160,153 | 1,028 | 318 | 5.10% | 0.1 | 1.8 | 26.3% | 10.0 | no |
| a-query | C1000-fresh100-query320000 | hybrid | 1000 | 100 | 320,000 | 320,183 | 1,025 | 312 | 9.99% | 0.1 | 2.8 | 26.3% | 10.0 | contactsd 45% |
| a-query | C1000-fresh100-query4000 | hybrid | 1000 | 100 | 4,000 | 4,132 | 1,005 | 420 | 0.18% | 0.1 | 0.9 | 26.3% | 10.0 | no |
| a-query | C1000-fresh100-query64000 | hybrid | 1000 | 100 | 64,000 | 64,144 | 1,028 | 336 | 2.16% | 0.1 | 1.3 | 26.3% | 10.0 | no |
| a-query | C1000-fresh100-query640000 | hybrid | 1000 | 100 | 640,000 | 640,362 | 1,012 | 318 | 20.38% | 0.2 | 7.2 | 26.3% | 10.0 | no |
| b-registration | C1000-fresh1500-query1000 | hybrid | 1000 | 1,500 | 1,000 | 2,800 | 725 | 509 | 0.18% | 0.4 | 1,146.9 | 25.9% | 10.0 | load 3.1, load 3.8, load 5.6, load 6.0 |
| b-registration | C1000-fresh4000-query1000 | hybrid | 1000 | 4,000 | 1,000 | 5,896 | 1,052 | 531 | 0.57% | 0.4 | 11,010.0 | 25.2% | 10.0 | StocksWidget 31%, load 3.9 |
| b-registration | C1000-fresh500-query1000 | hybrid | 1000 | 500 | 1,000 | 1,644 | 823 | 603 | 0.11% | 0.2 | 262.1 | 26.4% | 10.0 | load 4.2 |
| c-profile | C1000-fresh100-query640000 | hybrid | 1000 | 100 | 640,000 | 640,363 | 1,117 | 317 | 20.31% | 0.2 | 7.6 | 26.3% | 10.0 | log 88% |
| d-capacity | C5000-fresh100-query16000 | hybrid | 5000 | 100 | 16,000 | 16,142 | 1,435 | 1,758 | 2.83% | 0.1 | 4.0 | 25.9% | 10.0 | load 3.0 |
| d-capacity | C5000-fresh100-query640000 | hybrid | 5000 | 100 | 640,000 | 620,471 | 1,147 | 1,125 | 69.83% | 25,769,803.8 | 30,601,642.0 | 17.8% | 10.0 | no |
| d-validation | C1000-fresh100-query16000-hybrid | hybrid | 1000 | 100 | 16,000 | 16,137 | 1,008 | 352 | 0.58% | 0.1 | 0.8 | 26.3% | 10.0 | load 3.1 |
| d-validation | C1000-fresh100-query16000-realtime | realtime | 1000 | 100 | 16,000 | 16,105 | 1,126 | 295 | 0.48% | 0.1 | 0.7 | 25.7% | 10.0 | Mail 61%, contactsd 86%, mds_stores 66% |
