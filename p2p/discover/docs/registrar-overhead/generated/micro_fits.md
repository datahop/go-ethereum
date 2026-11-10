| operation | fit range | intercept (ns) | slope (ns per entry) | R² |
|---|---|---|---|---|
| Register, fresh ad (wait ticket) vs topic size T | T ≤ 256 | 372 | 1.88 ± 0.05 | 0.995 |
| Register, fresh ad (wait ticket) vs topic size T | T ≤ 4096 | -139 | 5.64 ± 0.10 | 0.995 |
| Register, fresh ad vs occupancy N | N ≤ 10000 | 337 | 0.01 ± 0.00 | 0.640 |
| Register, duplicate ad vs T | T ≤ 256 | 11 | 1.54 ± 0.01 | 1.000 |
| Register, duplicate ad vs T | T ≤ 4096 | -546 | 5.59 ± 0.12 | 0.994 |
| Register, admitted retry vs T | T ≤ 256 | 1,679 | 9.25 ± 0.24 | 0.994 |
| Register, admitted retry vs T | T ≤ 4096 | 1,758 | 7.53 ± 0.04 | 0.999 |
| Register, admitted retry vs N | N ≤ 10000 | 1,596 | 0.03 ± 0.01 | 0.704 |
| RandomNodes(16) vs T | T ≤ 256 | 4 | 41.36 ± 0.20 | 1.000 |
| RandomNodes(16) vs T | T ≤ 4096 | -1,069 | 46.60 ± 2.26 | 0.961 |
| RandomNodes(16) vs N | N ≤ 10000 | 600 | 0.00 ± 0.00 | 0.081 |
| RemoveNode (absent ID) vs N | N ≤ 10000 | -1,736 | 6.04 ± 0.13 | 0.994 |
