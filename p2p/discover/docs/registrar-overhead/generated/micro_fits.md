| operation | fit range | intercept (ns) | slope (ns per entry) | R² |
|---|---|---|---|---|
| Register, fresh ad (wait ticket) vs topic size T | T ≤ 256 | 364 | 1.64 ± 0.04 | 0.995 |
| Register, fresh ad (wait ticket) vs topic size T | T ≤ 4096 | -164 | 5.20 ± 0.12 | 0.992 |
| Register, fresh ad vs occupancy N | N ≤ 10000 | 335 | 0.01 ± 0.00 | 0.494 |
| Register, duplicate ad vs T | T ≤ 256 | 7 | 1.64 ± 0.04 | 0.995 |
| Register, duplicate ad vs T | T ≤ 4096 | -561 | 5.32 ± 0.14 | 0.990 |
| Register, admitted retry vs T | T ≤ 256 | 1,817 | 6.16 ± 1.03 | 0.794 |
| Register, admitted retry vs T | T ≤ 4096 | 1,360 | 7.20 ± 0.16 | 0.992 |
| Register, admitted retry vs N | N ≤ 10000 | 1,843 | 0.02 ± 0.02 | 0.051 |
| RandomNodes(16) vs T | T ≤ 256 | -19 | 45.67 ± 0.74 | 0.997 |
| RandomNodes(16) vs T | T ≤ 4096 | -205 | 46.67 ± 0.59 | 0.997 |
| RandomNodes(16) vs N | N ≤ 10000 | 666 | 0.00 ± 0.00 | 0.064 |
| RemoveNode (absent ID) vs N | N ≤ 10000 | -1,948 | 6.37 ± 0.23 | 0.982 |
