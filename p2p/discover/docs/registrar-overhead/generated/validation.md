| build | benchmark | ns/op | B/op | allocs/op |
|---|---|---|---|---|
| cgo | ValidationENR/size=max | 19,644.2 ± 159.4 | 4,334 | 93.0 |
| cgo | ValidationENR/size=typical | 19,017.9 ± 27.9 | 3,925 | 87.0 |
| cgo | ValidationEndpoint | 116.9 ± 0.4 | 108 | 5.0 |
| cgo | ValidationTicket/op=pack | 363.4 ± 0.4 | 368 | 4.0 |
| cgo | ValidationTicket/op=replay-check | 49.7 ± 0.4 | 128 | 0.0 |
| cgo | ValidationTicket/op=unpack | 415.5 ± 0.8 | 240 | 3.0 |
| purego | ValidationENR/size=max | 88,640.5 ± 51.1 | 5,716 | 117.0 |
| purego | ValidationENR/size=typical | 88,445.0 ± 592.1 | 5,291 | 111.0 |
| purego | ValidationEndpoint | 114.5 ± 0.8 | 108 | 5.0 |
| purego | ValidationTicket/op=pack | 359.9 ± 0.3 | 368 | 4.0 |
| purego | ValidationTicket/op=replay-check | 47.7 ± 0.2 | 128 | 0.0 |
| purego | ValidationTicket/op=unpack | 423.2 ± 1.2 | 240 | 3.0 |
