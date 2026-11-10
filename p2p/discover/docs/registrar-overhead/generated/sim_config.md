| experiment | points | varied | fixed |
|---|---|---|---|
| abandon | 7 | fresh/s ∈ {1, 5, 20}; abandon ∈ {none, production, spec} | C = 1000; E = 15 min; query/s = 20; advertisers = 1000000; topics = 300; Zipf α = 1; IP dist. = empirical; arrivals = deterministic; duration = 36 h; warm-up = 24 h |
| bounded | 2 | fresh/s ∈ {5, 50} | C = 1000; E = 15 min; query/s = 20; advertisers = 200000; topics = 50000; Zipf α = 0; IP dist. = diverse; abandon = production; arrivals = deterministic; duration = 18 h; warm-up = 6 h |
| capacity | 10 | C ∈ {500, 1000, 2000, 5000, 10000}; fresh/s ∈ {5, 20} | E = 15 min; query/s = 20; advertisers = 200000; topics = 300; Zipf α = 1; IP dist. = empirical; abandon = production; arrivals = deterministic; duration = 18 h; warm-up = 6 h |
| demand | 18 | C ∈ {1000, 5000}; fresh/s ∈ {0.25, 0.5, 1, 2, 5, 10, 20, 50, 100} | E = 15 min; query/s = 20; advertisers = 200000; topics = 300; Zipf α = 1; IP dist. = empirical; abandon = production; arrivals = deterministic; duration = 18 h; warm-up = 6 h |
| ipdist | 3 | IP dist. ∈ {concentrated, diverse, empirical} | C = 1000; E = 15 min; fresh/s = 5; query/s = 20; advertisers = 200000; topics = 300; Zipf α = 1; abandon = production; arrivals = deterministic; duration = 18 h; warm-up = 6 h |
| query | 4 | query/s ∈ {1, 10, 100, 1000} | C = 1000; E = 15 min; fresh/s = 5; advertisers = 200000; topics = 300; Zipf α = 1; IP dist. = empirical; abandon = production; arrivals = deterministic; duration = 18 h; warm-up = 6 h |
| skew | 4 | Zipf α ∈ {0, 0.5, 1, 1.5} | C = 1000; E = 15 min; fresh/s = 5; query/s = 20; advertisers = 200000; topics = 300; IP dist. = empirical; abandon = production; arrivals = deterministic; duration = 18 h; warm-up = 6 h |
