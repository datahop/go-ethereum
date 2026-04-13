# discv5 DISC-NG testbed

Testbed for running DISC-NG topic discovery experiments.

## Modes

### Local (default)

All nodes run as local processes on `127.0.0.1`. Fast, no Docker needed.
IP diversity scoring is not exercised (all nodes share localhost).

```bash
testbed/run.py --config testbed/discv5-smallconfig.json --name my-test
```

### Docker (with router-based NAT simulation)

Nodes run in Docker containers with a single router container that:
- Assigns virtual public IPs from scattered ranges (different /8, /16, /24)
- Simulates NAT behavior per node (public, NATted, port-forwarded)
- Applies per-node network latency via `tc netem`

Requires Linux with Docker (not macOS Docker Desktop).

```bash
testbed/run.py --docker --config testbed/discv5-docker-config.json --name my-test
```

## Installing Dependencies

Python >= 3.9 required.

```bash
python3 -m venv testbed/.venv
source testbed/.venv/bin/activate
pip install -r testbed/requirements.txt
```

## Configuration

All commands run from the go-ethereum project root directory.

### Common parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `nodes` | Total number of nodes | 500 |
| `topic` | Number of distinct topics | 1 |
| `searchIterations` | Searches per node | 5 |
| `returnedNodes` | Max results per search | 30 |

### Node parameters (passed to devp2p)

| Parameter | Description | Default |
|-----------|-------------|---------|
| `pingInterval` | Liveness check interval (seconds) | 10 |
| `bucketRefreshInterval` | Kademlia bucket refresh (seconds) | 60 |
| `adCacheSize` | Topic table capacity | 500 |
| `adLifetimeSeconds` | Ad expiry time (seconds) | 60 |
| `regBucketSize` | Active registrations per bucket | 10 |
| `regBucketStandbySize` | Standby registrations per bucket | 10 |
| `searchBucketSize` | Nodes per search bucket | 5 |
| `rpcSearchTimeoutSeconds` | RPC search timeout (seconds) | 120 |

### Docker NAT parameters (only with `--docker`)

| Parameter | Description | Default |
|-----------|-------------|---------|
| `nattedNodes` | Nodes behind NAT (unreachable) | 0 |
| `portForwardedNodes` | NATted nodes with port forwarding (reachable on discv5 port) | 0 |
| `minLatencyMs` | Minimum per-node latency (ms) | 2 |
| `maxLatencyMs` | Maximum per-node latency (ms) | 40 |
| `publicIpFile` | File with custom public IPs (one per line, optional) | generated |
| `latencyFile` | File with per-node latencies in ms (one per line, optional) | random uniform |

Node assignment order: public nodes first, then port-forwarded, then NATted.
Remaining `nodes - nattedNodes - portForwardedNodes` are public.

### Example configs

```bash
# 5 nodes, local, quick test
testbed/run.py --config testbed/discv5-smallconfig.json --name quick-test

# 100 nodes, Docker, mixed NAT (50 public + 20 port-forwarded + 30 NATted)
testbed/run.py --docker --config testbed/discv5-docker-config.json --name nat-test

# 500 nodes, local, standard benchmark
testbed/run.py --config testbed/discv5-stdconfig.json --name benchmark
```

## Running experiments

```bash
# Run with analysis (default)
testbed/run.py --config <config.json> --name <experiment-name>

# Run without analysis
testbed/run.py --config <config.json> --name <experiment-name> --no-analysis

# Re-run analysis on existing logs
testbed/analyse.py ./discv5_test_logs/<experiment-name>
```

## Results

Results are written to `discv5_test_logs/<name>/`:

```
discv5_test_logs/<name>/
  config.json          Node-level config
  experiment.json      Full experiment parameters
  node_mappings.txt    IP mappings and NAT types (Docker mode)
  keys/                Node keys and ID index
  logs/
    node-*.log         Per-node JSON logs
    logs.json          Workload events (registrations + searches)
  dfs/                 Parsed dataframes (JSON)
  figs/                Analysis plots (PDF)
```

### Generated plots

| Plot | Description |
|------|-------------|
| `discovered_search.pdf` | Avg lookup results per node |
| `times_registered.pdf` | Successful registrations per node |
| `times_registered_dist.pdf` | Registration distribution across DHT |
| `operation_time.pdf` | Search times by topic |
| `waiting_time.pdf` | Admission control waiting times by topic |
| `msg_type_count.pdf` | Protocol message type counts |
| `times_discovered.pdf` | How often each node appears in results |
| `op_returned.pdf` | Results returned per operation |

## Docker architecture

```
Single Docker bridge: 10.100.0.0/16

Router (10.100.0.1)
  - Assigns virtual public IPs from scattered /8 ranges
  - Per-node iptables rules:
    * public:         SNAT + full DNAT (all inbound accepted)
    * port-forwarded: SNAT + DNAT on UDP 30303 only
    * natted:         SNAT only (no inbound except responses)

Nodes (10.100.0.x)
  - Default gateway set to router
  - All traffic goes through router's NAT rules
  - ENRs advertise virtual public IPs
```
