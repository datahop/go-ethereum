# discv5 testbed

### Installing Dependencies

You need Python >= 3.9.

It is recommended to use virtualenv to keep your Python package collection in check.
Create your environment like this:

```
python3 -m venv .virtualenv
```

Now enter the environment (assuming a bash-like shell). This sets up your PATH and
Python-related environment variables to store everything in `.virtualenv`.

```
source .virtalenv/bin/activate
```

Now install the dependencies:

```
python -m pip install -r requirements.txt
```

### Running Experiments

All commands are to be executed from the go-ethereum project root directory. If you are
using virtualenv, remember to enter the environment first.

You can run the testbed with default settings like this:

```
testbed/run.py
```

There are several customization options available. You can run a single experiment with
custom configuration using the command below. Check out discv5-stdconfig.json for an
overview of the available parameters.

```
testbed/run.py --config myconfig.json
```

By default, `run.py` will perform analysis after running the experiment. This can be
disabled using the `--no-analysis` flag.

Logs and analysis outputs will be written to a subdirectoy of `discv5_test_logs/` named
after the experiment parameters. You can override the name using the `--name` flag.

If you want to re-run analysis for a past experiment, use the following command:

```
testbed/analyse.py ./discv5_test_logs/<experiment>
```

### Running modes

By default, all nodes run as local processes on `127.0.0.1` (`NetworkLocal`).
Fast, no Docker needed. IP diversity is not exercised because every node shares
localhost.

#### Docker mode

Each node runs in its own Docker container with a configurable IP, optional
bandwidth cap, and optional latency. There is no NAT and no router — each
container literally has the IP it advertises on a single bridge network.

IPs are laid out across distinct `/24`s (node N → `10.100.N.1`) so that the
discv5 per-bucket IP-subnet cap (`regBucketSubnet=24`) is exercised meaningfully.

Requires Linux with Docker (the `tc` shaping inside containers needs Linux
kernel features that macOS Docker Desktop does not support).

```
testbed/run.py --docker --config testbed/discv5-docker-config.json --name my-test
```

##### Docker config knobs

In addition to the standard config parameters, the Docker mode supports:

| Parameter | Description | Default |
|-----------|-------------|---------|
| `minLatencyMs` / `maxLatencyMs` | Per-node egress latency, drawn uniformly from this range. Omit both for no latency shaping. | unset |
| `minBandwidthMbit` / `maxBandwidthMbit` | Per-node egress bandwidth cap (Mbit/s), drawn uniformly. Omit both for no bandwidth shaping. | unset |

Each node's per-run latency and bandwidth are sampled once at start and applied
inside the container via `tc htb` + `tc netem` rules on `eth0`.
