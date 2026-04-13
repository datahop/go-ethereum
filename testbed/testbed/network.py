import json
import os
import os.path
import random
import subprocess
import time
import glob

import asyncio
from asyncio.subprocess import create_subprocess_exec

# This is the list of config parameters supported by cmd/devp2p.
run_param = {
    'adCacheSize',
    'adLifetimeSeconds',
    'regBucketSize',
    'regBucketStandbySize',
    'searchBucketSize',
    'bucketRefreshInterval',
    'rpcSearchTimeoutSeconds',
}


network_config_defaults = {
    'rpcBasePort': 20200,
    'udpBasePort': 30200,
}

MIN_LATENCY=2
MAX_LATENCY=40

# Safe first octets for virtual public IPs (non-private, non-reserved).
SAFE_OCTETS = [45, 51, 64, 78, 85, 91, 104, 138, 155, 163, 185, 193, 203, 212]

def public_ip_for_node(node: int) -> str:
    """Generate a virtual public IP scattered across different /8, /16, /24 ranges."""
    i = node - 1  # node is 1-based
    first = SAFE_OCTETS[i % len(SAFE_OCTETS)]
    second = (i // len(SAFE_OCTETS)) + 1
    third = (i % 250) + 1
    return f"{first}.{second}.{third}.1"

def private_ip_for_node(node: int) -> str:
    """Private IP on the Docker bridge network."""
    return f"10.100.{node // 256}.{node % 256}"

class Network:
    config: dict = {}

    def __init__(self, config=network_config_defaults):
        assert isinstance(config, dict)
        assert isinstance(config.get('rpcBasePort'), int)
        assert isinstance(config.get('udpBasePort'), int)
        self.config = config

    def build(self):
        print('Compiling devp2p tool')
        result = os.system("go build ./cmd/devp2p")
        assert(result == 0)

    def stop(self):
        pass

    def node_udp_endpoint(self, node: int):
        """Returns the UDP endpoint of a node."""
        raise NotImplementedError

    def node_api_url(self, node: int):
        """Returns the URL of the RPC server of a node."""
        raise NotImplementedError

    def start_node(self, node: int, bootnodes=[], nodekey=None, config_path=None):
        """Spawns a node.

        The 'nodekey' and 'config_path' keyword arguments are required.
        """
        raise NotImplementedError


class NetworkLocal(Network):
    proc: list[subprocess.Popen] = []

    node_env = os.environ.copy()
    node_env['GOMAXPROCS'] = '1'

    def node_udp_endpoint(self, node: int):
        port = self.config['udpBasePort'] + node
        return ('127.0.0.1', port)

    # node_api_url returns the RPC URL of node n.
    def node_api_url(self, n):
        port = self.config['rpcBasePort'] + n
        #print("port:"+str(port))
        url = 'http://127.0.0.1:' + str(port)
        return url

    def start_node(self, node: int, bootnodes=[], nodekey=None, config_path=None):
        assert nodekey is not None
        assert config_path is not None

        port = self.config['udpBasePort'] + node
        rpc = self.config['rpcBasePort'] + node
        nodeflags = [
            "--bootnodes", ','.join(bootnodes),
            "--nodekey", nodekey,
            "--addr", "127.0.0.1:"+str(port),
            "--rpc", "127.0.0.1:"+str(rpc),
            "--config", os.path.join(config_path, "config.json"),
        ]
        logfile = os.path.join(config_path, "logs", "node-"+str(node)+".log")
        logflags = ["--verbosity", "5", "--log.json", "--log.file", logfile]
        argv = ["./devp2p", *logflags, "discv5", "listen", *nodeflags]

        print("Starting node", str(node))
        p = subprocess.Popen(argv, stdout=subprocess.DEVNULL, stderr=None, env=self.node_env)
        self.proc.append(p)

    def stop(self):
        super().stop()

        if self.proc: print('Stopping network')
        for p in self.proc:
            p.terminate()
        for p in self.proc:
            p.wait()
        self.proc = []


class NetworkDocker(Network):
    containers: list[str] = []
    networks: list[str] = []

    def build(self):
        super().build()

        # remove stuff from previous runs
        #os.system('docker network prune -f')
        #os.system('docker container prune -f')

        # print("Building docker container")
        #result = os.system("docker build --tag devp2p -f Dockerfile.topdisc .")
        #assert(result == 0)

    def stop(self):
        super().stop()

        print("Stopping docker containers")
        for container_id in self.containers:
            os.system('docker kill ' + container_id)
            os.system('docker rm ' + container_id)
        self.containers = []

        for network_id in self.networks:
            os.system('docker network rm ' + network_id)
        self.networks = []

    def node_udp_endpoint(self, node: int):
        ip = self._node_ip(node)
        port = self.config['udpBasePort']
        return (ip, port)

    # node_api_url returns the RPC URL of node n.
    def node_api_url(self, n):
        port = self.config['rpcBasePort']
        ip = self._node_ip(n)
        return "http://" + ip + ":" + str(port)

    def start_node(self, node: int, bootnodes=[], nodekey=None, config_path=None):
        assert nodekey is not None
        assert config_path is not None

        # create node command line
        port = self.config['udpBasePort']
        rpc = self.config['rpcBasePort']
        ip = self._node_ip(node)
        nodeflags = [
            "--bootnodes", ','.join(bootnodes),
            "--nodekey", nodekey,
            "--addr", ip+':'+str(port),
            "--rpc", ip+':'+str(rpc),
            "--config", "/go-ethereum/discv5-test/config.json",
        ]
        logfile = "/go-ethereum/discv5-test/logs/node-"+str(node)+".log"
        logflags = ["--verbosity", "5", "--log.json", "--log.file", logfile]
        node_args = [*logflags, "discv5", "listen", *nodeflags]

        # start the docker container
        argv = [
            "docker", "run", "-d",
            "--name", "node"+str(node),
            "--network", self._node_network_name(node),
            "--cap-add", "NET_ADMIN",
            "--mount", "type=bind,source="+config_path+",target=/go-ethereum/discv5-test",
            "devp2p", *node_args,
        ]
        p = subprocess.run(argv, capture_output=True, text=True)
        if p.returncode != 0:
            print(p.stderr)
            p.check_returncode()

        container_id = p.stdout.split('\n')[0]
        print('Started node', node, 'container:', container_id)
        self.containers.append(container_id)
        #self._config_network(n)

    def create_docker_networks(self, n: int):
        for node in range(1, n+1):
            network = self._node_network_name(node)
            prefix = self._node_ip_prefix(node)
            subnet = prefix + '.0/24'
            gateway = prefix+'.1'
            argv = [
                'docker', 'network', 'create', network,
                '-d', 'bridge',
                '--subnet=' + subnet,
                '--gateway=' + gateway,
            ]
            p = subprocess.run(argv, capture_output=True, text=True)
            if p.returncode != 0:
                print('docker network create failed:')
                print(p.stderr)
            else:
                self.networks.append(p.stdout.split('\n')[0])

    def _node_network_name(self, node: int):
        return 'node' + str(node) + '-network'

    def _node_ip(self, node: int):
        return self._node_ip_prefix(node) + '.2'

    def _node_ip_prefix(self, node: int):
        IP1=172
        IP2=20
        IP3=0
        IP3=IP3+node-1
        while IP3 > 255:
            IP3=IP3-256
            IP2=IP2+1
        while IP2 > 255:
            IP2=IP2-256
            IP1=IP1+1

        ip=str(IP1)+"."+str(IP2)+"."+str(IP3)
        return ip

    def config_network(self, node: int):
        latency = random.randint(MIN_LATENCY,MAX_LATENCY)
        subprocess.Popen("docker exec node"+str(node)+" sh -c 'tc qdisc add dev eth0 root netem delay "+str(latency)+"ms'", stdout=subprocess.DEVNULL, stderr=None,shell=True)


class NetworkDockerRouter(Network):
    """Docker-based network with a single router container that simulates
    diverse public IPs and NAT behaviors using iptables.

    All nodes are on a single Docker bridge (10.100.0.0/16). The router
    assigns virtual public IPs from scattered ranges and applies per-node
    NAT rules (public, natted, port-forwarded).
    """

    DOCKER_NETWORK = 'discv5-testnet'
    DOCKER_SUBNET = '10.100.0.0/16'
    ROUTER_IP = '10.100.0.1'
    DISCV5_PORT = 30303
    RPC_PORT = 20200
    PROJECT = 'discv5'

    containers: list[str] = []
    node_types: dict[int, str] = {}  # node -> 'public'|'natted'|'port-forwarded'

    def __init__(self, config=None):
        cfg = {
            'rpcBasePort': self.RPC_PORT,
            'udpBasePort': self.DISCV5_PORT,
        }
        if config:
            cfg.update(config)
        super().__init__(cfg)

    def build(self):
        # Build the local devp2p binary (needed for key generation and ENR creation).
        print('Compiling devp2p tool')
        result = os.system("PATH=$PATH:/usr/local/go/bin go build ./cmd/devp2p")
        assert result == 0, "Failed to build devp2p"

        print('Building Docker images...')
        result = os.system(
            "docker build -t discv5-node -f testbed/docker/node/Dockerfile ."
        )
        assert result == 0, "Failed to build node image"
        result = os.system(
            "docker build -t discv5-router -f testbed/docker/router/Dockerfile ."
        )
        assert result == 0, "Failed to build router image"

    def classify_nodes(self, params: dict):
        """Assign node types based on config parameters."""
        n = params['nodes']
        natted = params.get('nattedNodes', 0)
        forwarded = params.get('portForwardedNodes', 0)
        public = n - natted - forwarded

        assert public >= 0, "nattedNodes + portForwardedNodes > nodes"

        self.node_types = {}
        node = 1
        for _ in range(public):
            self.node_types[node] = 'public'
            node += 1
        for _ in range(forwarded):
            self.node_types[node] = 'port-forwarded'
            node += 1
        for _ in range(natted):
            self.node_types[node] = 'natted'
            node += 1

    def node_udp_endpoint(self, node: int):
        """Returns the virtual public IP and port for a node."""
        return (public_ip_for_node(node), self.DISCV5_PORT)

    def node_api_url(self, node: int):
        """Returns the RPC URL using the private (Docker bridge) IP."""
        priv_ip = private_ip_for_node(node)
        return f"http://{priv_ip}:{self.RPC_PORT}"

    def _create_network(self):
        """Create the Docker bridge network."""
        # Remove existing network if present.
        os.system(f"docker network rm {self.DOCKER_NETWORK} 2>/dev/null")
        argv = [
            'docker', 'network', 'create',
            '--subnet', self.DOCKER_SUBNET,
            '--gateway', self.ROUTER_IP,
            self.DOCKER_NETWORK,
        ]
        p = subprocess.run(argv, capture_output=True, text=True)
        if p.returncode != 0:
            print('docker network create failed:', p.stderr)
            p.check_returncode()

    def _write_mappings(self, config_path: str, n: int):
        """Write the node_mappings.txt file for the router."""
        mappings_file = os.path.join(config_path, "node_mappings.txt")
        with open(mappings_file, 'w') as f:
            f.write("# private_ip public_ip type\n")
            for node in range(1, n + 1):
                priv_ip = private_ip_for_node(node)
                pub_ip = public_ip_for_node(node)
                ntype = self.node_types.get(node, 'public')
                f.write(f"{priv_ip} {pub_ip} {ntype}\n")
        return mappings_file

    def _start_router(self, config_path: str):
        """Start the router container."""
        abs_config_path = os.path.abspath(config_path)
        argv = [
            'docker', 'run', '-d',
            '--name', f'{self.PROJECT}-router',
            '--network', self.DOCKER_NETWORK,
            '--ip', self.ROUTER_IP,
            '--cap-add', 'NET_ADMIN',
            '--mount', f'type=bind,source={abs_config_path},target=/config',
            'discv5-router',
        ]
        p = subprocess.run(argv, capture_output=True, text=True)
        if p.returncode != 0:
            print('Router start failed:', p.stderr)
            p.check_returncode()

        container_id = p.stdout.strip()
        print(f'Started router: {container_id[:12]}')
        self.containers.append(container_id)

        # Wait for router to be ready.
        import time
        time.sleep(2)

    def start_node(self, node: int, bootnodes=[], nodekey=None, config_path=None):
        assert nodekey is not None
        assert config_path is not None

        abs_config_path = os.path.abspath(config_path)
        priv_ip = private_ip_for_node(node)
        pub_ip = public_ip_for_node(node)

        nodeflags = [
            "--bootnodes", ','.join(bootnodes),
            "--nodekey", nodekey,
            "--addr", f"{priv_ip}:{self.DISCV5_PORT}",
            "--rpc", f"{priv_ip}:{self.RPC_PORT}",
            "--config", "/config/config.json",
        ]
        logfile = f"/config/logs/node-{node}.log"
        logflags = ["--verbosity", "5", "--log.json", "--log.file", logfile]

        argv = [
            'docker', 'run', '-d',
            '--name', f'{self.PROJECT}-node-{node}',
            '--network', self.DOCKER_NETWORK,
            '--ip', priv_ip,
            '--cap-add', 'NET_ADMIN',
            '-e', f'GATEWAY={self.ROUTER_IP}',
            '--mount', f'type=bind,source={abs_config_path},target=/config',
            'discv5-node',
            'devp2p', *logflags, 'discv5', 'listen', *nodeflags,
        ]
        p = subprocess.run(argv, capture_output=True, text=True)
        if p.returncode != 0:
            print(f'Node {node} start failed:', p.stderr)
            p.check_returncode()

        container_id = p.stdout.strip()
        ntype = self.node_types.get(node, 'public')
        print(f'Started node {node} ({ntype}): {priv_ip} -> {pub_ip}')
        self.containers.append(container_id)

    def stop(self):
        super().stop()
        if self.containers:
            print('Stopping containers...')
        for cid in self.containers:
            os.system(f'docker rm -f {cid} 2>/dev/null')
        self.containers = []
        os.system(f'docker network rm {self.DOCKER_NETWORK} 2>/dev/null')


# _async_iter_concurrently runs fn over items. The function must return a
# coroutine, which will be scheduled as a task.
def _async_iter_concurrently(items, fn, concurrency=os.cpu_count()):
    assert asyncio.iscoroutinefunction(fn)

    tasks = set()
    async def wait_and_check_exn(tasks, **kwargs):
        done, tasks = await asyncio.wait(tasks, **kwargs)
        for t in done:
            if t.exception():
                raise t.exception()

    async def iterate():
        for item in items:
            if len(tasks) >= concurrency:
                await wait_and_check_exn(tasks, return_when=asyncio.FIRST_COMPLETED)
            t = asyncio.create_task(fn(item))
            tasks.add(t)
        # wait for all remaining tasks to finish
        await wait_and_check_exn(tasks)

    asyncio.run(iterate())

# _call_process runs the given command and returns its output.
# The process must exit with code zero.
async def _call_process(cmd: str, *args):
    proc = await create_subprocess_exec(cmd, *args, stdout=asyncio.subprocess.PIPE)
    output, _ = await proc.communicate()
    if proc.returncode != 0:
        raise Exception(cmd + ' exited with non-zero code ' + str(proc.returncode))
    return output.decode('utf-8')


# create_nodeid_index writes a node_id -> node index file in the keys directory.
def create_nodeid_index(config_path: str) -> dict[str, int]:
    keys_dir = os.path.join(config_path, "keys")
    if not os.path.isdir(keys_dir):
        raise FileNotFoundError("keys/ directory does not exist: " + keys_dir)

    # This generator returns all key files in keys_dir:
    def key_files():
        for file in glob.glob(os.path.join(keys_dir, "node-*.key")):
            node = int(os.path.basename(file).split('-')[1].split('.')[0])
            yield (node, file)

    # Create the index by invoking the devp2p tool for each key file.
    index = {}
    async def key_to_id(tuple):
        node, keyfile = tuple
        output = await _call_process("./devp2p", "key", "to-id", keyfile)
        node_id = output.split('\n')[0]
        index[node_id] = node

    _async_iter_concurrently(key_files(), key_to_id)

    # Write the index.
    index_file = os.path.join(keys_dir, "node_id_index.json")
    with open(index_file, 'w') as f:
        json.dump(index, f)
        f.write("\n")
    return index

# load_nodeid_index reads the node_id->node index file or creates
# it when it is not present.
def load_nodeid_index(config_path) -> dict[str, int]:
    keys_dir = os.path.join(config_path, "keys")
    index_file = os.path.join(keys_dir, "node_id_index.json")

    if not os.path.isfile(index_file):
        print('Missing node ID index, creating it now...')
        return create_nodeid_index(config_path)

    with open(index_file, 'r') as f:
        return json.load(f)

# create_enrs turns node key files into ENRs.
def create_enrs(network: Network, config_path: str, n: int):
    result = []

    async def key_to_enr(node: int):
        keyfile = os.path.join(config_path, 'keys', 'node-{}.key'.format(node))
        ip, port = network.node_udp_endpoint(node)
        args = ['key', 'to-enr', '--ip', ip, '--udp', str(port), keyfile]
        output = await _call_process('./devp2p', *args)
        result.append(output.split('\n')[0])

    _async_iter_concurrently(range(1, n+1), key_to_enr)
    return result

# make_keys creates n node keys.
def make_keys(config_path: str, n: int):
    keys_dir = os.path.join(config_path, "keys")
    os.makedirs(keys_dir, exist_ok=True)

    async def generate_key(node: int):
        file = os.path.join(keys_dir, "node-" + str(node) + ".key")
        await _call_process('./devp2p', 'key', 'generate', file)

    _async_iter_concurrently(range(1, n+1), generate_key)


def select_bootnodes(enrs):
    return [ enrs[0] ] + random.sample(enrs[1:], min(len(enrs)//3, 20))

def load_ip_list(filepath: str) -> list[str]:
    """Load public IPs from a file (one IP per line)."""
    ips = []
    with open(filepath) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#'):
                ips.append(line)
    return ips


def load_latency_list(filepath: str) -> list[int]:
    """Load per-node latencies from a file (one value in ms per line)."""
    latencies = []
    with open(filepath) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#'):
                latencies.append(int(line))
    return latencies


def generate_latencies(n: int, params: dict) -> list[int]:
    """Generate latencies per node from config (random uniform or from file)."""
    latency_file = params.get('latencyFile')
    if latency_file and os.path.isfile(latency_file):
        latencies = load_latency_list(latency_file)
        if len(latencies) < n:
            # Cycle if file has fewer entries than nodes.
            latencies = (latencies * ((n // len(latencies)) + 1))[:n]
        return latencies[:n]

    min_lat = params.get('minLatencyMs', MIN_LATENCY)
    max_lat = params.get('maxLatencyMs', MAX_LATENCY)
    return [random.randint(min_lat, max_lat) for _ in range(n)]


def start_nodes(network: Network, config_path: str, params: dict):
    n = params['nodes']

    # For NetworkDockerRouter, classify nodes and set up IP mappings.
    if isinstance(network, NetworkDockerRouter):
        network.classify_nodes(params)

        # Load or generate public IPs.
        ip_file = params.get('publicIpFile')
        if ip_file and os.path.isfile(ip_file):
            custom_ips = load_ip_list(ip_file)
            if len(custom_ips) < n:
                raise ValueError(f"IP file has {len(custom_ips)} IPs but need {n}")
            # Override the IP function with custom IPs.
            ip_map = {i+1: custom_ips[i] for i in range(n)}
            original_endpoint = network.node_udp_endpoint
            network.node_udp_endpoint = lambda node: (ip_map[node], network.DISCV5_PORT)

    print("Building keys...")
    make_keys(config_path, params['nodes'])

    print("Creating ENRs...")
    enrs = create_enrs(network, config_path, n)

    print("Creating node ID index...")
    create_nodeid_index(config_path)

    print("Starting", n, "nodes...")

    if isinstance(network, NetworkDockerRouter):
        network._create_network()
        network._write_mappings(config_path, n)
        network._start_router(config_path)
    elif isinstance(network, NetworkDocker):
        network.create_docker_networks(n)
        os.system("sudo iptables --flush DOCKER-ISOLATION-STAGE-1")

    for i in range(1, n+1):
        keyfile = os.path.join(config_path, "keys", "node-"+str(i)+".key")
        with open(keyfile, "r") as f:
            nodekey = f.read()
        bn = select_bootnodes(enrs)
        network.start_node(i, bootnodes=bn, nodekey=nodekey, config_path=config_path)

        if isinstance(network, NetworkDocker):
            network.config_network(i)

    # Apply latencies.
    if isinstance(network, NetworkDockerRouter):
        latencies = generate_latencies(n, params)
        for i in range(1, n+1):
            lat = latencies[i-1]
            if lat > 0:
                container = f'{network.PROJECT}-node-{i}'
                os.system(
                    f"docker exec {container} sh -c "
                    f"'tc qdisc add dev eth0 root netem delay {lat}ms' 2>/dev/null"
                )
        print(f"Applied latencies: min={min(latencies)}ms max={max(latencies)}ms")

    print("Nodes started")

def filter_params(params):
    result={}
    for param in params:
        if(param in run_param):
            result[param]=params[param]
    return result

def write_experiment(config_path, params):
    logs_dir = os.path.join(config_path, "logs")
    if os.path.exists(logs_dir):
        print("Removing old logs...")
        for filename in os.listdir(logs_dir):
            os.remove(os.path.join(logs_dir, filename))
    else:
        os.mkdir(logs_dir)

    node_config = filter_params(params)
    print("Experiment parameters:", params)
    # print("Node config:", node_config)
    with open(config_path+'config.json', 'w') as f:
        f.write(json.dumps(node_config))
    with open(config_path+'experiment.json', 'w') as f:
        f.write(json.dumps(params))

def run_testbed(network: Network, config_path, params):
    network.build()
    write_experiment(config_path, params)
    start_nodes(network, config_path, params)
