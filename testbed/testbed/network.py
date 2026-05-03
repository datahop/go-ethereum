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

class Network:
    config: dict = {}

    def __init__(self, config=network_config_defaults):
        assert isinstance(config, dict)
        assert isinstance(config.get('rpcBasePort'), int)
        assert isinstance(config.get('udpBasePort'), int)
        self.config = config
        self.params: dict = {}

    def build(self):
        print('Compiling devp2p tool')
        result = os.system("go build ./cmd/devp2p")
        assert(result == 0)

    def stop(self):
        pass

    def prepare(self, params: dict):
        """Receives the experiment params before nodes are started.

        Subclasses can override this to capture per-run configuration
        (bandwidth, latency, etc.) needed during start_node.
        """
        self.params = params

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
    """Docker-based testbed: each node runs in its own container with a
    configurable IP, bandwidth, and latency. No NAT, no router — every
    container literally has the IP it advertises on a single bridge network.

    IPs are laid out so that each node falls in a distinct /24 (node N -> 10.100.N.1),
    exercising the discv5 per-bucket IP-subnet cap (regBucketSubnet=24).
    Per-node bandwidth and latency are applied via `tc` inside each container.
    """

    DOCKER_NETWORK = 'discv5-testnet'
    DOCKER_SUBNET = '10.100.0.0/16'
    DOCKER_IMAGE = 'discv5-testbed-node'

    def __init__(self, config=network_config_defaults):
        super().__init__(config)
        self.containers: list[str] = []
        self.network_created: bool = False

    def build(self):
        # Remove any leftover containers/network from a previous failed run.
        os.system('docker ps -aq --filter "name=discv5-node-" | xargs -r docker rm -f >/dev/null 2>&1 || true')
        os.system(f'docker network rm {self.DOCKER_NETWORK} >/dev/null 2>&1 || true')

        print(f'Building node image: {self.DOCKER_IMAGE}')
        result = os.system(
            f'docker build -t {self.DOCKER_IMAGE} '
            f'-f testbed/docker/node/Dockerfile .'
        )
        assert result == 0

    def stop(self):
        super().stop()
        if self.containers:
            print('Stopping containers')
            for cid in self.containers:
                os.system('docker kill ' + cid + ' >/dev/null 2>&1 || true')
                os.system('docker rm ' + cid + ' >/dev/null 2>&1 || true')
            self.containers = []
        if self.network_created:
            os.system(f'docker network rm {self.DOCKER_NETWORK} >/dev/null 2>&1 || true')
            self.network_created = False

    def _create_network(self):
        argv = [
            'docker', 'network', 'create',
            '--driver', 'bridge',
            '--subnet', self.DOCKER_SUBNET,
            self.DOCKER_NETWORK,
        ]
        p = subprocess.run(argv, capture_output=True, text=True)
        if p.returncode != 0:
            print(p.stderr)
            p.check_returncode()
        self.network_created = True

    def _node_ip(self, node: int) -> str:
        # /24-diverse layout: node N -> 10.100.N.1 (N in 0..255).
        if node > 255:
            raise ValueError(f"node index {node} exceeds /24 layout (max 256)")
        return f'10.100.{node}.1'

    def node_udp_endpoint(self, node: int):
        return (self._node_ip(node), self.config['udpBasePort'])

    def node_api_url(self, node: int):
        return f'http://{self._node_ip(node)}:{self.config["rpcBasePort"]}'

    def _node_latency_ms(self, node: int):
        lo = self.params.get('minLatencyMs')
        hi = self.params.get('maxLatencyMs')
        if lo is None and hi is None:
            return None
        if lo is None: lo = hi
        if hi is None: hi = lo
        return random.randint(int(lo), int(hi))

    def _node_bw_mbit(self, node: int):
        lo = self.params.get('minBandwidthMbit')
        hi = self.params.get('maxBandwidthMbit')
        if lo is None and hi is None:
            return None
        if lo is None: lo = hi
        if hi is None: hi = lo
        return random.randint(int(lo), int(hi))

    def start_node(self, node: int, bootnodes=[], nodekey=None, config_path=None):
        assert nodekey is not None
        assert config_path is not None
        if not self.network_created:
            self._create_network()

        ip = self._node_ip(node)
        port = self.config['udpBasePort']
        rpc = self.config['rpcBasePort']

        nodeflags = [
            '--bootnodes', ','.join(bootnodes),
            '--nodekey', nodekey,
            '--addr', f'{ip}:{port}',
            '--rpc', f'{ip}:{rpc}',
            '--config', '/work/config.json',
        ]
        logfile = '/work/logs/node-' + str(node) + '.log'
        logflags = ['--verbosity', '5', '--log.json', '--log.file', logfile]
        node_args = [*logflags, 'discv5', 'listen', *nodeflags]

        env = []
        latency = self._node_latency_ms(node)
        if latency is not None:
            env += ['-e', f'NODE_LATENCY_MS={latency}']
        bw = self._node_bw_mbit(node)
        if bw is not None:
            env += ['-e', f'NODE_BW_MBIT={bw}']

        argv = [
            'docker', 'run', '-d',
            '--name', f'discv5-node-{node}',
            '--network', self.DOCKER_NETWORK,
            '--ip', ip,
            '--cap-add', 'NET_ADMIN',
            '--mount', f'type=bind,source={config_path},target=/work',
            *env,
            self.DOCKER_IMAGE, *node_args,
        ]
        p = subprocess.run(argv, capture_output=True, text=True)
        if p.returncode != 0:
            print(p.stderr)
            p.check_returncode()

        cid = p.stdout.split('\n')[0]
        print(f'Started node {node} ({ip}) container {cid[:12]}')
        self.containers.append(cid)

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

def start_nodes(network: Network, config_path: str, params: dict):
    n = params['nodes']

    print("Building keys...")
    make_keys(config_path, params['nodes'])

    print("Creating ENRs...")
    enrs = create_enrs(network, config_path, n)

    print("Creating node ID index...")
    create_nodeid_index(config_path)

    print("Starting", n, "nodes...")
    network.prepare(params)

    for i in range(1, n+1):
        keyfile = os.path.join(config_path, "keys", "node-"+str(i)+".key")
        with open(keyfile, "r") as f:
            nodekey = f.read()
        bn = select_bootnodes(enrs)
        network.start_node(i, bootnodes=bn, nodekey=nodekey, config_path=config_path)

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
