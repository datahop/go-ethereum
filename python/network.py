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

        #command = 'sh -c "tc qdisc add dev eth0 root netem delay '+str(latency)+'ms"'
        #argv = ["docker","exec","node"+str(node),command]
        #p = subprocess.run(argv, capture_output=True, text=True)
        #if p.returncode != 0:
        #    print(p.stderr)
        #    p.check_returncode()

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

    if isinstance(network, NetworkDocker):
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
