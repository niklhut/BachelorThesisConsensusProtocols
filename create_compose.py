import argparse
import yaml
from datetime import datetime
import shlex

def generate_compose(
       image: str,
       client_image: str,
       num_peers: int,
       operations: int,
       concurrency: int,
       compaction_threshold: int,
       test_suite: str = None,
       client_start_delay: int = 5,
       distributed_actor_system: bool = False,
       manual_locks: bool = False,
       collect_metrics: bool = False
   ):
    services = {}
    network_name = "raftnet"
    peers = ",".join(
        f"{i}:raft_node_{i}:100{str(i).zfill(2)}" for i in range(1, num_peers + 1)
    )

    # Peer nodes
    for i in range(1, num_peers + 1):
        port = f"100{str(i).zfill(2)}"  # e.g., 10001
        container_name = f"raft_node_{i}"

        peer_peers = ",".join(
            f"{j}:raft_node_{j}:100{str(j).zfill(2)}"
            for j in range(1, num_peers + 1) if j != i
        )

        peer_command = [
            "peer",
            "--id", str(i),
            "--port", port,
            "--address", container_name,
            "--peers", peer_peers,
            "--compaction-threshold", str(compaction_threshold),
        ]
        if collect_metrics:
            peer_command.append("--collect-metrics")
        if distributed_actor_system:
            peer_command.append("--use-distributed-actor-system")
        if manual_locks:
            peer_command.append("--use-manual-lock")

        services[container_name] = {
            "image": image,
            "container_name": container_name,
            "networks": [network_name],
            "command": peer_command
        }

    # Client node
    raw_client_cmd = [
        "./Raft", "client",
        "--peers", str(peers),
        "--stress-test",
        "--operations", str(operations),
        "--concurrency", str(concurrency),
    ]
    if test_suite:
        raw_client_cmd.extend(["--test-suite", test_suite])
    if distributed_actor_system:
        raw_client_cmd.append("--use-distributed-actor-system")

    # Join the command into a single shell string with a sleep
    quoted_cmd = ' '.join(shlex.quote(arg) for arg in raw_client_cmd)
    if client_start_delay > 0:
        client_shell_cmd = f"sleep {client_start_delay} && {quoted_cmd}"
    else:
        client_shell_cmd = quoted_cmd


    services["raft_client"] = {
        "image": client_image,
        "container_name": "raft_client",
        "environment": [
            "STRESS_TEST_BASE_URL=${STRESS_TEST_BASE_URL}",
            "STRESS_TEST_API_KEY=${STRESS_TEST_API_KEY}",
            "STRESS_TEST_MACHINE_NAME=${STRESS_TEST_MACHINE_NAME}",
        ],
        "depends_on": [f"raft_node_{i}" for i in range(1, num_peers + 1)],
        "networks": [network_name],
        "entrypoint": ["sh", "-c", client_shell_cmd]
    }

    compose = {
        "services": services,
        "networks": {
            network_name: {
                "driver": "bridge"
            }
        }
    }

    return compose

def main():
    parser = argparse.ArgumentParser(description="Generate Docker Compose for Raft Cluster")
    parser.add_argument("--peers", type=int, required=True, help="Number of Raft peer nodes")
    parser.add_argument("--distributed-actor-system", action="store_true", help="Use distributed actor system (raftswift only)")
    parser.add_argument("--manual-locks", action="store_true", help="Use manual locks instead of actors (raftswift only)")
    parser.add_argument("--output", type=str, default="docker-compose.yml", help="Output YAML file")
    parser.add_argument("--image", type=str, default="docker.niklabs.de/niklhut/raft-swift:latest", help="Docker image")
    parser.add_argument("--client-image", type=str, default="docker.niklabs.de/niklhut/raft-swift:latest", help="Docker image")
    parser.add_argument("--operations", type=int, default=20000, help="Number of operations to perform")
    parser.add_argument("--concurrency", type=int, default=10, help="Concurrency level")
    parser.add_argument("--compaction-threshold", type=int, default=1000, help="Compaction threshold")
    default_test_suite = f"Test Suite {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    parser.add_argument("--test-suite", type=str, default=default_test_suite, help=f"Name of the test suite to run (default: {default_test_suite})")
    parser.add_argument("--client-start-delay", type=int, default=5, help="Delay in seconds before starting the client (default: 5)")
    parser.add_argument("--collect-metrics", action="store_true", help="Collect metrics during the stress test")
    args = parser.parse_args()

    compose_yaml = generate_compose(
        image=args.image,
        client_image=args.client_image,
        num_peers=args.peers,
        operations=args.operations,
        concurrency=args.concurrency,
        compaction_threshold=args.compaction_threshold,
        test_suite=args.test_suite,
        client_start_delay=args.client_start_delay,
        distributed_actor_system=args.distributed_actor_system,
        manual_locks=args.manual_locks,
        collect_metrics=args.collect_metrics
    )

    with open(args.output, "w") as f:
        yaml.dump(compose_yaml, f, default_flow_style=False, sort_keys=False)

    print("Generated configuration:")
    print(f"  Output file: {args.output}")
    print(f"  Number of peers: {args.peers}")
    if 'raftswift' in args.image.lower():
        print(f"  Use distributed actor system: {args.distributed_actor_system}")
        print(f"  Use manual locks: {args.manual_locks}")
    print(f"  Operations: {args.operations}")
    print(f"  Concurrency: {args.concurrency}")
    print(f"  Compaction threshold: {args.compaction_threshold}")
    if args.test_suite:
        print(f"  Test suite: {args.test_suite}")
    print(f"  Client start delay: {args.client_start_delay}s")


if __name__ == "__main__":
    main()