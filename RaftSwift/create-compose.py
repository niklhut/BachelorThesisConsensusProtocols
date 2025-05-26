import argparse
import yaml

def generate_compose(num_peers, use_actors, image):
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
            "--peers", peer_peers
        ]
        if use_actors:
            peer_command.append("--use-distributed-actor-system")

        services[container_name] = {
            "image": image,
            "container_name": container_name,
            "networks": [network_name],
            "command": peer_command
        }

    # Client node
    raw_client_cmd = [
        "./Raft", "client",
        "--peers", peers,
        "--stress-test",
        "--operations", "20000"
    ]
    if use_actors:
        raw_client_cmd.append("--use-distributed-actor-system")

    # Join the command into a single shell string with a sleep
    client_shell_cmd = f"sleep 5 && {' '.join(raw_client_cmd)}"

    services["raft_client"] = {
        "image": image,
        "container_name": "raft_client",
        "depends_on": [f"raft_node_{i}" for i in range(1, num_peers + 1)],
        "networks": [network_name],
        "entrypoint": ["sh", "-c", client_shell_cmd]
    }

    compose = {
        "version": "3.8",
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
    parser.add_argument("--actors", action="store_true", help="Use distributed actor system")
    parser.add_argument("--output", type=str, default="docker-compose.yml", help="Output YAML file")
    parser.add_argument("--image", type=str, default="docker.niklabs.de/niklhut/raft-swift:latest", help="Docker image")
    args = parser.parse_args()

    compose_yaml = generate_compose(args.peers, args.actors, args.image)

    with open(args.output, "w") as f:
        yaml.dump(compose_yaml, f, default_flow_style=False, sort_keys=False)

    print(f"Generated {args.output} for {args.peers} peers with actors={args.actors}")

if __name__ == "__main__":
    main()
