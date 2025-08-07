#!/usr/bin/env python3
"""
Comprehensive distributed test runner for Raft implementation.
Tests all combinations of specified parameters and manages Docker containers on remote servers.
"""

import argparse
import itertools
import time
import os
import sys
from datetime import datetime
from typing import List, Dict, Any
import logging
import signal

try:
    import paramiko
except ImportError:
    print("Paramiko library is not installed. Please install it with 'pip install paramiko'")
    sys.exit(1)

try:
    from dotenv import load_dotenv
except ImportError:
    print("python-dotenv library is not installed. Please install it with 'pip install python-dotenv'")
    sys.exit(1)

# Server configuration
SERVERS = {
    # "zs01": "10.10.2.181",
    "zs02": "10.10.2.182",
    "zs03": "10.10.2.183",
    "zs04": "10.10.2.184",
    "zs05": "10.10.2.185",
    # "zs06": "10.10.2.186",
    "zs07": "10.10.2.187",
    "zs08": "10.10.2.188",
}

class RaftDistributedTestRunner:
    def __init__(self, test_suite_name: str = None, timeout: int = 70, retries: int = 1, repetitions: int = 3):
        self.test_suite_name = test_suite_name or f"Raft Distributed Test Suite {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        self.total_tests = 0
        self.results = []
        self.interrupted = False
        self.timeout = timeout
        self.retries = retries
        self.repetitions = repetitions
        self.ssh_clients = {}

        # Setup logging
        os.makedirs("test-output", exist_ok=True)
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(f'test-output/raft_distributed_test_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

        # Setup signal handling for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """Handle interruption signals gracefully"""
        self.logger.info("Received interruption signal. Cleaning up...")
        self.interrupted = True
        self._cleanup_containers_on_all_servers()
        self._disconnect_ssh()
        sys.exit(0)

    def _connect_ssh(self, server_name: str):
        """Establish an SSH connection to a server, respecting the user's SSH config."""
        if server_name not in self.ssh_clients:
            try:
                ssh_config = paramiko.SSHConfig()
                user_config_file = os.path.expanduser("~/.ssh/config")
                if os.path.exists(user_config_file):
                    with open(user_config_file) as f:
                        ssh_config.parse(f)

                host_config = ssh_config.lookup(server_name)

                client = paramiko.SSHClient()
                client.load_system_host_keys()
                client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

                connect_kwargs = {
                    'hostname': host_config.get('hostname', server_name),
                    'username': host_config.get('user'),
                    'key_filename': host_config.get('identityfile'),
                }
                if 'port' in host_config:
                    connect_kwargs['port'] = int(host_config['port'])

                client.connect(**connect_kwargs)
                self.ssh_clients[server_name] = client
                self.logger.info(f"Connected to {server_name}")
            except Exception as e:
                self.logger.error(f"Failed to connect to {server_name}: {e}")
                raise

    def _disconnect_ssh(self):
        """Disconnect all SSH clients."""
        for server_name, client in self.ssh_clients.items():
            try:
                client.close()
                self.logger.info(f"Disconnected from {server_name}")
            except Exception as e:
                self.logger.warning(f"Error disconnecting from {server_name}: {e}")
        self.ssh_clients = {}

    def _execute_remote_command(self, server_name: str, command: str):
        """Execute a command on a remote server."""
        if server_name not in self.ssh_clients:
            self._connect_ssh(server_name)

        client = self.ssh_clients[server_name]
        try:
            stdin, stdout, stderr = client.exec_command(command)
            exit_code = stdout.channel.recv_exit_status()
            output = stdout.read().decode('utf-8').strip()
            error = stderr.read().decode('utf-8').strip()
            if exit_code != 0:
                self.logger.error(f"Command failed on {server_name}: {command}")
                self.logger.error(f"Exit code: {exit_code}")
                self.logger.error(f"Stdout: {output}")
                self.logger.error(f"Stderr: {error}")
            return output, error, exit_code
        except Exception as e:
            self.logger.error(f"Exception executing command on {server_name}: {e}")
            return None, str(e), -1

    def _cleanup_containers_on_all_servers(self):
        """Clean up containers on all servers."""
        self.logger.info("Cleaning up containers on all servers...")
        for server_name in SERVERS.keys():
            self._execute_remote_command(server_name, "docker stop -t 0 $(docker ps -a -q --filter 'name=raft*') || true")
            self._execute_remote_command(server_name, "docker rm $(docker ps -a -q --filter 'name=raft*') || true")

    def _pull_docker_images(self, images: List[str], client_image: str):
        """Pull Docker images on all servers."""
        self.logger.info("Pulling Docker images on all servers...")
        all_images = set(images)
        all_images.add(client_image)

        for server_name in SERVERS.keys():
            for image in all_images:
                self.logger.info(f"Pulling image {image} on {server_name}...")
                output, error, exit_code = self._execute_remote_command(server_name, f"docker pull {image}")
                if exit_code != 0:
                    self.logger.error(f"Failed to pull image {image} on {server_name}")

    def generate_test_combinations(self,
                                 images: List[str],
                                 compaction_thresholds: List[int],
                                 peer_counts: List[int],
                                 operation_counts: List[int],
                                 concurrency_levels: List[int]) -> List[Dict[str, Any]]:
        """Generate all combinations of test parameters"""
        combinations = []

        for image, threshold, peers, operations, concurrency in itertools.product(
            images, compaction_thresholds, peer_counts, operation_counts, concurrency_levels
        ):
            if peers > len(SERVERS):
                self.logger.warning(f"Skipping test with {peers} peers because there are not enough servers.")
                continue

            # For raftswift implementation, test with different combinations of flags
            if 'raftswift' in image.lower():
                # Test with both flags off
                combinations.append({
                    'image': image,
                    'compaction_threshold': threshold,
                    'peers': peers,
                    'operations': operations,
                    'concurrency': concurrency,
                    'distributed_actor_system': False,
                    'manual_locks': False
                })
                # Test with distributed_actor_system on, manual_locks off
                combinations.append({
                    'image': image,
                    'compaction_threshold': threshold,
                    'peers': peers,
                    'operations': operations,
                    'concurrency': concurrency,
                    'distributed_actor_system': True,
                    'manual_locks': False
                })
                # Test with distributed_actor_system off, manual_locks on
                combinations.append({
                    'image': image,
                    'compaction_threshold': threshold,
                    'peers': peers,
                    'operations': operations,
                    'concurrency': concurrency,
                    'distributed_actor_system': False,
                    'manual_locks': True
                })
                # Test with both flags on
                combinations.append({
                    'image': image,
                    'compaction_threshold': threshold,
                    'peers': peers,
                    'operations': operations,
                    'concurrency': concurrency,
                    'distributed_actor_system': True,
                    'manual_locks': True
                })
            else:
                # For other implementations, don't use any special flags
                combinations.append({
                    'image': image,
                    'compaction_threshold': threshold,
                    'peers': peers,
                    'operations': operations,
                    'concurrency': concurrency,
                    'distributed_actor_system': False,
                    'manual_locks': False
                })

        return combinations

    def _run_single_test(self, test_number: int, params: Dict[str, Any], client_image: str) -> Dict[str, Any]:
        """Run a single test configuration with repetitions."""
        test_name = f"Test {test_number}/{self.total_tests}"
        self.logger.info(f"Starting {test_name} with {self.repetitions} repetitions")
        self.logger.info(f"Parameters: {params}")

        # Assign servers
        server_names = list(SERVERS.keys())
        node_servers = server_names[:params['peers']]
        client_server = node_servers[-1] # Co-locate client with the last node

        peers_config = ",".join([f"{i+1}:{SERVERS[server_name]}:50051" for i, server_name in enumerate(node_servers)])

        repetition_results = []
        start_time = time.time()
        first_repetition_failed = False

        try:
            # Start node containers
            for i, server_name in enumerate(node_servers):
                node_id = i + 1
                # Build the base command
                command = f"docker run -d --name raft_node_{node_id} --network host {params['image']} peer --id {node_id} --port 50051 --address {SERVERS[server_name]} --peers '{peers_config}' --compaction-threshold {params['compaction_threshold']}"

                # Add distributed actor system flag if enabled
                if params.get('distributed_actor_system'):
                    command += " --use-distributed-actor-system"
                # Add manual locks flag if enabled
                if params.get('manual_locks'):
                    command += " --use-manual-lock"
                self.logger.info(f"Starting node {node_id} on {server_name} with command: {command}")
                self._execute_remote_command(server_name, command)

            time.sleep(4) # Give nodes time to elect a leader

            for i in range(self.repetitions):
                rep_name = f"{test_name} Rep {i+1}/{self.repetitions}"
                self.logger.info(f"Starting {rep_name}")

                # Start client
                env_vars = {
                    "STRESS_TEST_BASE_URL": os.environ.get("STRESS_TEST_BASE_URL"),
                    "STRESS_TEST_API_KEY": os.environ.get("STRESS_TEST_API_KEY"),
                    "STRESS_TEST_MACHINE_NAME": os.environ.get("STRESS_TEST_MACHINE_NAME"),
                }
                env_flags = " ".join([f"-e {key}={value}" for key, value in env_vars.items() if value])

                client_command = f"docker run --name raft_client --network host {env_flags} {client_image} client --peers {peers_config} --stress-test --operations {params['operations']} --concurrency {params['concurrency']} --test-suite '{self.test_suite_name}'"

                # Add distributed actor system flag if enabled
                if params.get('distributed_actor_system'):
                    client_command += " --use-distributed-actor-system"

                self.logger.info(f"Starting client on {client_server} with command: {client_command}")
                self._execute_remote_command(client_server, client_command)

                status = self._wait_for_client_completion(client_server, self.timeout)

                logs, _, _ = self._execute_remote_command(client_server, "docker logs raft_client")

                rep_result = {'repetition': i+1, 'status': status, 'logs': logs}
                repetition_results.append(rep_result)

                self._execute_remote_command(client_server, "docker rm raft_client")

                if status != 'SUCCESS':
                    if i == 0: # First run failed
                        self.logger.error(f"{rep_name} failed. Aborting all repetitions for this test.")
                        first_repetition_failed = True
                        break
                    else:
                        self.logger.warning(f"{rep_name} failed. Continuing with next repetition.")

        except Exception as e:
            self.logger.error(f"Error during test execution: {e}")
            return {
                'test_number': test_number, 'parameters': params, 'success': False,
                'duration': time.time() - start_time, 'error': str(e),
                'repetitions': repetition_results, 'timestamp': datetime.now().isoformat()
            }
        finally:
            for server_name in node_servers:
                self._execute_remote_command(server_name, "docker stop -t 0 $(docker ps -a -q --filter 'name=raft_node*') || true")
                self._execute_remote_command(server_name, "docker rm $(docker ps -a -q --filter 'name=raft_node*') || true")
            self._execute_remote_command(client_server, "docker stop -t 0 raft_client || true")
            self._execute_remote_command(client_server, "docker rm raft_client || true")


        overall_success = all(r['status'] == 'SUCCESS' for r in repetition_results)
        result = {
            'test_number': test_number, 'parameters': params, 'success': overall_success,
            'duration': time.time() - start_time, 'repetitions': repetition_results,
            'timestamp': datetime.now().isoformat()
        }
        if first_repetition_failed:
            result['error'] = "First repetition failed."
        return result

    def _wait_for_client_completion(self, server_name: str, timeout: int) -> str:
        """Wait for the client container to complete and return 'SUCCESS', 'FAILED', 'TIMEOUT', or 'INTERRUPTED'."""
        start_time = time.time()

        while time.time() - start_time < timeout:
            if self.interrupted:
                return "INTERRUPTED"

            output, _, exit_code = self._execute_remote_command(server_name, "docker inspect -f '{{.State.Status}}' raft_client")
            if exit_code == 0 and output == 'exited':
                _, _, exit_code_container = self._execute_remote_command(server_name, "docker inspect -f '{{.State.ExitCode}}' raft_client")
                self.logger.info(f"Client container exited with code: {exit_code_container}")
                return "SUCCESS" if exit_code_container == 0 else "FAILED"

            time.sleep(2)

        self.logger.error("Timeout waiting for client completion")
        return "TIMEOUT"

    def run_tests(self,
                  images: List[str],
                  compaction_thresholds: List[int],
                  peer_counts: List[int],
                  operation_counts: List[int],
                  concurrency_levels: List[int],
                  client_image: str):
        combinations = self.generate_test_combinations(
            images, compaction_thresholds, peer_counts, operation_counts,
            concurrency_levels
        )
        self.total_tests = len(combinations)
        self.logger.info(f"Starting distributed test suite: {self.test_suite_name}")
        self.logger.info(f"Total tests to run: {self.total_tests}")

        self._pull_docker_images(images, client_image)
        self._cleanup_containers_on_all_servers()

        test_number = 0
        for params in combinations:
            test_number += 1
            if self.interrupted:
                break

            for attempt in range(self.retries):
                result = self._run_single_test(test_number, params, client_image)

                if 'error' not in result or "First repetition failed." not in result['error']:
                    break

                if attempt < self.retries - 1:
                    self.logger.warning(f"Test {test_number} failed on attempt {attempt + 1}/{self.retries} because first repetition failed. Retrying...")
                    time.sleep(5)
                else:
                    self.logger.error(f"Test {test_number} failed on all {self.retries} attempts because first repetition failed.")

            self.results.append(result)
            time.sleep(2)

        self._disconnect_ssh()
        return self.results

    def generate_report(self, output_file: str = None):
        if not output_file:
            output_file = f"test-output/raft_distributed_test_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"

        os.makedirs(os.path.dirname(output_file), exist_ok=True)

        with open(output_file, 'w') as f:
            f.write(f"Raft Distributed Test Suite Report: {self.test_suite_name}\n")
            f.write(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")

            successful_tests = [r for r in self.results if r['success']]
            failed_tests = [r for r in self.results if not r['success']]

            f.write(f"Summary: {len(successful_tests)} / {self.total_tests} tests passed.\n\n")

            if failed_tests:
                f.write("--- FAILED TESTS ---\n")
                for result in failed_tests:
                    f.write(f"Test {result['test_number']}: FAILED\n")
                    f.write(f"  Parameters: {result['parameters']}\n")
                    f.write(f"  Duration: {result['duration']:.2f}s\n")
                    if 'error' in result:
                        f.write(f"  Error: {result['error']}\n")
                    for rep in result['repetitions']:
                        f.write(f"    Repetition {rep['repetition']}: {rep['status']}\n")
                    f.write("\n")

            if successful_tests:
                f.write("--- PASSED TESTS ---\n")
                for result in successful_tests:
                    f.write(f"Test {result['test_number']}: PASSED\n")
                    f.write(f"  Parameters: {result['parameters']}\n")
                    f.write(f"  Duration: {result['duration']:.2f}s\n")
                    for rep in result['repetitions']:
                        f.write(f"    Repetition {rep['repetition']}: {rep['status']}\n")
                    f.write("\n")

        self.logger.info(f"Report generated at {output_file}")


def main():
    load_dotenv(dotenv_path=".env.distributed")
    parser = argparse.ArgumentParser(description="Run comprehensive distributed Raft implementation tests")
    parser.add_argument("--images", nargs="+", type=str, required=True)
    parser.add_argument("--client-image", type=str, default="registry.niklabs.de/niklhut/raftswift:latest")
    parser.add_argument("--compaction-thresholds", nargs="+", type=int, default=[1000])
    parser.add_argument("--peer-counts", nargs="+", type=int, default=[3])
    parser.add_argument("--operation-counts", nargs="+", type=int, default=[10000])
    parser.add_argument("--concurrency-levels", nargs="+", type=int, default=[2])
    parser.add_argument("--test-suite", type=str)
    parser.add_argument("--report", type=str)
    parser.add_argument("--timeout", type=int, default=180)
    parser.add_argument("--retries", type=int, default=3)
    parser.add_argument("--repetitions", type=int, default=3)
    args = parser.parse_args()

    runner = RaftDistributedTestRunner(
        test_suite_name=args.test_suite,
        timeout=args.timeout,
        retries=args.retries,
        repetitions=args.repetitions
    )

    try:
        runner.run_tests(
            images=args.images,
            compaction_thresholds=args.compaction_thresholds,
            peer_counts=args.peer_counts,
            operation_counts=args.operation_counts,
            concurrency_levels=args.concurrency_levels,
            client_image=args.client_image
        )
        runner.generate_report(args.report)
    except KeyboardInterrupt:
        print("\nTest run interrupted by user")
    finally:
        runner._cleanup_containers_on_all_servers()
        runner._disconnect_ssh()

if __name__ == "__main__":
    main()
