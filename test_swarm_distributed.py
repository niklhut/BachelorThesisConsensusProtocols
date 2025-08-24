#!/usr/bin/env python3
import argparse
import itertools
import os
import subprocess
import sys
import time
from datetime import datetime
import logging
import signal
import random

from dotenv import load_dotenv

# Node mapping (hostname to IP)
SERVERS = {
    "zs03.lab.dm.informatik.tu-darmstadt.de": "10.10.2.183",
    "zs04.lab.dm.informatik.tu-darmstadt.de": "10.10.2.184",
    "zs05.lab.dm.informatik.tu-darmstadt.de": "10.10.2.185",
    "zs07.lab.dm.informatik.tu-darmstadt.de": "10.10.2.187",
    "zs02.lab.dm.informatik.tu-darmstadt.de": "10.10.2.182",
    "zs08.lab.dm.informatik.tu-darmstadt.de": "10.10.2.188",
    "zs01.lab.dm.informatik.tu-darmstadt.de": "10.10.2.181",
}

SERVICE_PREFIX = "nhuthmann_"

class RaftSwarmTestRunner:
    def __init__(self, test_suite_name=None, timeout=180, retries=1, repetitions=3):
        self.test_suite_name = test_suite_name or f"Raft Swarm Test Suite {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        self.total_tests = 0
        self.results = []
        self.interrupted = False
        self.timeout = timeout
        self.retries = retries
        self.repetitions = repetitions

        os.makedirs("test-output", exist_ok=True)
        logging.basicConfig(
            level=logging.DEBUG,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[
                logging.FileHandler(f'test-output/raft_swarm_test_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        self.logger.info("Received interruption signal. Cleaning up...")
        self.interrupted = True
        self._remove_all_services()
        sys.exit(0)

    def _run_cmd(self, cmd: str, check=True, background=False):
        self.logger.debug(f"Running command: {cmd}")
        if background:
            # Start process but don't wait
            proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            return proc
        else:
            result = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            if check and result.returncode != 0:
                raise RuntimeError(f"Command failed: {cmd}\n{result.stderr.decode()}")
            return result.stdout.decode().strip()

    def _remove_all_services(self):
        self.logger.info("Removing all Raft-related services with prefix...")
        try:
            services = self._run_cmd("docker service ls --format '{{.Name}}'")
            for svc in services.splitlines():
                if svc.startswith(f"{SERVICE_PREFIX}raft_node") or svc.startswith(f"{SERVICE_PREFIX}raft_client") or svc.startswith(f"{SERVICE_PREFIX}pull_"):
                    self._run_cmd(f"docker service rm {svc}", check=False)
        except Exception as e:
            self.logger.warning(f"Error removing services: {e}")

    def generate_test_combinations(self, images, compaction_thresholds, peer_counts, operation_counts, concurrency_levels):
        combinations = []
        for image, threshold, peers, operations, concurrency in itertools.product(
            images, compaction_thresholds, peer_counts, operation_counts, concurrency_levels
        ):
            if peers > len(SERVERS):
                self.logger.warning(f"Skipping test with {peers} peers — not enough servers.")
                continue

            if 'raftswift' in image.lower():
                # Four permutations of the flags
                combinations.extend([
                    {
                        'image': image,
                        'compaction_threshold': threshold,
                        'peers': peers,
                        'operations': operations,
                        'concurrency': concurrency,
                        'distributed_actor_system': False,
                        'manual_locks': False
                    },
                    {
                        'image': image,
                        'compaction_threshold': threshold,
                        'peers': peers,
                        'operations': operations,
                        'concurrency': concurrency,
                        'distributed_actor_system': True,
                        'manual_locks': False
                    },
                    {
                        'image': image,
                        'compaction_threshold': threshold,
                        'peers': peers,
                        'operations': operations,
                        'concurrency': concurrency,
                        'distributed_actor_system': False,
                        'manual_locks': True
                    },
                    {
                        'image': image,
                        'compaction_threshold': threshold,
                        'peers': peers,
                        'operations': operations,
                        'concurrency': concurrency,
                        'distributed_actor_system': True,
                        'manual_locks': True
                    }
                ])
            else:
                # Non-swift images get just one combination, no flags
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

    def _start_nodes(self, params, node_servers, port):
        procs = []
        for i, hostname in enumerate(node_servers):
            node_id = i + 1
            svc_name = f"{SERVICE_PREFIX}raft_node_{node_id}"

            flags = ""
            if params.get('distributed_actor_system'):
                flags += " --use-distributed-actor-system"
            if params.get('manual_locks'):
                flags += " --use-manual-lock"

            peers_config = ",".join([
                f"{idx+1}:{SERVERS[peer_host]}:{port}"
                for idx, peer_host in enumerate(node_servers)
                if peer_host != hostname
            ])

            cmd = (
                f"docker service create --with-registry-auth "
                f"--name {svc_name} "
                f"--network host --restart-condition none "
                f"--constraint 'node.hostname=={hostname}' "
                f"{params['image']} peer --id {node_id} --port {port} "
                f"--address {SERVERS[hostname]} --peers '{peers_config}' "
                f"--compaction-threshold {params['compaction_threshold']}{flags}"
            )
            procs.append(self._run_cmd(cmd, check=False, background=True))

        time.sleep(5)

    def _run_single_test(self, test_number, params, client_image):
        test_name = f"Test {test_number}/{self.total_tests}"
        self.logger.info(f"Starting {test_name} - Params: {params}")

        node_servers = list(SERVERS.keys())[:params['peers']]
        client_server = node_servers[-1]

        start_time = time.time()
        repetition_results = []

        port = random.randint(50000, 50100)

        try:
            # Start nodes before first repetition
            self._start_nodes(params, node_servers, port)

            for r in range(self.repetitions):
                rep_name = f"{test_name} Rep {r+1}/{self.repetitions}"
                self.logger.info(f"Starting {rep_name}")

                attempt = 0
                status = None

                while attempt < self.retries:
                    attempt += 1
                    self.logger.info(f"Repetition {r+1}, Attempt {attempt}/{self.retries}")

                    if r == 0 and attempt > 1:
                        # Restart everything for first repetition retry
                        self.logger.info("Restarting ALL services for first repetition retry...")
                        self._remove_all_services()
                        port = random.randint(50000, 50100)
                        self._start_nodes(params, node_servers, port)
                    elif r > 0 and attempt > 1:
                        # Just restart the client for later repetitions
                        self.logger.info("Retrying client only for later repetition...")

                    # Start client
                    client_svc = f"{SERVICE_PREFIX}raft_client_{int(time.time())}"
                    env_vars = {
                        "STRESS_TEST_BASE_URL": os.environ.get("STRESS_TEST_BASE_URL"),
                        "STRESS_TEST_API_KEY": os.environ.get("STRESS_TEST_API_KEY"),
                        "STRESS_TEST_MACHINE_NAME": os.environ.get("STRESS_TEST_MACHINE_NAME"),
                    }
                    env_flags = " ".join([f"-e {k}={v}" for k, v in env_vars.items() if v])

                    flags = ""
                    if params.get('distributed_actor_system'):
                        flags += " --use-distributed-actor-system"

                    peers_config = ",".join([
                        f"{idx+1}:{SERVERS[host]}:{port}"
                        for idx, host in enumerate(node_servers)
                    ])

                    cmd = (
                        f"docker service create --with-registry-auth "
                        f"--name {client_svc} "
                        f"--network host --restart-condition none "
                        f"--constraint 'node.hostname=={client_server}' "
                        f"{env_flags} "
                        f"{client_image} client --peers {peers_config} --stress-test "
                        f"--operations {params['operations']} --concurrency {params['concurrency']} "
                        f"--test-suite '{self.test_suite_name}'{flags}"
                    )
                    self._run_cmd(cmd, check=False, background=True)

                    # Wait for client
                    start_time_client = time.time()
                    status = "TIMEOUT"
                    terminal_states = ["Complete", "Shutdown", "Failed", "Rejected"]

                    while time.time() - start_time_client < self.timeout:
                        if self.interrupted:
                            status = "INTERRUPTED"
                            break

                        tasks_state = self._run_cmd(
                            f"docker service ps {client_svc} --format '{{{{.CurrentState}}}}'",
                            check=False
                        ).splitlines()

                        if not tasks_state:
                            time.sleep(2)
                            continue

                        if all(any(state.startswith(s) for s in terminal_states) for state in tasks_state):
                            if all(state.startswith("Complete") for state in tasks_state):
                                status = "SUCCESS"
                            else:
                                status = "FAILED"
                                logs = self._run_cmd(f"docker service logs {client_svc}", check=False)
                                self.logger.error(f"Client {client_svc} failed:\n{logs}")
                            break

                        time.sleep(2)

                    if status == "SUCCESS":
                        break
                    else:
                        self.logger.warning(f"Attempt {attempt} failed ({status}). Retrying...")

                repetition_results.append({"repetition": r+1, "status": status})
        finally:
            self._remove_all_services()

        return {
            "test_number": test_number,
            "parameters": params,
            "success": all(rep["status"] == "SUCCESS" for rep in repetition_results),
            "duration": time.time() - start_time,
            "repetitions": repetition_results
        }

    def run_tests(self, images, compaction_thresholds, peer_counts, operation_counts, concurrency_levels, client_image, resume_from=1):
        combinations = self.generate_test_combinations(images, compaction_thresholds, peer_counts, operation_counts, concurrency_levels)
        self.total_tests = len(combinations)

        for idx, params in enumerate(combinations, start=1):
            if idx < resume_from:
                self.logger.info(f"Skipping Test {idx}/{self.total_tests} due to resume setting...")
                continue
            if self.interrupted:
                break
            self.results.append(self._run_single_test(idx, params, client_image))

        return self.results

    def generate_report(self, output_file: str = None):
        if not output_file:
            output_file = f"test-output/raft_swarm_test_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"

        os.makedirs(os.path.dirname(output_file), exist_ok=True)

        with open(output_file, 'w') as f:
            f.write(f"Raft Swarm Test Suite Report: {self.test_suite_name}\n")
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
    parser = argparse.ArgumentParser(description="Run Raft tests on Docker Swarm")
    parser.add_argument("--images", nargs="+", required=True)
    parser.add_argument("--client-image", type=str, default="registry.niklabs.de/niklhut/raftswift:latest")
    parser.add_argument("--compaction-thresholds", nargs="+", type=int, default=[1000])
    parser.add_argument("--peer-counts", nargs="+", type=int, default=[3])
    parser.add_argument("--operation-counts", nargs="+", type=int, default=[10000])
    parser.add_argument("--concurrency-levels", nargs="+", type=int, default=[2])
    parser.add_argument("--timeout", type=int, default=180)
    parser.add_argument("--retries", type=int, default=3)
    parser.add_argument("--repetitions", type=int, default=3)
    parser.add_argument("--test-suite", type=str, help="Name of the test suite (overrides default timestamped name)")
    parser.add_argument("--resume", type=int, default=1, help="Resume from the given test number")
    args = parser.parse_args()

    runner = RaftSwarmTestRunner(
        test_suite_name=args.test_suite,
        timeout=args.timeout,
        retries=args.retries,
        repetitions=args.repetitions
    )
    runner.run_tests(
        images=args.images,
        compaction_thresholds=args.compaction_thresholds,
        peer_counts=args.peer_counts,
        operation_counts=args.operation_counts,
        concurrency_levels=args.concurrency_levels,
        client_image=args.client_image,
        resume_from=args.resume
    )
    runner.generate_report()

if __name__ == "__main__":
    main()
