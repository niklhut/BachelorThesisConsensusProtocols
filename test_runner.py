#!/usr/bin/env python3
"""
Comprehensive test runner for Raft implementation.
Tests all combinations of specified parameters and manages Docker containers.
"""

import argparse
import itertools
import subprocess
import time
import os
import sys
import yaml
from datetime import datetime
from typing import List, Dict, Any
import docker
import signal
import logging

# Import the original script functions
try:
    from create_compose import generate_compose
except ImportError:
    # If import fails, we'll use subprocess to call the original script
    generate_compose = None

class RaftTestRunner:
    def __init__(self, test_suite_name: str = None, timeout: int = 70, retries: int = 1, repetitions: int = 3):
        self.test_suite_name = test_suite_name or f"Raft Test Suite {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        self.docker_client = docker.from_env()
        self.total_tests = 0
        self.results = []
        self.interrupted = False
        self.timeout = timeout
        self.retries = retries
        self.repetitions = repetitions

        # Setup logging
        os.makedirs("test-output", exist_ok=True)
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(f'test-output/raft_test_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
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
        self._cleanup_containers()
        sys.exit(0)

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
            # For raftswift implementation, we want to test with different combinations of flags
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

    def _generate_compose_file(self, test_number: int, params: Dict[str, Any], client_image: str, client_start_delay: int = 0) -> str:
        """Generate docker-compose file for given parameters"""
        compose_file = f"docker-compose-test-{test_number}.yml"

        if generate_compose:
            # Use imported function
            compose_yaml = generate_compose(
                image=params['image'],
                client_image=client_image,
                num_peers=params['peers'],
                operations=params['operations'],
                concurrency=params['concurrency'],
                compaction_threshold=params['compaction_threshold'],
                test_suite=self.test_suite_name,
                client_start_delay=client_start_delay,
                distributed_actor_system=params.get('distributed_actor_system', False),
                manual_locks=params.get('manual_locks', False)
            )

            with open(compose_file, 'w') as f:
                yaml.dump(compose_yaml, f, default_flow_style=False, sort_keys=False)
        else:
            # Use subprocess to call original script
            cmd = [
                sys.executable, "create_compose.py",
                "--peers", str(params['peers']),
                "--operations", str(params['operations']),
                "--compaction-threshold", str(params['compaction_threshold']),
                "--image", params['image'],
                "--client-image", client_image,
                "--test-suite", self.test_suite_name,
                "--output", compose_file,
                "--client-start-delay", str(client_start_delay)
            ]

            if params.get('distributed_actor_system'):
                cmd.append("--distributed-actor-system")
            if params.get('manual_locks'):
                cmd.append("--manual-locks")

            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode != 0:
                raise RuntimeError(f"Failed to generate compose file: {result.stderr}")

        return compose_file

    def _cleanup_containers(self, cleanup_all=True):
        """Clean up containers. If cleanup_all is False, only cleans client."""
        try:
            containers = self.docker_client.containers.list(all=True)
            if cleanup_all:
                target_containers = [c for c in containers if 'raft' in c.name.lower()]
            else:
                target_containers = [c for c in containers if 'raft_client' in c.name.lower()]

            if not target_containers:
                return

            for container in target_containers:
                try:
                    # Force stop with a 0-second timeout before removing to avoid graceful shutdown period.
                    container.stop(timeout=0)
                except Exception:
                    # Ignore errors, e.g., if the container is already stopped.
                    pass
                try:
                    container.remove(force=True)
                except Exception as e:
                    self.logger.warning(f"Error removing container {container.name}: {e}")

            if cleanup_all:
                networks = self.docker_client.networks.list()
                raft_networks = [n for n in networks if 'raft' in n.name.lower() and n.name != 'bridge']
                for network in raft_networks:
                    try:
                        network.remove()
                    except Exception as e:
                        self.logger.warning(f"Error removing network {network.name}: {e}")

        except Exception as e:
            self.logger.error(f"Error during cleanup: {e}")

    def _wait_for_client_completion(self, timeout: int) -> str:
        """Wait for the client container to complete and return 'SUCCESS', 'FAILED', 'TIMEOUT', or 'INTERRUPTED'."""
        start_time = time.time()

        while time.time() - start_time < timeout:
            if self.interrupted:
                return "INTERRUPTED"

            try:
                client_container = self.docker_client.containers.get('raft_client')
                if client_container.status == 'exited':
                    exit_code = client_container.attrs['State']['ExitCode']
                    self.logger.info(f"Client container exited with code: {exit_code}")
                    return "SUCCESS" if exit_code == 0 else "FAILED"
            except docker.errors.NotFound:
                pass  # Wait
            except Exception as e:
                self.logger.warning(f"Error checking client status: {e}")

            time.sleep(2)

        self.logger.error("Timeout waiting for client completion")
        return "TIMEOUT"

    def _run_single_test(self, test_number: int, params: Dict[str, Any], client_image: str) -> Dict[str, Any]:
        """Run a single test configuration with repetitions."""
        test_name = f"Test {test_number}/{self.total_tests}"
        self.logger.info(f"Starting {test_name} with {self.repetitions} repetitions")
        self.logger.info(f"Parameters: {params}")

        compose_file = self._generate_compose_file(test_number, params, client_image, client_start_delay=0)
        repetition_results = []
        start_time = time.time()
        first_repetition_failed = False

        try:
            # Start node containers
            node_services = [s for s in yaml.safe_load(open(compose_file))['services'].keys() if 'node' in s]
            subprocess.run(["docker-compose", "-f", compose_file, "up", "-d"] + node_services, check=True)
            time.sleep(4) # Give nodes time to elect a leader

            for i in range(self.repetitions):
                rep_name = f"{test_name} Rep {i+1}/{self.repetitions}"
                self.logger.info(f"Starting {rep_name}")

                # Start client
                subprocess.run(["docker-compose", "-f", compose_file, "up", "-d", "raft_client"], check=True)

                status = self._wait_for_client_completion(self.timeout)

                logs = ""
                try:
                    logs = self.docker_client.containers.get('raft_client').logs().decode('utf-8')
                except Exception:
                    pass

                rep_result = {'repetition': i+1, 'status': status, 'logs': logs}
                repetition_results.append(rep_result)

                self._cleanup_containers(cleanup_all=False) # Remove client

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
            self._cleanup_containers(cleanup_all=True)
            if os.path.exists(compose_file):
                os.remove(compose_file)

        overall_success = all(r['status'] == 'SUCCESS' for r in repetition_results)
        result = {
            'test_number': test_number, 'parameters': params, 'success': overall_success,
            'duration': time.time() - start_time, 'repetitions': repetition_results,
            'timestamp': datetime.now().isoformat()
        }
        if first_repetition_failed:
            result['error'] = "First repetition failed."
        return result

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
        self.logger.info(f"Starting test suite: {self.test_suite_name}")
        self.logger.info(f"Total tests to run: {self.total_tests}")

        self._cleanup_containers()

        test_number = 0
        for params in combinations:
            test_number += 1
            if self.interrupted:
                break

            for attempt in range(self.retries):
                result = self._run_single_test(test_number, params, client_image)

                if 'error' not in result or "First repetition failed." not in result['error']:
                    # Success or a non-retryable failure
                    break

                if attempt < self.retries - 1:
                    self.logger.warning(f"Test {test_number} failed on attempt {attempt + 1}/{self.retries} because first repetition failed. Retrying...")
                    time.sleep(5)
                else:
                    self.logger.error(f"Test {test_number} failed on all {self.retries} attempts because first repetition failed.")

            self.results.append(result)
            time.sleep(2)

        return self.results

    def generate_report(self, output_file: str = None):
        if not output_file:
            output_file = f"test-output/raft_test_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"

            # Create the output directory if it doesn't exist
            os.makedirs(os.path.dirname(output_file), exist_ok=True)

        # ... (rest of the report generation, adapted for repetitions)


def main():
    parser = argparse.ArgumentParser(description="Run comprehensive Raft implementation tests")
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

    runner = RaftTestRunner(
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
        runner._cleanup_containers()

if __name__ == "__main__":
    main()