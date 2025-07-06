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
    def __init__(self, test_suite_name: str = None):
        self.test_suite_name = test_suite_name or f"Raft Test Suite {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        self.docker_client = docker.from_env()
        self.current_test = 0
        self.total_tests = 0
        self.results = []
        self.interrupted = False

        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(f'raft_test_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
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
                                 concurrency_levels: List[int],
                                 use_actors: bool = False) -> List[Dict[str, Any]]:
        """Generate all combinations of test parameters"""
        combinations = []

        for image, threshold, peers, operations, concurrency in itertools.product(
            images, compaction_thresholds, peer_counts, operation_counts, concurrency_levels
        ):
            combinations.append({
                'image': image,
                'compaction_threshold': threshold,
                'peers': peers,
                'operations': operations,
                'concurrency': concurrency,
                'use_actors': use_actors
            })

        return combinations

    def _generate_compose_file(self, params: Dict[str, Any], client_image: str) -> str:
        """Generate docker-compose file for given parameters"""
        compose_file = f"docker-compose-test-{self.current_test}.yml"

        if generate_compose:
            # Use imported function
            compose_yaml = generate_compose(
                image=params['image'],
                client_image=client_image,
                num_peers=params['peers'],
                use_actors=params['use_actors'],
                operations=params['operations'],
                compaction_threshold=params['compaction_threshold'],
                test_suite=self.test_suite_name
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
                "--output", compose_file
            ]

            if params['use_actors']:
                cmd.append("--actors")

            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode != 0:
                raise RuntimeError(f"Failed to generate compose file: {result.stderr}")

        return compose_file

    def _cleanup_containers(self):
        """Clean up all containers from previous tests"""
        try:
            # Get all containers with raft in the name
            containers = self.docker_client.containers.list(all=True)
            raft_containers = [c for c in containers if 'raft' in c.name.lower()]

            if not raft_containers:
                return

            # Stop all running containers simultaneously
            running_containers = [c for c in raft_containers if c.status == 'running']
            if running_containers:
                self.logger.info(f"Stopping {len(running_containers)} containers simultaneously...")

                # Start stopping all containers in parallel
                stop_threads = []
                import threading

                def stop_container(container):
                    try:
                        self.logger.debug(f"Stopping container: {container.name}")
                        container.stop(timeout=10)
                    except Exception as e:
                        self.logger.warning(f"Error stopping container {container.name}: {e}")

                for container in running_containers:
                    thread = threading.Thread(target=stop_container, args=(container,))
                    thread.start()
                    stop_threads.append(thread)

                # Wait for all stop operations to complete
                for thread in stop_threads:
                    thread.join()

                self.logger.info("All containers stopped")

            # Remove all containers simultaneously
            self.logger.info(f"Removing {len(raft_containers)} containers simultaneously...")

            remove_threads = []

            def remove_container(container):
                try:
                    self.logger.debug(f"Removing container: {container.name}")
                    container.remove(force=True)
                except Exception as e:
                    self.logger.warning(f"Error removing container {container.name}: {e}")

            for container in raft_containers:
                thread = threading.Thread(target=remove_container, args=(container,))
                thread.start()
                remove_threads.append(thread)

            # Wait for all remove operations to complete
            for thread in remove_threads:
                thread.join()

            self.logger.info("All containers removed")

            # Clean up networks
            networks = self.docker_client.networks.list()
            raft_networks = [n for n in networks if 'raft' in n.name.lower() and n.name != 'bridge']

            if raft_networks:
                self.logger.info(f"Removing {len(raft_networks)} networks...")
                for network in raft_networks:
                    try:
                        self.logger.debug(f"Removing network: {network.name}")
                        network.remove()
                    except Exception as e:
                        self.logger.warning(f"Error removing network {network.name}: {e}")

        except Exception as e:
            self.logger.error(f"Error during cleanup: {e}")

    def _wait_for_client_completion(self, compose_file: str, timeout: int = 1800) -> bool:
        """Wait for the client container to complete and return success status"""
        start_time = time.time()

        while time.time() - start_time < timeout:
            if self.interrupted:
                return False

            try:
                # Check client container status
                client_container = self.docker_client.containers.get('raft_client')

                if client_container.status == 'exited':
                    exit_code = client_container.attrs['State']['ExitCode']
                    self.logger.info(f"Client container exited with code: {exit_code}")
                    return exit_code == 0

            except docker.errors.NotFound:
                # Container doesn't exist yet, wait a bit more
                pass
            except Exception as e:
                self.logger.warning(f"Error checking client status: {e}")

            time.sleep(5)

        self.logger.error("Timeout waiting for client completion")
        return False

    def _run_single_test(self, params: Dict[str, Any], client_image: str) -> Dict[str, Any]:
        """Run a single test configuration"""
        self.current_test += 1
        test_name = f"Test {self.current_test}/{self.total_tests}"

        self.logger.info(f"Starting {test_name}")
        self.logger.info(f"Parameters: {params}")

        start_time = time.time()
        compose_file = None

        try:
            # Generate compose file
            compose_file = self._generate_compose_file(params, client_image)

            # Start containers
            self.logger.info("Starting containers...")
            result = subprocess.run(
                ["docker-compose", "-f", compose_file, "up", "-d"],
                capture_output=True, text=True
            )

            if result.returncode != 0:
                raise RuntimeError(f"Failed to start containers: {result.stderr}")

            # Wait for client to complete
            self.logger.info("Waiting for test completion...")
            success = self._wait_for_client_completion(compose_file)

            # Get logs from client
            try:
                client_container = self.docker_client.containers.get('raft_client')
                logs = client_container.logs().decode('utf-8')
            except:
                logs = "Could not retrieve logs"

            end_time = time.time()
            duration = end_time - start_time

            result = {
                'test_number': self.current_test,
                'parameters': params,
                'success': success,
                'duration': duration,
                'logs': logs,
                'timestamp': datetime.now().isoformat()
            }

            self.logger.info(f"{test_name} completed in {duration:.2f}s - {'SUCCESS' if success else 'FAILED'}")
            return result

        except Exception as e:
            self.logger.error(f"{test_name} failed with error: {e}")
            return {
                'test_number': self.current_test,
                'parameters': params,
                'success': False,
                'duration': time.time() - start_time,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }

        finally:
            # Always cleanup containers after each test
            self._cleanup_containers()

            # Clean up compose file
            if compose_file and os.path.exists(compose_file):
                try:
                    os.remove(compose_file)
                except Exception as e:
                    self.logger.warning(f"Could not remove compose file: {e}")

    def run_tests(self,
                  images: List[str],
                  compaction_thresholds: List[int],
                  peer_counts: List[int],
                  operation_counts: List[int],
                  concurrency_levels: List[int],
                  client_image: str = "registry.niklabs.de/niklhut/raft-swift:latest",
                  use_actors: bool = False) -> List[Dict[str, Any]]:
        """Run all test combinations"""

        combinations = self.generate_test_combinations(
            images, compaction_thresholds, peer_counts, operation_counts,
            concurrency_levels, use_actors
        )

        self.total_tests = len(combinations)
        self.logger.info(f"Starting test suite: {self.test_suite_name}")
        self.logger.info(f"Total tests to run: {self.total_tests}")
        self.logger.info(f"Testing {len(images)} different images")
        self.logger.info(f"Client image: {client_image}")

        # Initial cleanup
        self._cleanup_containers()

        for params in combinations:
            if self.interrupted:
                break

            result = self._run_single_test(params, client_image)
            self.results.append(result)

            # Brief pause between tests
            time.sleep(2)

        return self.results

    def generate_report(self, output_file: str = None):
        """Generate a comprehensive test report"""
        if not output_file:
            output_file = f"raft_test_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"

        total_tests = len(self.results)
        successful_tests = sum(1 for r in self.results if r['success'])
        failed_tests = total_tests - successful_tests

        with open(output_file, 'w') as f:
            f.write(f"Raft Implementation Test Report\n")
            f.write(f"{'='*50}\n\n")
            f.write(f"Test Suite: {self.test_suite_name}\n")
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            f.write(f"Summary:\n")
            f.write(f"  Total Tests: {total_tests}\n")
            f.write(f"  Successful: {successful_tests}\n")
            f.write(f"  Failed: {failed_tests}\n")
            f.write(f"  Success Rate: {(successful_tests/total_tests)*100:.1f}%\n\n")

            # Detailed results grouped by image
            f.write("Detailed Results:\n")
            f.write("-" * 50 + "\n")

            # Group results by image for better readability
            results_by_image = {}
            for result in self.results:
                image = result['parameters']['image']
                if image not in results_by_image:
                    results_by_image[image] = []
                results_by_image[image].append(result)

            for image, image_results in results_by_image.items():
                f.write(f"\nImage: {image}\n")
                f.write("=" * 80 + "\n")

                image_success = sum(1 for r in image_results if r['success'])
                image_total = len(image_results)
                f.write(f"Image Results: {image_success}/{image_total} successful ({(image_success/image_total)*100:.1f}%)\n\n")

                for result in image_results:
                    f.write(f"  Test {result['test_number']}:\n")
                    f.write(f"    Parameters: {result['parameters']}\n")
                    f.write(f"    Result: {'SUCCESS' if result['success'] else 'FAILED'}\n")
                    f.write(f"    Duration: {result['duration']:.2f}s\n")
                    f.write(f"    Timestamp: {result['timestamp']}\n")

                    if 'error' in result:
                        f.write(f"    Error: {result['error']}\n")
                    f.write("\n")

        self.logger.info(f"Test report generated: {output_file}")
        print(f"\nTest Summary:")
        print(f"  Total Tests: {total_tests}")
        print(f"  Successful: {successful_tests}")
        print(f"  Failed: {failed_tests}")
        print(f"  Success Rate: {(successful_tests/total_tests)*100:.1f}%")
        print(f"  Report saved to: {output_file}")

def main():
    parser = argparse.ArgumentParser(description="Run comprehensive Raft implementation tests")
    parser.add_argument("--images", nargs="+", type=str, required=True,
                        help="List of Docker images to test")
    parser.add_argument("--client-image", type=str, default="registry.niklabs.de/niklhut/raft-swift:latest",
                        help="Docker image for client (default: registry.niklabs.de/niklhut/raft-swift:latest)")
    parser.add_argument("--compaction-thresholds", nargs="+", type=int, default=[500, 1000, 2000],
                        help="List of compaction thresholds to test")
    parser.add_argument("--peer-counts", nargs="+", type=int, default=[3, 5, 7],
                        help="List of peer counts to test")
    parser.add_argument("--operation-counts", nargs="+", type=int, default=[1000, 5000, 10000],
                        help="List of operation counts to test")
    parser.add_argument("--concurrency-levels", nargs="+", type=int, default=[1, 2, 4],
                        help="List of concurrency levels to test")
    parser.add_argument("--actors", action="store_true",
                        help="Use distributed actor system for all tests")
    parser.add_argument("--test-suite", type=str,
                        help="Name of the test suite")
    parser.add_argument("--report", type=str,
                        help="Output file for test report")

    args = parser.parse_args()

    runner = RaftTestRunner(args.test_suite)

    try:
        results = runner.run_tests(
            images=args.images,
            compaction_thresholds=args.compaction_thresholds,
            peer_counts=args.peer_counts,
            operation_counts=args.operation_counts,
            concurrency_levels=args.concurrency_levels,
            client_image=args.client_image,
            use_actors=args.actors
        )

        runner.generate_report(args.report)

    except KeyboardInterrupt:
        print("\nTest run interrupted by user")
    except Exception as e:
        print(f"Test run failed with error: {e}")
        runner.logger.error(f"Test run failed: {e}")
    finally:
        runner._cleanup_containers()

if __name__ == "__main__":
    main()