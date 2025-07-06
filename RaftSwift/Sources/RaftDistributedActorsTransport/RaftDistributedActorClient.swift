import DistributedCluster
import Logging
import RaftCore
import RaftTest

/// A Raft client that uses distributed actors for communication
public final class RaftDistributedActorClient: RaftTestApplication, PeerConnectable {
    /// The list of peers
    let peers: [Peer]

    /// The logger
    let logger = Logger(label: "raft.RaftDistributedActorClient")

    public init(peers: [Peer]) {
        self.peers = peers
    }

    private func setupClient() async throws -> RaftClient<DistributedActorClientTransport> {
        let actorSystem = await ClusterSystem("raft.DistributedActorSystem.Client")

        connectToPeers(actorSystem: actorSystem)

        let transport = DistributedActorClientTransport(peers: peers, actorSystem: actorSystem)

        try await transport.findPeers()

        try await withThrowingTaskGroup { group in
            for peer in peers {
                group.addTask {
                    let _ = try await actorSystem.cluster.joined(
                        endpoint: Cluster.Endpoint(host: peer.address, port: peer.port),
                        within: .seconds(5),
                    )
                }
            }

            try await group.waitForAll()
        }

        print("\n\nAll peers joined\n\n")

        let client = RaftClient(
            peers: peers,
            transport: transport,
        )

        return client
    }

    public func runInteractiveClient() async throws {
        let client = try await setupClient()

        let interactiveConsoleClient = InteractiveConsoleClient(client: client)

        try await interactiveConsoleClient.run()
    }

    public func runStressTest(operations: Int, concurrency: Int, testSuiteName: String) async throws {
        let client = try await setupClient()

        let stressTestClient = StressTestClient(client: client, testSuite: testSuiteName)

        try await stressTestClient.run(operations: operations, concurrency: concurrency)
    }

    public func runFunctionalityTests() async throws {
        let client = try await setupClient()

        let functionalityTestClient = RaftTestClient(client: client)

        let results = await functionalityTestClient.runTestSuite()
        await functionalityTestClient.printTestReport(results)
    }
}
