import DistributedCluster
import Foundation
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
        let randomId = Int.random(in: 7000 ... 8000)
        let actorSystem = await ClusterSystem("raft.DistributedActorSystem.Client.\(randomId)") { settings in
            settings.bindPort = randomId
            settings.downingStrategy = .timeout(.default)
            settings.logging.logLevel = .error
        }

        connectToPeers(actorSystem: actorSystem)

        let transport = DistributedActorClientTransport(peers: peers, actorSystem: actorSystem)

        try await transport.findPeers()

        try await withThrowingTaskGroup { group in
            for peer in peers {
                group.addTask {
                    let _ = try await actorSystem.cluster.joined(
                        endpoint: Cluster.Endpoint(host: peer.address, port: peer.port),
                        within: .seconds(10),
                    )
                }
            }

            try await group.waitForAll()
        }

        logger.info("Connected to all peers")

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

    public func runStressTest(
        operations: Int,
        concurrency: Int,
        testSuiteName: String,
        timeout: TimeInterval,
        durationSeconds: Int?,
        cpuCores: Double?,
        memory: Double?,
        payloadSizeBytes: Int,
        skipSanityCheck: Bool,
    ) async -> Bool {
        do {
            let client = try await setupClient()
            let stressTestClient = StressTestClient(
                client: client,
                testSuite: testSuiteName,
                cpuCores: cpuCores,
                memory: memory,
                payloadSizeBytes: payloadSizeBytes,
            )

            return await stressTestClient.run(
                operations: operations,
                concurrency: concurrency,
                timeout: timeout,
                durationSeconds: durationSeconds,
                skipSanityCheck: skipSanityCheck,
            )
        } catch {
            logger.error("Failed to set up client: \(error)")
            return false
        }
    }

    public func runFunctionalityTests() async throws {
        let client = try await setupClient()

        let functionalityTestClient = RaftTestClient(client: client)

        let results = await functionalityTestClient.runTestSuite()
        await functionalityTestClient.printTestReport(results)
    }
}
