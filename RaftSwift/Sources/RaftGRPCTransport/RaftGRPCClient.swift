import Foundation
import GRPCCore
import GRPCNIOTransportHTTP2
import Logging
import RaftCore
import RaftTest

/// A Raft client that uses gRPC for communication
public final class RaftGRPCClient: RaftTestApplication {
    /// The list of peers
    let peers: [Peer]

    /// The logger
    let logger = Logger(label: "raft.RaftGRPCClient")

    public init(peers: [Peer]) {
        self.peers = peers
    }

    private func setupClient() async throws -> RaftClient<GRPCClientTransport> {
        let client = RaftClient(
            peers: peers,
            transport: GRPCClientTransport(clientPool: GRPCClientPool()),
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
    ) async throws {
        let client = try await setupClient()

        let stressTestClient = StressTestClient(
            client: client,
            testSuite: testSuiteName,
            cpuCores: cpuCores,
            memory: memory,
            payloadSizeBytes: payloadSizeBytes,
        )

        try await stressTestClient.run(
            operations: operations,
            concurrency: concurrency,
            timeout: timeout,
            durationSeconds: durationSeconds,
            skipSanityCheck: skipSanityCheck,
        )
    }

    public func runFunctionalityTests() async throws {
        let client = try await setupClient()

        let functionalityTestClient = RaftTestClient(client: client)

        let results = await functionalityTestClient.runTestSuite()
        await functionalityTestClient.printTestReport(results)
    }
}
