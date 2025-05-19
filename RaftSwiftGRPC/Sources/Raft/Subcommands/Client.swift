import ArgumentParser
import ConsoleKitTerminal
import Foundation
import GRPCCore
import GRPCNIOTransportHTTP2

final class Client: AsyncParsableCommand {
    static let configuration = CommandConfiguration(
        commandName: "client",
        abstract: "Start a raft client node",
    )

    lazy var terminal = Terminal()
    lazy var logger = ConsoleLogger(label: "raft.Client", console: terminal)

    @Option(help: "The list of peers in the format 'id:name:port,...'.")
    var peers: [Raft_Peer]

    @Flag(help: "Run in interactive mode")
    var interactive: Bool = false

    func run() async throws {
        let client = RaftClient(peers: peers)

        if interactive {
            let consoleClient = InteractiveConsoleClient(client: client)
            try await consoleClient.run()
        }
    }
}
