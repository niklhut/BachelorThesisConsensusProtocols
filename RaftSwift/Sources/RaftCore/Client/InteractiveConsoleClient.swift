import ConsoleKitTerminal

/// Interactive console client for Raft.
/// Allows to send read, read debug and put requests to the Raft cluster.
public struct InteractiveConsoleClient<Transport: RaftClientTransport> {
    /// The Raft client to use for communication with the server.
    let client: RaftClient<Transport>

    /// The terminal to use for output.
    let terminal = Terminal()

    /// Initializes a new instance of the InteractiveConsoleClient class.
    /// - Parameters:
    ///   - client: The Raft client to use for communication with the server.
    public init(client: RaftClient<Transport>) {
        self.client = client
    }

    /// Runs the interactive console client.
    public func run() async throws {
        while true {
            terminal.clear(.screen)

            let leader = try await client.findLeader()
            terminal.output("Current leader: \(String(leader.id))".consoleText(color: .brightCyan))

            let choice = terminal.choose("Do you want to read or write a value?", from: ["Read", "Read Debug", "Write"])
            switch choice {
            case "Read":
                let key = terminal.ask("Enter the key to read:")
                let response = try await client.get(request: .init(key: key))
                if let value = response.value {
                    terminal.output("Value: \(value)".consoleText(color: .green))
                } else {
                    terminal.output("Key not found".consoleText(color: .red))
                }

            case "Read Debug":
                let key = terminal.ask("Enter the key to read:")
                let peerId = terminal.choose("Choose a peer to read from:", from: client.peers.map(\.id))
                let response = try await client.getDebug(request: .init(key: key), from: client.peers.first { $0.id == peerId }!)
                if let value = response.value {
                    terminal.output("Value: \(value)".consoleText(color: .green))
                } else {
                    terminal.output("Key not found".consoleText(color: .red))
                }

            case "Write":
                let key = terminal.ask("Enter the key to write:")
                let value = terminal.ask("Enter the value to write:")
                let response = try await client.put(request: .init(key: key, value: value))
                if response.success {
                    terminal.output("Value written successfully".consoleText(color: .green))
                } else {
                    terminal.output("Failed to write value".consoleText(color: .red))
                }

            default:
                break
            }

            _ = terminal.ask("Press enter to continue...")
        }
    }
}
