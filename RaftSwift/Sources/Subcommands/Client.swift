import ArgumentParser

struct Client: AsyncParsableCommand {
    static let configuration = CommandConfiguration(
        commandName: "client",
        abstract: "Start a client node"
    )

    @Option(help: "The list of peers in the format 'id:name:port,...'.")
    var peers: [PeerConfig]
    
    @Option(help: "Maximum retry attempts for connecting to peers")
    var maxRetries: Int = 10
    
    @Option(help: "Delay between retry attempts in seconds")
    var retryDelay: Double = 5.0

    func run() async throws {
        print("Client node started. Attempting to connect to peers...")
        // TODO
    }
}
