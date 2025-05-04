import ArgumentParser

struct Client: AsyncParsableCommand {
    static let configuration = CommandConfiguration(
        commandName: "client",
        abstract: "Start a client node"
    )

    @Option(help: "The list of peers in the format 'id:name:port,...'.")
    var peers: [PeerConfig]
    
    @Option(help: "Maximum retry attempts for connecting to peers")
    var maxRetries: Int = GlobalConfig.maxRetries
    
    @Option(help: "Delay between retry attempts in seconds")
    var retryDelay: Double = GlobalConfig.retryDelay

    func run() async throws {
        print("Client node started. Attempting to connect to peers...")
        // TODO
    }
}
