import GRPCCore
import GRPCNIOTransportHTTP2
import RaftCore

actor GRPCClientPool {
    typealias Client = GRPCClient<HTTP2ClientTransport.Posix>

    private let interceptors: [ClientInterceptor]
    private var clients: [Int: Client] = [:]

    init(interceptors: [ClientInterceptor] = []) {
        self.interceptors = interceptors
    }

    func client(for peer: Peer) async throws -> Client {
        if let client = clients[peer.id] {
            return client
        }

        let client = try GRPCClient(
            transport: .http2NIOPosix(
                target: peer.target,
                transportSecurity: .plaintext,
            ),
            interceptors: interceptors,
        )

        clients[peer.id] = client

        Task {
            try await client.runConnections()
        }

        return client
    }

    deinit {
        clients.values.forEach { $0.beginGracefulShutdown() }
    }
}
