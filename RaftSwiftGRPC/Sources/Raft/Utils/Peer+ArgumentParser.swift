import ArgumentParser

extension Raft_Peer: ExpressibleByArgument {
    init(id: UInt32, address: String, port: UInt32) {
        self.id = id
        self.address = address
        self.port = port
    }

    public init?(argument: String) {
        let parts = argument.split(separator: ":")
        guard parts.count == 3,
              let id = UInt32(parts[0]),
              let port = UInt32(parts[2])
        else {
            return nil
        }
        self.id = id
        address = String(parts[1])
        self.port = port
    }
}

extension [Raft_Peer]: @retroactive ExpressibleByArgument {
    public init?(argument: String) {
        let parts = argument.split(separator: ",")
        self = parts.compactMap { Raft_Peer(argument: String($0)) }
    }
}
