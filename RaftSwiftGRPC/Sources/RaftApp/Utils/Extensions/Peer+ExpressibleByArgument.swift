import ArgumentParser
import RaftCore

extension RaftCore.Peer: ExpressibleByArgument {
    public init?(argument: String) {
        let parts = argument.split(separator: ":")
        guard parts.count == 3,
              let id = Int(parts[0]),
              let port = Int(parts[2])
        else {
            return nil
        }

        self.init(id: id, address: String(parts[1]), port: port)
    }
}

extension [RaftCore.Peer]: @retroactive ExpressibleByArgument {
    public init?(argument: String) {
        let parts = argument.split(separator: ",")
        self = parts.compactMap { RaftCore.Peer(argument: String($0)) }
    }
}
