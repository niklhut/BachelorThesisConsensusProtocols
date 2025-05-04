import ArgumentParser

public struct PeerConfig: Sendable, ExpressibleByArgument {
    let id: Int
    let name: String
    let port: Int
    let nameIsIPAddress: Bool

    public init(id: Int, name: String, port: Int) {
        self.id = id
        self.name = name
        self.port = port
        self.nameIsIPAddress = isIPv4Address(name)
    }

    public init?(argument: String) {
        let parts = argument.split(separator: ":")
        guard parts.count == 3,
            let id = Int(parts[0]),
            let port = Int(parts[2])
        else {
            return nil
        }
        self.id = id
        self.name = String(parts[1])
        self.port = port
        self.nameIsIPAddress = isIPv4Address(self.name)
    }
}

extension Array: @retroactive ExpressibleByArgument where Element == PeerConfig {
    public init?(argument: String) {
        let parts = argument.split(separator: ",")
        self = parts.compactMap { PeerConfig(argument: String($0)) }
    }
}
