import Foundation
import GRPCNIOTransportHTTP2
import RaftCore

extension Peer {
    /// Returns true if the address is an IP address.
    var addressIsIPAdress: Bool {
        isIPv4Address(address)
    }

    /// Returns a ResolvableTarget for the peer's address.
    /// If the address is an IP address, it returns an IPv4 target.
    /// Otherwise, it returns a DNS target.
    var target: any ResolvableTarget {
        if addressIsIPAdress {
            .ipv4(
                address: address,
                port: Int(port),
            )
        } else {
            .dns(
                host: address,
                port: Int(port),
            )
        }
    }
}

private func isIPv4Address(_ input: String) -> Bool {
    let ipv4Regex = #"^(25[0-5]|2[0-4][0-9]|1[0-9]{2}|[1-9]?[0-9])\.(25[0-5]|2[0-4][0-9]|1[0-9]{2}|[1-9]?[0-9])\.(25[0-5]|2[0-4][0-9]|1[0-9]{2}|[1-9]?[0-9])\.(25[0-5]|2[0-4][0-9]|1[0-9]{2}|[1-9]?[0-9])$"#
    guard let regex = try? NSRegularExpression(pattern: ipv4Regex) else {
        return false
    }
    let range = NSRange(location: 0, length: input.utf16.count)
    return regex.firstMatch(in: input, options: [], range: range) != nil
}
