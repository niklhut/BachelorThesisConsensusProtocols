import Foundation

func isIPv4Address(_ input: String) -> Bool {
    let ipv4Regex = #"^(25[0-5]|2[0-4][0-9]|1[0-9]{2}|[1-9]?[0-9])\.(25[0-5]|2[0-4][0-9]|1[0-9]{2}|[1-9]?[0-9])\.(25[0-5]|2[0-4][0-9]|1[0-9]{2}|[1-9]?[0-9])\.(25[0-5]|2[0-4][0-9]|1[0-9]{2}|[1-9]?[0-9])$"#
    guard let regex = try? NSRegularExpression(pattern: ipv4Regex) else {
        return false
    }
    let range = NSRange(location: 0, length: input.utf16.count)
    return regex.firstMatch(in: input, options: [], range: range) != nil
}