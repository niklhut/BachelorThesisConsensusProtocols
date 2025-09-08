import Foundation

public actor StressTestRuntime {
    public static let shared = StressTestRuntime()

    private(set) var baseUrl: String?
    private(set) var apiKey: String?
    private(set) var machineName: String?
    private(set) var allowPartialOnStop = false

    private init() {}

    public func configureAnalytics(baseUrl: String?, apiKey: String?, machineName: String?) {
        self.baseUrl = baseUrl
        self.apiKey = apiKey
        self.machineName = machineName
    }

    public func setAllowPartialOnStop(_ allow: Bool) {
        allowPartialOnStop = allow
    }
}
