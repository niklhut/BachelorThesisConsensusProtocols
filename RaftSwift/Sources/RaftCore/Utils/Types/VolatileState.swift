import Foundation

/// Volatile state maintained in memory
struct VolatileState {
    /// Index of highest log entry known to be committed
    var commitIndex: Int = 0

    /// Index of highest log entry applied to state machine
    var lastApplied: Int = 0

    /// Current role/state of this server
    var state: ServerState = .follower

    /// ID of the current leader, if this node is a follower
    var currentLeaderID: Int?

    /// Last heartbeat time
    var lastHeartbeat: Date = .init()

    /// Election timeout
    var electionTimeout: Int = 0
}
