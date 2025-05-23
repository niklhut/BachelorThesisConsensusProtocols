/// Leader only volatile state tracking next log entry to send to each follower
struct LeaderState {
    /// For each follower, index of the next log entry to send to that follower
    var nextIndex: [Int: Int] = [:]

    /// For each follower, index of highest log entry known to be replicated on that follower
    var matchIndex: [Int: Int] = [:]
}
