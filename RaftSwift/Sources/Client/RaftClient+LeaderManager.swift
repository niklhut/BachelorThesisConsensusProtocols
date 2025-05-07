import Logging

extension RaftClient {
    actor LeaderManager {
        let logger: Logger
        var currentLeader: RaftNode?

        init(initialLeader: RaftNode?, logger: Logger) {
            currentLeader = initialLeader
            self.logger = logger
        }

        func getLeader() -> RaftNode? {
            currentLeader
        }

        func updateLeader(_ newLeader: RaftNode?) {
            if let newLeader, currentLeader == nil || newLeader.id != currentLeader?.id {
                currentLeader = newLeader
                Task {
                    logger.info("Updated leader to \(newLeader.id)")
                }
            }
        }
    }
}
