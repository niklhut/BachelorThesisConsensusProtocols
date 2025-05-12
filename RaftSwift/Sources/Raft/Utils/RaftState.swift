enum RaftState: Codable {
    case follower
    case candidate
    case leader
}
