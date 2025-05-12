// swift-tools-version: 6.1
import PackageDescription

let package = Package(
    name: "Raft",
    platforms: [.macOS(.v15)],
    dependencies: [
        .package(url: "https://github.com/apple/swift-log.git", from: "1.0.0"),
        .package(url: "https://github.com/apple/swift-argument-parser.git", from: "1.2.0"),
        .package(url: "https://github.com/apple/swift-distributed-actors.git", branch: "main"),
        .package(url: "https://github.com/happn-app/CollectionConcurrencyKit.git", branch: "taskgroup"),
        .package(url: "https://github.com/swiftlang/swift-testing.git", branch: "main"),
        .package(url: "https://github.com/onevcat/Rainbow", from: "4.0.0"),
    ],
    targets: [
        .executableTarget(
            name: "Raft",
            dependencies: [
                .product(name: "Logging", package: "swift-log"),
                .product(name: "ArgumentParser", package: "swift-argument-parser"),
                .product(name: "DistributedCluster", package: "swift-distributed-actors"),
                .product(name: "CollectionConcurrencyKit", package: "CollectionConcurrencyKit"),
                .product(name: "Rainbow", package: "Rainbow"),
            ]
        ),
        .testTarget(
            name: "RaftTests",
            dependencies: [
                .target(name: "Raft"),
                .product(name: "Testing", package: "swift-testing"),
            ]
        ),
    ]
)
