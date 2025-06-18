// swift-tools-version: 6.1

import PackageDescription

let package = Package(
    name: "Raft",
    platforms: [.macOS(.v15)],
    products: [
        .executable(name: "Raft", targets: ["RaftApp"]),
    ],
    dependencies: [
        // GRPC
        .package(url: "https://github.com/grpc/grpc-swift-2.git", from: "2.0.0"),
        .package(url: "https://github.com/grpc/grpc-swift-nio-transport.git", from: "2.0.0"),
        .package(url: "https://github.com/grpc/grpc-swift-protobuf.git", from: "2.0.0"),
        // Distributed Actors
        .package(url: "https://github.com/apple/swift-distributed-actors.git", branch: "main"),
        // Logging
        .package(url: "https://github.com/apple/swift-log.git", from: "1.0.0"),
        .package(url: "https://github.com/vapor/console-kit.git", from: "4.15.2"),
        // Argument Parser
        .package(url: "https://github.com/apple/swift-argument-parser.git", from: "1.2.0"),
        // Testing
        .package(url: "https://github.com/swiftlang/swift-testing.git", from: "6.1.0"),
    ],
    targets: [
        .target(
            name: "RaftCore",
            dependencies: [
                .product(name: "Logging", package: "swift-log"),
            ],
        ),
        .target(
            name: "RaftTest",
            dependencies: [
                "RaftCore",
                .product(name: "Logging", package: "swift-log"),
                .product(name: "ConsoleKitTerminal", package: "console-kit"),
            ],
        ),
        .target(
            name: "RaftGRPCTransport",
            dependencies: [
                "RaftCore",
                "RaftTest",
                .product(name: "GRPCCore", package: "grpc-swift-2"),
                .product(name: "GRPCNIOTransportHTTP2", package: "grpc-swift-nio-transport"),
                .product(name: "GRPCProtobuf", package: "grpc-swift-protobuf"),
            ],
            exclude: ["Protos/grpc-swift-proto-generator-config.docker.json"], plugins: [
                .plugin(name: "GRPCProtobufGenerator", package: "grpc-swift-protobuf"),
            ],
        ),
        .target(
            name: "RaftDistributedActorsTransport",
            dependencies: [
                "RaftCore",
                "RaftTest",
                .product(name: "DistributedCluster", package: "swift-distributed-actors"),
            ],
        ),
        .executableTarget(
            name: "RaftApp",
            dependencies: [
                "RaftCore",
                "RaftTest",
                "RaftGRPCTransport",
                "RaftDistributedActorsTransport",
                .product(name: "ArgumentParser", package: "swift-argument-parser"),
                .product(name: "Logging", package: "swift-log"),
            ],
        ),
        .testTarget(
            name: "RaftTests",
            dependencies: [
                "RaftApp",
                "RaftCore",
                "RaftGRPCTransport",
                "RaftDistributedActorsTransport",
                .product(name: "Testing", package: "swift-testing"),
            ],
        ),
    ],
)
