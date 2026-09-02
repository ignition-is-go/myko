// swift-tools-version: 5.9

import PackageDescription

let package = Package(
    name: "MykoSwift",
    platforms: [
        .iOS(.v17),
        .macOS(.v14),
    ],
    products: [
        .library(name: "MykoSwift", targets: ["MykoSwift"]),
    ],
    targets: [
        .target(name: "MykoSwift"),
        .testTarget(name: "MykoSwiftTests", dependencies: ["MykoSwift"]),
    ]
)
