import Foundation
import XCTest
@testable import MykoSwift

final class MykoNodeBootstrapTests: XCTestCase {
    private struct Node: Equatable {
        let path: String
        let identity: Data
    }

    private final class MemoryStore: MykoOpaqueValueStore {
        var value: Data?

        init(_ value: Data? = nil) {
            self.value = value
        }

        func load() throws -> Data? {
            value
        }

        func save(_ value: Data) throws {
            self.value = value
        }

        func remove() throws {
            value = nil
        }
    }

    func testCreatesAndPersistsANewIdentity() throws {
        let store = MemoryStore()
        let directory = URL(fileURLWithPath: "/example/myko", isDirectory: true)
        let expectedIdentity = Data([1, 2, 3])

        let result = try MykoNodeBootstrap.loadOrCreate(
            storageDirectory: directory,
            identityStore: store,
            restore: { _, _ in
                XCTFail("restore should not run")
                return Node(path: "", identity: Data())
            },
            create: { Node(path: $0, identity: expectedIdentity) },
            identity: { $0.identity }
        )

        XCTAssertFalse(result.restoredIdentity)
        XCTAssertEqual(result.node, Node(path: directory.path, identity: expectedIdentity))
        XCTAssertEqual(store.value, expectedIdentity)
    }

    func testRestoresWithoutCreatingOrReplacingIdentity() throws {
        let expectedIdentity = Data([4, 5, 6])
        let store = MemoryStore(expectedIdentity)
        let directory = URL(fileURLWithPath: "/example/myko", isDirectory: true)

        let result = try MykoNodeBootstrap.loadOrCreate(
            storageDirectory: directory,
            identityStore: store,
            restore: { Node(path: $0, identity: $1) },
            create: { _ in
                XCTFail("create should not run")
                return Node(path: "", identity: Data())
            },
            identity: { _ in
                XCTFail("identity should not be read")
                return Data()
            }
        )

        XCTAssertTrue(result.restoredIdentity)
        XCTAssertEqual(result.node, Node(path: directory.path, identity: expectedIdentity))
        XCTAssertEqual(store.value, expectedIdentity)
    }
}
