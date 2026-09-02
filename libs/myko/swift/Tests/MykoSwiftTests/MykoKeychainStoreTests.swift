import Foundation
import MykoSwift
import XCTest

#if canImport(Security)
final class MykoKeychainStoreTests: XCTestCase {
    func testStoresReplacesAndRemovesOpaqueValues() throws {
        let store = MykoKeychainStore(
            service: "myko.swift.tests.\(UUID().uuidString)",
            account: "opaque-value"
        )
        defer { try? store.remove() }

        XCTAssertNil(try store.load())

        try store.save(Data([1, 2, 3]))
        XCTAssertEqual(try store.load(), Data([1, 2, 3]))

        try store.save(Data([4, 5]))
        XCTAssertEqual(try store.load(), Data([4, 5]))

        try store.remove()
        XCTAssertNil(try store.load())
    }
}
#endif
