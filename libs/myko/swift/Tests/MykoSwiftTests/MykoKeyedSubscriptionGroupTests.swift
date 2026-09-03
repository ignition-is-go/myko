import Foundation
import MykoSwift
import XCTest

private struct TestRow: Sendable {
    let id: String
}

private final class KeyedTestSubscription: MykoBlockingSubscription, @unchecked Sendable {
    private let condition = NSCondition()
    private var cancelled = false

    func current() throws -> Int {
        0
    }

    func next() throws -> Int {
        condition.lock()
        defer { condition.unlock() }
        while !cancelled {
            condition.wait()
        }
        throw CancellationError()
    }

    func cancel() {
        condition.lock()
        cancelled = true
        condition.broadcast()
        condition.unlock()
    }
}

final class MykoKeyedSubscriptionGroupTests: XCTestCase {
    @MainActor
    func testReconcileRetainsStableKeysAndRemovesMissingOnes() {
        let lifecycle = MykoSubscriptionGroup()
        let keyed = MykoKeyedSubscriptionGroup<String, KeyedTestSubscription>(
            lifecycle: lifecycle
        )

        keyed.reconcile(
            [TestRow(id: "one"), TestRow(id: "two"), TestRow(id: "two")],
            identifiedBy: { $0.id },
            label: { "test.\($0.id)" },
            open: { _ in KeyedTestSubscription() },
            receive: { _, _ in .keepAlive },
            failure: { _, _ in }
        )
        XCTAssertEqual(keyed.keys, ["one", "two"])
        XCTAssertEqual(keyed.count, 2)
        XCTAssertEqual(lifecycle.count, 2)

        lifecycle.activate()
        keyed.reconcile(
            [TestRow(id: "two"), TestRow(id: "three")],
            identifiedBy: { $0.id },
            label: { "test.\($0.id)" },
            open: { _ in KeyedTestSubscription() },
            receive: { _, _ in .keepAlive },
            failure: { _, _ in }
        )
        XCTAssertEqual(keyed.keys, ["two", "three"])
        XCTAssertEqual(keyed.count, 2)
        XCTAssertEqual(lifecycle.count, 2)

        keyed.removeAll()
        XCTAssertTrue(keyed.keys.isEmpty)
        XCTAssertEqual(lifecycle.count, 0)
    }
}
