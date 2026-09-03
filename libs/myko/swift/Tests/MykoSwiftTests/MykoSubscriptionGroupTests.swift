import Foundation
import MykoSwift
import XCTest

@MainActor
private final class TestCancellableSubscription: MykoCancellableSubscription {
    private(set) var cancellations = 0

    func cancel() {
        cancellations += 1
    }
}

final class MykoSubscriptionGroupTests: XCTestCase {
    @MainActor
    func testGroupRetainsStaticSubscriptionsAcrossCancellation() {
        let first = TestCancellableSubscription()
        let second = TestCancellableSubscription()
        let group = MykoSubscriptionGroup()

        group.insert(first)
        group.insert(first)
        group.insert(second)
        XCTAssertEqual(group.count, 2)

        group.cancelAll()
        XCTAssertEqual(first.cancellations, 1)
        XCTAssertEqual(second.cancellations, 1)

        XCTAssertTrue(group.remove(first))
        XCTAssertFalse(group.remove(first))
        group.removeAll()

        XCTAssertEqual(first.cancellations, 2)
        XCTAssertEqual(second.cancellations, 2)
        XCTAssertEqual(group.count, 0)
    }

    @MainActor
    func testRegisteredBindingFollowsTheGroupLifecycle() {
        let binding = MykoSubscriptionBinding<TestBlockingSubscription>()
        let group = MykoSubscriptionGroup()

        group.register(
            binding,
            open: { TestBlockingSubscription() },
            receive: { _ in .keepAlive },
            failure: { _ in }
        )

        XCTAssertFalse(group.isActive)
        XCTAssertFalse(binding.isActive)

        group.activate()
        XCTAssertTrue(group.isActive)
        XCTAssertTrue(binding.isActive)

        group.cancelAll()
        XCTAssertFalse(group.isActive)
        XCTAssertFalse(binding.isActive)

        group.activate()
        XCTAssertTrue(binding.isActive)
        XCTAssertTrue(group.restart(binding))

        group.removeAll()
        XCTAssertFalse(group.isActive)
        XCTAssertFalse(binding.isActive)
    }
}

private final class TestBlockingSubscription: MykoBlockingSubscription, @unchecked Sendable {
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
