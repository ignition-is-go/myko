import Foundation
import MykoSwift
import XCTest

private final class TestSubscription: MykoBlockingSubscription, @unchecked Sendable {
    enum TestError: Error {
        case cancelled
    }

    private let condition = NSCondition()
    private var queued: [Int] = []
    private var cancelled = false

    func current() throws -> Int {
        0
    }

    func next() throws -> Int {
        condition.lock()
        defer { condition.unlock() }
        while queued.isEmpty && !cancelled {
            condition.wait()
        }
        guard !cancelled else { throw TestError.cancelled }
        return queued.removeFirst()
    }

    func cancel() {
        condition.lock()
        cancelled = true
        condition.broadcast()
        condition.unlock()
    }

    func send(_ value: Int) {
        condition.lock()
        queued.append(value)
        condition.signal()
        condition.unlock()
    }
}

final class MykoSubscriptionBindingTests: XCTestCase {
    @MainActor
    func testDeliversRevisionsAndCancellationWakesTheConsumer() async {
        let initial = expectation(description: "initial revision")
        let changed = expectation(description: "changed revision")
        let subscription = TestSubscription()
        let binding = MykoSubscriptionBinding<TestSubscription>(label: "myko.swift.test")
        var received: [Int] = []

        binding.start(
            open: { subscription },
            receive: { value in
                received.append(value)
                if value == 0 { initial.fulfill() }
                if value == 1 { changed.fulfill() }
                return .keepAlive
            },
            failure: { _ in
                XCTFail("an explicit cancellation must not surface as a failure")
            }
        )

        await fulfillment(of: [initial], timeout: 1)
        subscription.send(1)
        await fulfillment(of: [changed], timeout: 1)
        binding.cancel()

        XCTAssertEqual(received, [0, 1])
        XCTAssertFalse(binding.isActive)
    }

    @MainActor
    func testTerminalRevisionFinishesTheBinding() async {
        let terminal = expectation(description: "terminal revision")
        let subscription = TestSubscription()
        let binding = MykoSubscriptionBinding<TestSubscription>(label: "myko.swift.terminal")

        binding.start(open: { subscription }) { _ in
            terminal.fulfill()
            return .finish
        }

        await fulfillment(of: [terminal], timeout: 1)
        XCTAssertFalse(binding.isActive)
    }
}
