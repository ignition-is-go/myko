import Foundation
import MykoSwift
import XCTest

private final class SessionBlockingSubscription: MykoBlockingSubscription, @unchecked Sendable {
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

final class MykoNodeSessionTests: XCTestCase {
    @MainActor
    func testNodeLifecycleOwnsSubscriptionAndOperationLifetimes() async {
        let running = expectation(description: "node session is running")
        let stopping = expectation(description: "node session is stopping")
        let subscription = MykoSubscriptionBinding<SessionBlockingSubscription>()
        var session: MykoNodeSession<Int>!
        session = MykoNodeSession(
            start: { 7 },
            stop: {},
            receive: { update in
                switch update {
                case .running:
                    XCTAssertTrue(session.isActive)
                    XCTAssertTrue(session.subscriptions.isActive)
                    XCTAssertTrue(subscription.isActive)
                    running.fulfill()
                case .stopping:
                    XCTAssertFalse(session.isActive)
                    XCTAssertFalse(session.subscriptions.isActive)
                    XCTAssertFalse(subscription.isActive)
                    stopping.fulfill()
                case .starting, .stopped, .failed:
                    break
                }
            }
        )
        session.subscriptions.register(
            subscription,
            open: { SessionBlockingSubscription() },
            receive: { _ in .keepAlive },
            failure: { _ in }
        )

        session.setActive(true)
        await fulfillment(of: [running], timeout: 1)

        let release = DispatchSemaphore(value: 0)
        var deliveredStaleResult = false
        let task = session.operations.run(
            operation: {
                release.wait()
                return 42
            },
            receive: { _ in deliveredStaleResult = true }
        )

        session.setActive(false)
        await fulfillment(of: [stopping], timeout: 1)
        release.signal()
        await task.value

        XCTAssertFalse(deliveredStaleResult)
    }
}
