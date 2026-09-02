import Foundation
import MykoSwift
import XCTest

private final class LifecycleRecorder: @unchecked Sendable {
    private let lock = NSLock()
    private var starts = 0
    private var stops = 0

    func started() -> Int {
        lock.lock()
        defer { lock.unlock() }
        starts += 1
        return starts
    }

    func stopped() {
        lock.lock()
        stops += 1
        lock.unlock()
    }

    var counts: (starts: Int, stops: Int) {
        lock.lock()
        defer { lock.unlock() }
        return (starts, stops)
    }
}

final class MykoNodeLifecycleTests: XCTestCase {
    @MainActor
    func testRapidTransitionsAreSerializedAndPublishOnlyTheNewestCompletion() async {
        let finalRunning = expectation(description: "final node is running")
        let recorder = LifecycleRecorder()
        var delivered: [MykoNodeLifecyclePhase] = []
        let lifecycle = MykoNodeLifecycle<Int>(
            label: "myko.swift.node-lifecycle.test",
            start: { recorder.started() },
            stop: { recorder.stopped() },
            receive: { update in
                switch update {
                case .starting:
                    delivered.append(.starting)
                case .running:
                    delivered.append(.running)
                    finalRunning.fulfill()
                case .stopping:
                    delivered.append(.stopping)
                case .stopped:
                    delivered.append(.stopped)
                case .failed:
                    delivered.append(.failed)
                }
            }
        )

        lifecycle.setActive(true)
        lifecycle.setActive(false)
        lifecycle.setActive(true)

        await fulfillment(of: [finalRunning], timeout: 1)
        XCTAssertEqual(lifecycle.phase, .running)
        XCTAssertEqual(recorder.counts.starts, 2)
        XCTAssertEqual(recorder.counts.stops, 1)
        XCTAssertEqual(delivered, [.starting, .stopping, .starting, .running])
    }

    @MainActor
    func testFailedActivationCanBeRetried() async {
        enum ExpectedError: Error {
            case firstAttempt
        }

        let failed = expectation(description: "first activation fails")
        let running = expectation(description: "retry succeeds")
        let recorder = LifecycleRecorder()
        let lifecycle = MykoNodeLifecycle<Int>(
            start: {
                let attempt = recorder.started()
                if attempt == 1 { throw ExpectedError.firstAttempt }
                return attempt
            },
            stop: { recorder.stopped() },
            receive: { update in
                switch update {
                case .failed:
                    failed.fulfill()
                case .running:
                    running.fulfill()
                case .starting, .stopping, .stopped:
                    break
                }
            }
        )

        lifecycle.setActive(true)
        await fulfillment(of: [failed], timeout: 1)
        XCTAssertEqual(lifecycle.phase, .failed)

        lifecycle.setActive(true)
        await fulfillment(of: [running], timeout: 1)
        XCTAssertEqual(lifecycle.phase, .running)
        XCTAssertEqual(recorder.counts.starts, 2)
    }
}
