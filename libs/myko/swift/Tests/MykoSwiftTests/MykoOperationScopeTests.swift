import Foundation
import MykoSwift
import XCTest

final class MykoOperationScopeTests: XCTestCase {
    private enum ExpectedError: Error, Equatable {
        case failure
    }

    @MainActor
    func testDeliversSuccessAndFailureOnTheMainActor() async {
        let scope = MykoOperationScope()
        var value: Int?
        var failure: ExpectedError?

        let successTask = scope.run(
            operation: { 42 },
            success: { value = $0 },
            failure: { _ in XCTFail("unexpected failure") }
        )
        await successTask.value

        let failureTask = scope.run(
            operation: { () throws -> Int in throw ExpectedError.failure },
            success: { _ in XCTFail("unexpected success") },
            failure: { failure = $0 as? ExpectedError }
        )
        await failureTask.value

        XCTAssertEqual(value, 42)
        XCTAssertEqual(failure, .failure)
    }

    @MainActor
    func testInvalidationDiscardsAnOlderNativeCompletion() async {
        let started = expectation(description: "native work started")
        let release = DispatchSemaphore(value: 0)
        let scope = MykoOperationScope()
        var received = false

        let task = scope.run(
            operation: {
                started.fulfill()
                release.wait()
                return 42
            },
            receive: { _ in received = true }
        )

        await fulfillment(of: [started], timeout: 1)
        scope.invalidate()
        release.signal()
        await task.value

        XCTAssertFalse(received)
    }
}
