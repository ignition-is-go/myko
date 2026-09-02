import MykoSwift
import XCTest

final class MykoBlockingCallTests: XCTestCase {
    private enum TestError: Error, Equatable {
        case expected
    }

    func testReturnsBlockingWorkResult() async throws {
        let value = try await MykoBlockingCall.run { 42 }

        XCTAssertEqual(value, 42)
    }

    func testPropagatesBlockingWorkFailure() async {
        do {
            let _: Int = try await MykoBlockingCall.run {
                throw TestError.expected
            }
            XCTFail("the blocking call should fail")
        } catch let error as TestError {
            XCTAssertEqual(error, .expected)
        } catch {
            XCTFail("unexpected error: \(error)")
        }
    }
}
