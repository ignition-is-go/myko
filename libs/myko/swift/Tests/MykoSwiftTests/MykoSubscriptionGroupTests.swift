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
}
