import XCTest
@testable import MykoSwift

final class MykoCollectionStateTests: XCTestCase {
    private struct Row: Identifiable, Equatable {
        let id: String
        let value: Int
    }

    func testMaterializesResetUpsertAndRemovalRevisions() {
        var state = MykoCollectionState<Row>()

        state.apply(
            reset: true,
            upserts: [Row(id: "one", value: 1), Row(id: "two", value: 2)],
            removedIDs: []
        )
        state.apply(
            reset: false,
            upserts: [Row(id: "one", value: 3), Row(id: "three", value: 4)],
            removedIDs: ["two"]
        )

        XCTAssertEqual(
            state.values.sorted { $0.id < $1.id },
            [Row(id: "one", value: 3), Row(id: "three", value: 4)]
        )
    }

    func testUpsertWinsWhenOneBatchAlsoRemovesTheSameIdentity() {
        var state = MykoCollectionState<Row>()
        state.apply(
            reset: true,
            upserts: [Row(id: "one", value: 1)],
            removedIDs: []
        )

        state.apply(
            reset: false,
            upserts: [Row(id: "one", value: 2)],
            removedIDs: ["one"]
        )

        XCTAssertEqual(state.values, [Row(id: "one", value: 2)])
    }

    func testClearReleasesTheMaterializedScope() {
        var state = MykoCollectionState<Row>()
        state.apply(
            reset: true,
            upserts: [Row(id: "one", value: 1)],
            removedIDs: []
        )

        state.clear()

        XCTAssertTrue(state.values.isEmpty)
    }
}
