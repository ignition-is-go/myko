/// Materializes lossless keyed Myko collection revisions for presentation.
///
/// Native bindings carry only the rows changed by a revision. This state owns
/// the authoritative client-side index so application stores do not each
/// reimplement reset, upsert, and removal semantics.
public struct MykoCollectionState<Element: Identifiable> where Element.ID: Hashable {
    private var elementsByID: [Element.ID: Element]

    public init() {
        elementsByID = [:]
    }

    /// The currently materialized rows. Collection order is intentionally not
    /// prescribed because it belongs to the consuming view.
    public var values: [Element] {
        Array(elementsByID.values)
    }

    /// Applies one typed collection revision and returns the new materialized rows.
    ///
    /// A reset discards the previous index before applying removals and upserts.
    /// Applying removals first lets an upsert in the same revision win, matching
    /// the ordered final state of a Myko batch.
    @discardableResult
    public mutating func apply(
        reset: Bool,
        upserts: [Element],
        removedIDs: [Element.ID]
    ) -> [Element] {
        if reset {
            elementsByID.removeAll(keepingCapacity: true)
        }
        removedIDs.forEach { elementsByID.removeValue(forKey: $0) }
        upserts.forEach { elementsByID[$0.id] = $0 }
        return values
    }

    /// Discards all materialized rows when their subscription scope closes.
    public mutating func clear() {
        elementsByID.removeAll(keepingCapacity: false)
    }
}
