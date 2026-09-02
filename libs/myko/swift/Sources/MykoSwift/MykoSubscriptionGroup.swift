/// A type-erased subscription lifecycle owned by a presentation model.
@MainActor
public protocol MykoCancellableSubscription: AnyObject {
    func cancel()
}

extension MykoSubscriptionBinding: MykoCancellableSubscription {}

/// Retains heterogeneous Myko subscription bindings as one lifecycle unit.
///
/// A node-backed presentation model registers each static or dynamically
/// created binding once, then cancels the group before stopping its node. This
/// keeps application code independent of the concrete query, report, and view
/// update types while preserving their strongly typed bindings.
@MainActor
public final class MykoSubscriptionGroup {
    private var subscriptions: [ObjectIdentifier: any MykoCancellableSubscription] = [:]

    public init() {}

    /// Retains a subscription until it is removed or the group is cancelled.
    public func insert<Subscription: MykoCancellableSubscription>(_ subscription: Subscription) {
        subscriptions[ObjectIdentifier(subscription)] = subscription
    }

    /// Stops and removes one retained subscription.
    @discardableResult
    public func remove<Subscription: MykoCancellableSubscription>(
        _ subscription: Subscription
    ) -> Bool {
        let retained = subscriptions.removeValue(forKey: ObjectIdentifier(subscription))
        retained?.cancel()
        return retained != nil
    }

    /// Stops every retained subscription without discarding its registration.
    ///
    /// Static bindings can therefore be restarted after foreground activation
    /// and remain covered by the next group cancellation.
    public func cancelAll() {
        subscriptions.values.forEach { $0.cancel() }
    }

    /// Stops every retained subscription and releases the registrations.
    public func removeAll() {
        let retained = Array(subscriptions.values)
        subscriptions.removeAll(keepingCapacity: false)
        retained.forEach { $0.cancel() }
    }

    /// Number of distinct retained subscriptions.
    public var count: Int {
        subscriptions.count
    }
}
