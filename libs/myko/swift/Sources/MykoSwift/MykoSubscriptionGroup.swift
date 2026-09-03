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
    private var starters: [ObjectIdentifier: @MainActor () -> Void] = [:]

    /// Whether lifecycle-managed subscriptions should currently be running.
    public private(set) var isActive = false

    public nonisolated init() {}

    /// Retains a subscription until it is removed or the group is cancelled.
    public func insert<Subscription: MykoCancellableSubscription>(_ subscription: Subscription) {
        subscriptions[ObjectIdentifier(subscription)] = subscription
    }

    /// Retains a typed subscription and declares how it is opened whenever its
    /// owning node or presentation scope becomes active.
    ///
    /// Registration is declarative: `activate()` starts every registered
    /// binding, `cancelAll()` stops them without
    /// discarding their declarations, and a later activation opens fresh
    /// subscriptions. Registering while active starts the new binding
    /// immediately.
    public func register<Subscription: MykoBlockingSubscription>(
        _ binding: MykoSubscriptionBinding<Subscription>,
        open: @escaping @Sendable () throws -> Subscription,
        receive: @escaping @MainActor (Subscription.Update) -> MykoSubscriptionAction,
        failure: @escaping @MainActor (Error) -> Void
    ) {
        let identifier = ObjectIdentifier(binding)
        subscriptions[identifier] = binding
        let start: @MainActor () -> Void = { [weak binding] in
            binding?.start(open: open, receive: receive, failure: failure)
        }
        starters[identifier] = start
        if isActive {
            start()
        }
    }

    /// Starts every declaratively registered subscription.
    ///
    /// Repeated activation is a no-op. Use `restart(_:)` to
    /// explicitly reopen one failed or manually refreshed binding.
    public func activate() {
        guard !isActive else { return }
        isActive = true
        starters.values.forEach { $0() }
    }

    /// Reopens one registered binding while this lifecycle group is active.
    @discardableResult
    public func restart<Subscription: MykoBlockingSubscription>(
        _ binding: MykoSubscriptionBinding<Subscription>
    ) -> Bool {
        let identifier = ObjectIdentifier(binding)
        guard isActive, subscriptions[identifier] != nil, let start = starters[identifier] else {
            return false
        }
        start()
        return true
    }

    /// Stops and removes one retained subscription.
    @discardableResult
    public func remove<Subscription: MykoCancellableSubscription>(
        _ subscription: Subscription
    ) -> Bool {
        let identifier = ObjectIdentifier(subscription)
        starters.removeValue(forKey: identifier)
        let retained = subscriptions.removeValue(forKey: identifier)
        retained?.cancel()
        return retained != nil
    }

    /// Stops every retained subscription without discarding its registration.
    ///
    /// Static bindings can therefore be restarted after foreground activation
    /// and remain covered by the next group cancellation.
    public func cancelAll() {
        isActive = false
        subscriptions.values.forEach { $0.cancel() }
    }

    /// Stops every retained subscription and releases the registrations.
    public func removeAll() {
        let retained = Array(subscriptions.values)
        isActive = false
        subscriptions.removeAll(keepingCapacity: false)
        starters.removeAll(keepingCapacity: false)
        retained.forEach { $0.cancel() }
    }

    /// Number of distinct retained subscriptions.
    public var count: Int {
        subscriptions.count
    }
}
