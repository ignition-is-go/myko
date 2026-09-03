/// Reconciles a dynamic family of typed subscriptions by stable application key.
///
/// This is the native Swift equivalent of a keyed reactive collection: rows
/// which remain present retain their subscription identity, removed rows are
/// cancelled, and newly inserted rows start immediately when the parent
/// lifecycle is active. Applications supply only their typed open and update
/// projections.
@MainActor
public final class MykoKeyedSubscriptionGroup<
    Key: Hashable,
    Subscription: MykoBlockingSubscription
> {
    private let lifecycle: MykoSubscriptionGroup
    private var bindings: [Key: MykoSubscriptionBinding<Subscription>] = [:]

    public init(lifecycle: MykoSubscriptionGroup) {
        self.lifecycle = lifecycle
    }

    /// Keys with a retained subscription declaration.
    public var keys: Set<Key> {
        Set(bindings.keys)
    }

    /// Number of retained keyed subscriptions.
    public var count: Int {
        bindings.count
    }

    /// Makes the retained subscription set match the supplied elements.
    ///
    /// Duplicate keys are ignored after their first occurrence. An existing key
    /// keeps its current binding; callers should remove it first when changing
    /// the subscription scope represented by that key.
    public func reconcile<Element: Sendable>(
        _ elements: [Element],
        identifiedBy identify: (Element) -> Key,
        label: (Element) -> String,
        open: @escaping @Sendable (Element) throws -> Subscription,
        receive: @escaping @MainActor (Element, Subscription.Update) -> MykoSubscriptionAction,
        failure: @escaping @MainActor (Element, Error) -> Void
    ) {
        var desired: Set<Key> = []
        var unique: [(Key, Element)] = []
        for element in elements {
            let key = identify(element)
            if desired.insert(key).inserted {
                unique.append((key, element))
            }
        }

        for key in bindings.keys.filter({ !desired.contains($0) }) {
            remove(key)
        }

        for (key, element) in unique where bindings[key] == nil {
            let binding = MykoSubscriptionBinding<Subscription>(label: label(element))
            bindings[key] = binding
            lifecycle.register(
                binding,
                open: { try open(element) },
                receive: { update in receive(element, update) },
                failure: { error in failure(element, error) }
            )
        }
    }

    /// Cancels and forgets one keyed subscription.
    @discardableResult
    public func remove(_ key: Key) -> Bool {
        guard let binding = bindings.removeValue(forKey: key) else { return false }
        return lifecycle.remove(binding)
    }

    /// Cancels and forgets every keyed subscription.
    public func removeAll() {
        let retained = Array(bindings.values)
        bindings.removeAll(keepingCapacity: false)
        retained.forEach { lifecycle.remove($0) }
    }
}
