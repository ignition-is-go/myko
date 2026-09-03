/// Coordinates an embedded Myko node with the reactive work that is valid
/// only while that node is active.
///
/// Applications declare subscriptions in `subscriptions` and run blocking
/// node commands through `operations`. The session opens every declared
/// subscription after node startup and, before publishing a stop or failure,
/// cancels those subscriptions and prevents stale command completions from
/// reaching presentation state.
@MainActor
public final class MykoNodeSession<Info> {
    public let subscriptions: MykoSubscriptionGroup
    public let operations: MykoOperationScope

    public private(set) var isActive = false

    private var lifecycle: MykoNodeLifecycle<Info>?
    private let receive: @MainActor (MykoNodeLifecycleUpdate<Info>) -> Void

    public init(
        label: String = "myko.swift.node-session",
        subscriptions: MykoSubscriptionGroup = MykoSubscriptionGroup(),
        operations: MykoOperationScope = MykoOperationScope(),
        start: @escaping () throws -> Info,
        stop: @escaping () throws -> Void,
        receive: @escaping @MainActor (MykoNodeLifecycleUpdate<Info>) -> Void
    ) {
        self.subscriptions = subscriptions
        self.operations = operations
        self.receive = receive
        lifecycle = MykoNodeLifecycle(
            label: label,
            start: start,
            stop: stop,
            receive: { [weak self] update in
                self?.apply(update)
            }
        )
    }

    /// The current serialized native-node lifecycle phase.
    public var phase: MykoNodeLifecyclePhase {
        lifecycle?.phase ?? .stopped
    }

    /// Reconciles the native node and all node-bound reactive work with the
    /// platform's desired active state.
    public func setActive(_ active: Bool) {
        lifecycle?.setActive(active)
    }

    private func apply(_ update: MykoNodeLifecycleUpdate<Info>) {
        switch update {
        case .running:
            isActive = true
            subscriptions.activate()
        case .starting, .stopping, .stopped, .failed:
            suspendNodeBoundWork()
        }
        receive(update)
    }

    private func suspendNodeBoundWork() {
        isActive = false
        operations.invalidate()
        if subscriptions.isActive {
            subscriptions.cancelAll()
        }
    }
}
