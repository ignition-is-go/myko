import Dispatch
import Foundation

/// The surface generated subscription objects expose to Myko's Swift
/// lifecycle binding.
///
/// Application-specific UniFFI objects conform in the consuming application;
/// their update records remain fully typed. `cancel()` must wake any concurrent
/// call blocked in `next()`.
public protocol MykoBlockingSubscription: AnyObject {
    associatedtype Update

    func current() throws -> Update
    func next() throws -> Update
    func cancel()
}

/// Whether a received revision should keep its long-lived subscription open.
public enum MykoSubscriptionAction: Sendable {
    case keepAlive
    case finish
}

private final class MykoSubscriptionRun<Subscription: MykoBlockingSubscription>:
    @unchecked Sendable
{
    private let lock = NSLock()
    private var subscription: Subscription?
    private var cancelled = false

    var isActive: Bool {
        lock.lock()
        defer { lock.unlock() }
        return !cancelled
    }

    func install(_ subscription: Subscription) -> Bool {
        lock.lock()
        defer { lock.unlock() }
        guard !cancelled else { return false }
        self.subscription = subscription
        return true
    }

    func cancel() {
        let installed: Subscription?
        lock.lock()
        if cancelled {
            installed = nil
        } else {
            cancelled = true
            installed = subscription
            subscription = nil
        }
        lock.unlock()
        installed?.cancel()
    }
}

/// Owns one long-lived typed Myko subscription for a Swift presentation model.
///
/// Blocking FFI work runs on a dedicated serial queue. Revisions and failures
/// are delivered on the main actor, stale work is rejected after a restart,
/// and cancellation wakes the Rust-side `next()` call. This is the Swift
/// equivalent of Myko's non-visual Ratatui, GPUI, and Leptos bindings.
@MainActor
public final class MykoSubscriptionBinding<Subscription: MykoBlockingSubscription> {
    public typealias Update = Subscription.Update

    private let worker: DispatchQueue
    private var run: MykoSubscriptionRun<Subscription>?

    public init(label: String) {
        worker = DispatchQueue(label: label, qos: .userInitiated)
    }

    public convenience init() {
        self.init(label: "myko.swift.subscription")
    }

    deinit {
        run?.cancel()
    }

    /// Replaces the current subscription and begins consuming its revisions.
    ///
    /// The `receive` closure runs on the main actor. Return `.finish` for a
    /// terminal command report; return `.keepAlive` for a persistent query,
    /// report, or view.
    public func start(
        open: @escaping @Sendable () throws -> Subscription,
        receive: @escaping @MainActor (Update) -> MykoSubscriptionAction,
        failure: @escaping @MainActor (Error) -> Void
    ) {
        cancel()
        let nextRun = MykoSubscriptionRun<Subscription>()
        run = nextRun

        worker.async { [weak self] in
            do {
                let subscription = try open()
                guard nextRun.install(subscription) else {
                    subscription.cancel()
                    return
                }

                var update = try subscription.current()
                while nextRun.isActive {
                    let delivered = update
                    DispatchQueue.main.async { [weak self] in
                        guard let self, self.run === nextRun, nextRun.isActive else { return }
                        if receive(delivered) == .finish {
                            self.finish(nextRun)
                        }
                    }
                    update = try subscription.next()
                }
            } catch {
                guard nextRun.isActive else { return }
                DispatchQueue.main.async { [weak self] in
                    guard let self, self.run === nextRun, nextRun.isActive else { return }
                    self.finish(nextRun)
                    failure(error)
                }
            }
        }
    }

    /// Replaces the current subscription and ignores cancellation failures.
    public func start(
        open: @escaping @Sendable () throws -> Subscription,
        receive: @escaping @MainActor (Update) -> MykoSubscriptionAction
    ) {
        start(open: open, receive: receive, failure: { _ in })
    }

    /// Cancels the current subscription and rejects its already-queued updates.
    public func cancel() {
        let previous = run
        run = nil
        previous?.cancel()
    }

    /// Whether this binding currently owns a subscription run.
    public var isActive: Bool {
        run?.isActive == true
    }

    private func finish(_ finished: MykoSubscriptionRun<Subscription>) {
        guard run === finished else { return }
        run = nil
        finished.cancel()
    }
}
