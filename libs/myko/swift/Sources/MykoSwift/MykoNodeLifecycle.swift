import Dispatch

/// Stable presentation-facing phases of an embedded Myko node.
public enum MykoNodeLifecyclePhase: Sendable, Equatable {
    case stopped
    case starting
    case running
    case stopping
    case failed
}

/// A node lifecycle transition delivered on the main actor.
public enum MykoNodeLifecycleUpdate<Info> {
    case starting
    case running(Info)
    case stopping
    case stopped
    case failed(Error)
}

private enum MykoNodeWorkerState<Info> {
    case stopped
    case running(Info)
}

private enum MykoNodeWorkerResult<Info> {
    case running(Info)
    case stopped
    case failed(Error)
}

private final class MykoNodeLifecycleCompletion<Info>: @unchecked Sendable {
    private let receive: (MykoNodeWorkerResult<Info>) -> Void

    init(receive: @escaping (MykoNodeWorkerResult<Info>) -> Void) {
        self.receive = receive
    }

    func callAsFunction(_ result: MykoNodeWorkerResult<Info>) {
        receive(result)
    }
}

/// Serializes native start and stop calls away from the main actor.
private final class MykoNodeLifecycleWorker<Info>: @unchecked Sendable {
    private let queue: DispatchQueue
    private let startOperation: () throws -> Info
    private let stopOperation: () throws -> Void
    private var state = MykoNodeWorkerState<Info>.stopped

    init(
        label: String,
        start: @escaping () throws -> Info,
        stop: @escaping () throws -> Void
    ) {
        queue = DispatchQueue(label: label, qos: .userInitiated)
        startOperation = start
        stopOperation = stop
    }

    func request(
        active: Bool,
        completion: @escaping (MykoNodeWorkerResult<Info>) -> Void
    ) {
        let completion = MykoNodeLifecycleCompletion(receive: completion)
        queue.async { [self, completion] in
            completion(transition(active: active))
        }
    }

    private func transition(active: Bool) -> MykoNodeWorkerResult<Info> {
        switch (active, state) {
        case (true, .running(let info)):
            return .running(info)
        case (true, .stopped):
            do {
                let info = try startOperation()
                state = .running(info)
                return .running(info)
            } catch {
                return .failed(error)
            }
        case (false, .stopped):
            return .stopped
        case (false, .running):
            do {
                try stopOperation()
                state = .stopped
                return .stopped
            } catch {
                return .failed(error)
            }
        }
    }
}

/// Owns foreground/background lifecycle for one embedded Myko node.
///
/// Native FFI start and stop calls are serialized on a dedicated worker.
/// Superseded completions are discarded, so a rapid active/background/active
/// sequence cannot publish stale UI state or start and stop the node
/// concurrently. The application supplies only its typed node constructor and
/// maps main-actor updates into presentation state.
@MainActor
public final class MykoNodeLifecycle<Info> {
    public private(set) var phase = MykoNodeLifecyclePhase.stopped

    private let worker: MykoNodeLifecycleWorker<Info>
    private let receive: @MainActor (MykoNodeLifecycleUpdate<Info>) -> Void
    private var requestedActive: Bool?
    private var generation: UInt = 0

    public init(
        label: String = "myko.swift.node-lifecycle",
        start: @escaping () throws -> Info,
        stop: @escaping () throws -> Void,
        receive: @escaping @MainActor (MykoNodeLifecycleUpdate<Info>) -> Void
    ) {
        worker = MykoNodeLifecycleWorker(
            label: label,
            start: start,
            stop: stop
        )
        self.receive = receive
    }

    /// Reconciles the native node with the platform's desired active state.
    ///
    /// Repeating the current request is a no-op. A failed request may be
    /// retried by submitting the same desired state again.
    public func setActive(_ active: Bool) {
        guard requestedActive != active else { return }
        requestedActive = active
        generation &+= 1
        let requestGeneration = generation

        if active {
            phase = .starting
            receive(.starting)
        } else {
            phase = .stopping
            receive(.stopping)
        }

        worker.request(active: active) { [weak self] result in
            DispatchQueue.main.async { [weak self] in
                self?.finish(result, generation: requestGeneration)
            }
        }
    }

    private func finish(_ result: MykoNodeWorkerResult<Info>, generation: UInt) {
        guard self.generation == generation else { return }
        switch result {
        case .running(let info):
            phase = .running
            receive(.running(info))
        case .stopped:
            phase = .stopped
            receive(.stopped)
        case .failed(let error):
            requestedActive = nil
            phase = .failed
            receive(.failed(error))
        }
    }
}
