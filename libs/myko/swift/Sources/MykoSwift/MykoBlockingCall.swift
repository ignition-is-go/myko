import Dispatch

private final class MykoBlockingWork<Value>: @unchecked Sendable {
    private let operation: () throws -> Value
    private let continuation: CheckedContinuation<Value, Error>

    init(
        operation: @escaping () throws -> Value,
        continuation: CheckedContinuation<Value, Error>
    ) {
        self.operation = operation
        self.continuation = continuation
    }

    func run() {
        continuation.resume(with: Result { try operation() })
    }
}

/// Bridges a synchronous native Myko call into Swift structured concurrency.
///
/// UniFFI methods are synchronous even when they enter an async Rust runtime.
/// Running them through this helper keeps the caller's actor responsive and
/// resumes the awaiting task on its original executor.
public enum MykoBlockingCall {
    public enum Priority: Sendable {
        case userInitiated
        case utility

        fileprivate var qos: DispatchQoS.QoSClass {
            switch self {
            case .userInitiated:
                .userInitiated
            case .utility:
                .utility
            }
        }
    }

    public static func run<Value>(
        priority: Priority = .userInitiated,
        _ operation: @escaping () throws -> Value
    ) async throws -> Value {
        try await withCheckedThrowingContinuation { continuation in
            let work = MykoBlockingWork(
                operation: operation,
                continuation: continuation
            )
            DispatchQueue.global(qos: priority.qos).async {
                work.run()
            }
        }
    }
}
