/// Owns asynchronous calls whose results are meaningful only for the current
/// lifetime of a Myko node or presentation scope.
///
/// UniFFI exposes synchronous Rust functions. `run` moves that blocking work
/// off the main actor through `MykoBlockingCall`, then delivers the result on
/// the main actor only if the scope has not been invalidated. The underlying
/// call is allowed to finish because arbitrary FFI work cannot be cancelled
/// safely; its stale result is discarded.
@MainActor
public final class MykoOperationScope {
    private var generation: UInt = 0

    public init() {}

    /// Prevents every operation started before this call from publishing a
    /// completion into the current presentation state.
    public func invalidate() {
        generation &+= 1
    }

    /// Runs blocking native work and delivers its result on the main actor.
    ///
    /// The returned task can be awaited by tests or callers that need explicit
    /// completion. Invalidated operations still finish their native work, but
    /// do not invoke `receive`.
    @discardableResult
    public func run<Value>(
        priority: MykoBlockingCall.Priority = .userInitiated,
        operation: @escaping () throws -> Value,
        receive: @escaping @MainActor (Result<Value, Error>) -> Void
    ) -> Task<Void, Never> {
        let operationGeneration = generation
        return Task { [weak self] in
            let result: Result<Value, Error>
            do {
                result = .success(
                    try await MykoBlockingCall.run(
                        priority: priority,
                        operation
                    )
                )
            } catch {
                result = .failure(error)
            }

            guard let self, self.generation == operationGeneration else { return }
            receive(result)
        }
    }

    /// Runs blocking native work with separate success and failure handlers.
    @discardableResult
    public func run<Value>(
        priority: MykoBlockingCall.Priority = .userInitiated,
        operation: @escaping () throws -> Value,
        success: @escaping @MainActor (Value) -> Void,
        failure: @escaping @MainActor (Error) -> Void
    ) -> Task<Void, Never> {
        run(priority: priority, operation: operation) { result in
            switch result {
            case .success(let value):
                success(value)
            case .failure(let error):
                failure(error)
            }
        }
    }
}
