import Foundation

/// The result of restoring or creating one embedded native Myko node.
public struct MykoNodeBootstrapResult<Node> {
    public let node: Node
    public let storageDirectory: URL
    public let restoredIdentity: Bool

    public init(node: Node, storageDirectory: URL, restoredIdentity: Bool) {
        self.node = node
        self.storageDirectory = storageDirectory
        self.restoredIdentity = restoredIdentity
    }
}

/// Reusable persistence bootstrap for an embedded native Myko node.
///
/// The application supplies only its generated node constructors and chooses
/// an application-specific directory and secure-store namespace. Myko owns the
/// restore-or-create control flow so every native application preserves node
/// identity in the same fail-closed way.
public enum MykoNodeBootstrap {
    /// Resolves and creates an application-owned directory beneath the user's
    /// Application Support directory.
    public static func applicationSupportDirectory(
        named name: String,
        fileManager: FileManager = .default
    ) throws -> URL {
        let root = try fileManager.url(
            for: .applicationSupportDirectory,
            in: .userDomainMask,
            appropriateFor: nil,
            create: true
        )
        let directory = root.appendingPathComponent(name, isDirectory: true)
        try fileManager.createDirectory(
            at: directory,
            withIntermediateDirectories: true
        )
        return directory
    }

    /// Restores a node from its securely persisted identity, or creates a new
    /// node and persists its identity before returning it.
    ///
    /// A corrupt or incompatible stored identity is surfaced to the caller;
    /// it is never silently replaced with a different node identity.
    public static func loadOrCreate<Node>(
        storageDirectory: URL,
        identityStore: any MykoOpaqueValueStore,
        restore: (String, Data) throws -> Node,
        create: (String) throws -> Node,
        identity: (Node) throws -> Data
    ) throws -> MykoNodeBootstrapResult<Node> {
        let path = storageDirectory.path
        if let persistedIdentity = try identityStore.load() {
            return MykoNodeBootstrapResult(
                node: try restore(path, persistedIdentity),
                storageDirectory: storageDirectory,
                restoredIdentity: true
            )
        }

        let node = try create(path)
        try identityStore.save(try identity(node))
        return MykoNodeBootstrapResult(
            node: node,
            storageDirectory: storageDirectory,
            restoredIdentity: false
        )
    }
}
