import Foundation
#if canImport(Security)
import Security

/// Stores one opaque Myko value in the Apple Keychain.
///
/// The caller supplies a stable service and account so applications can keep
/// node identities and other secrets in separate namespaces. Values never
/// leave the current device through backup or Keychain synchronization.
public struct MykoKeychainStore: Sendable {
    public enum StoreError: LocalizedError, Sendable {
        case invalidResult
        case status(OSStatus)

        public var errorDescription: String? {
            switch self {
            case .invalidResult:
                "Keychain returned an invalid value."
            case .status(let status):
                SecCopyErrorMessageString(status, nil) as String?
                    ?? "Keychain operation failed (\(status))."
            }
        }
    }

    public let service: String
    public let account: String

    public init(service: String, account: String) {
        self.service = service
        self.account = account
    }

    public func load() throws -> Data? {
        var query = baseQuery
        query[kSecReturnData as String] = true
        query[kSecMatchLimit as String] = kSecMatchLimitOne

        var result: CFTypeRef?
        switch SecItemCopyMatching(query as CFDictionary, &result) {
        case errSecSuccess:
            guard let data = result as? Data else {
                throw StoreError.invalidResult
            }
            return data
        case errSecItemNotFound:
            return nil
        case let status:
            throw StoreError.status(status)
        }
    }

    public func save(_ value: Data) throws {
        let values: [String: Any] = [
            kSecValueData as String: value,
            kSecAttrAccessible as String: kSecAttrAccessibleWhenUnlockedThisDeviceOnly,
        ]
        let updateStatus = SecItemUpdate(baseQuery as CFDictionary, values as CFDictionary)
        switch updateStatus {
        case errSecSuccess:
            return
        case errSecItemNotFound:
            var attributes = baseQuery
            values.forEach { attributes[$0.key] = $0.value }
            let addStatus = SecItemAdd(attributes as CFDictionary, nil)
            guard addStatus == errSecSuccess else {
                throw StoreError.status(addStatus)
            }
        case let status:
            throw StoreError.status(status)
        }
    }

    public func remove() throws {
        switch SecItemDelete(baseQuery as CFDictionary) {
        case errSecSuccess, errSecItemNotFound:
            return
        case let status:
            throw StoreError.status(status)
        }
    }

    private var baseQuery: [String: Any] {
        [
            kSecClass as String: kSecClassGenericPassword,
            kSecAttrService as String: service,
            kSecAttrAccount as String: account,
            kSecAttrSynchronizable as String: false,
        ]
    }
}
#endif
