import Crypto
import Foundation
import PhrenKit

extension MoshiConnection {
    public static func webServers(host: LiveHost, privateKey: Data) async throws -> [WebServer] {
        let data = try await fetchData(host: host, key: .init(rawRepresentation: privateKey),
                                       request: GatewayRequest(path: "/events", webSocket: true))
        try Task.checkCancellation()
        return try WebServer.readSnapshot(data)
    }
}
