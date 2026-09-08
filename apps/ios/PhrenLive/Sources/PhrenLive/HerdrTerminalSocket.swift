import Foundation
import NIOCore
import NIOWebSocket
import PhrenKit

/// One Herdr client in the configured server, carried by the existing SSH key.
/// Closing this connection detaches the client; it does not kill remote panes.
public final class HerdrTerminalSocket: @unchecked Sendable {
    private let lock = NSLock()
    private var channel: Channel?
    func attach(_ channel: Channel) { lock.lock(); self.channel = channel; lock.unlock() }
    private func current() -> Channel? { lock.lock(); defer { lock.unlock() }; return channel }
    public init() {}

    public func input(_ text: String) async throws {
        guard !text.isEmpty, text.utf8.count <= 65_536 else { return }
        try await send(["type": "input", "data": text])
    }
    public func resize(columns: Int, rows: Int) async throws {
        guard (10...500).contains(columns), (2...300).contains(rows) else { return }
        try await send(["type": "resize", "cols": columns, "rows": rows])
    }
    public func acknowledge(_ bytes: Int) async throws {
        guard bytes >= 0, bytes <= 8_388_608 else { return }
        try await send(["type": "ack", "bytes": bytes])
    }
    private func send(_ value: [String: Any]) async throws {
        let data = try JSONSerialization.data(withJSONObject: value)
        guard let channel = current(), channel.isActive else { throw LiveConnectionError.disconnected }
        try await channel.writeAndFlush(WebSocketFrame(fin: true, opcode: .text, maskKey: .random(), data: ByteBuffer(bytes: data))).get()
    }
}

extension MoshiConnection {
    public static func herdrTerminal(host: LiveHost, privateKey: Data, socket: HerdrTerminalSocket) -> AsyncThrowingStream<Data, Error> {
        AsyncThrowingStream(bufferingPolicy: .bufferingOldest(8)) { continuation in
            let task = Task {
                do {
                    let request = GatewayRequest(path: GatewayRequest.path("/v1/pty", ["mux": "herdr", "muxSession": host.herdrSession ?? "default"]),
                                                 webSocket: true, streaming: true,
                                                 initialMessages: [Data(#"{"type":"ack","bytes":0}"#.utf8), Data(#"{"type":"resize","cols":80,"rows":24}"#.utf8)], terminalSocket: socket)
                    _ = try await fetchData(host: host, key: .init(rawRepresentation: privateKey), request: request) { data in
                        if case .dropped = continuation.yield(data) { throw LiveConnectionError.oversized }
                    }
                    continuation.finish()
                } catch { continuation.finish(throwing: error) }
            }
            continuation.onTermination = { _ in task.cancel() }
        }
    }
}
