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
    public static func herdrTerminal(host: LiveHost, privateKey: Data, socket: HerdrTerminalSocket,
                                     columns: Int = 80, rows: Int = 24) -> HerdrTerminalOutput {
        let buffer = TerminalOutputBuffer()
        let signals = AsyncThrowingStream<Void, Error>(bufferingPolicy: .bufferingNewest(1)) { continuation in
            let task = Task {
                do {
                    let size = try JSONSerialization.data(withJSONObject: ["type": "resize", "cols": min(500, max(10, columns)), "rows": min(300, max(2, rows))])
                    let request = GatewayRequest(path: GatewayRequest.path("/v1/pty", ["mux": "herdr", "muxSession": host.herdrSession ?? "default"]),
                                                 webSocket: true, streaming: true,
                                                 initialMessages: [Data(#"{"type":"ack","bytes":0}"#.utf8), size], terminalSocket: socket)
                    _ = try await fetchData(host: host, key: .init(rawRepresentation: privateKey), request: request) { data in
                        try buffer.append(data)
                        // Only wakeups coalesce. Terminal bytes are never dropped:
                        // escape sequences and UTF-8 can span WebSocket messages.
                        continuation.yield(())
                    }
                    continuation.finish()
                } catch { continuation.finish(throwing: error) }
            }
            continuation.onTermination = { _ in task.cancel() }
        }
        return HerdrTerminalOutput(buffer: buffer, signals: signals)
    }
}

/// Single-consumer, byte-bounded output. The consumer acknowledges only bytes
/// it has rendered, so the helper's credit window applies backpressure.
public struct HerdrTerminalOutput: AsyncSequence, Sendable {
    public typealias Element = Data
    let buffer: TerminalOutputBuffer
    let signals: AsyncThrowingStream<Void, Error>
    public func makeAsyncIterator() -> AsyncIterator { AsyncIterator(buffer: buffer, signals: signals.makeAsyncIterator()) }
    public struct AsyncIterator: AsyncIteratorProtocol {
        let buffer: TerminalOutputBuffer
        var signals: AsyncThrowingStream<Void, Error>.AsyncIterator
        public mutating func next() async throws -> Data? {
            try Task.checkCancellation()
            if let bytes = buffer.take() { return bytes }
            while try await signals.next() != nil {
                try Task.checkCancellation()
                if let bytes = buffer.take() { return bytes }
            }
            return nil
        }
    }
}

final class TerminalOutputBuffer: @unchecked Sendable {
    private let lock = NSLock()
    private var pending = Data()
    private var offset = 0
    static let capacity = 1_048_576
    func append(_ bytes: Data) throws {
        lock.lock(); defer { lock.unlock() }
        guard bytes.count <= Self.capacity - (pending.count - offset) else { throw LiveConnectionError.oversized }
        if offset > 0 { pending = pending.subdata(in: offset..<pending.count); offset = 0 }
        pending.append(bytes)
    }
    func take() -> Data? {
        lock.lock(); defer { lock.unlock() }
        guard offset < pending.count else { return nil }
        let end = min(pending.count, offset + 65_536)
        let bytes = pending.subdata(in: offset..<end)
        offset = end
        if offset == pending.count { pending = Data(); offset = 0 }
        return bytes
    }
}
