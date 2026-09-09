import Foundation
import NIOCore
import NIOSSH
import PhrenKit

extension MoshiConnection {
    /// The helper filters Codex lifecycle/usage rows out of its chat socket.
    /// A foreground-only reader supplements them over the same pinned SSH host.
    public static func chatProgress(host: LiveHost, privateKey: Data, target: AgentChatTarget) -> AsyncThrowingStream<AgentChatTranscript, Error> {
        AsyncThrowingStream(bufferingPolicy: .bufferingOldest(8)) { continuation in
            let worker = Task {
                do {
                    guard target.hostID == host.id, target.muxID == host.muxID else { throw PhrenKitError.validation("This chat belongs to another computer.") }
                    let command = try chatProgressCommand(target)
                    _ = try await fetchData(host: host, key: .init(rawRepresentation: privateKey),
                        request: .init(path: "", streaming: true, progressCommand: command)) { data in
                            let frame = try AgentChatTranscript.read(data, source: target.source)
                            if case .dropped = continuation.yield(frame) { throw LiveConnectionError.oversized }
                        }
                    continuation.finish()
                } catch { continuation.finish(throwing: error) }
            }
            continuation.onTermination = { _ in worker.cancel() }
        }
    }

    static func chatProgressCommand(_ target: AgentChatTarget) throws -> String {
        guard UUID(uuidString: target.sessionID) != nil else {
            throw PhrenKitError.validation("Token counters are unavailable for this conversation.")
        }
        return "phren-chat-progress " + target.source + " " + target.sessionID.lowercased()
    }

    static var progressReaderURL: URL { Bundle.module.url(forResource: "chat-progress", withExtension: "py")! }
}

/// NIO confines mutable state to the SSH channel's event loop.
final class ChatProgressFrames: ChannelInboundHandler {
    typealias InboundIn = SSHChannelData
    let exchange: Exchange
    let command: String
    private var pending = Data()
    init(exchange: Exchange, command: String) { self.exchange = exchange; self.command = command }

    func channelActive(context: ChannelHandlerContext) {
        context.triggerUserOutboundEvent(SSHChannelRequestEvent.ExecRequest(command: command, wantReply: true), promise: nil)
    }
    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        let value = unwrapInboundIn(data)
        // stderr can contain host paths; it is never shown in the conversation.
        guard value.type == .channel, case .byteBuffer(let buffer) = value.data else { return }
        pending.append(contentsOf: buffer.readableBytesView)
        guard pending.count <= 262_144 else { exchange.finish(.failure(LiveConnectionError.oversized)); return }
        while let end = pending.firstIndex(of: 10) {
            let frame = Data(pending[..<end]); pending.removeSubrange(...end)
            exchange.receive(frame)
        }
    }
    func userInboundEventTriggered(context: ChannelHandlerContext, event: Any) {
        if event is ChannelFailureEvent { exchange.finish(.failure(LiveConnectionError.disconnected)) }
        else { context.fireUserInboundEventTriggered(event) }
    }
    func errorCaught(context: ChannelHandlerContext, error: Error) { exchange.finish(.failure(error)) }
    func channelInactive(context: ChannelHandlerContext) { exchange.finish(.failure(LiveConnectionError.disconnected)) }
}
