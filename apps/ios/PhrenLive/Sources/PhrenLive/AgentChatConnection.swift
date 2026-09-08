import Crypto
import Foundation
import NIOCore
import NIOHTTP1
import NIOWebSocket
import PhrenKit

extension MoshiConnection {
    public static func chatPanes(host: LiveHost, privateKey: Data, workspaceID: String, tabID: String) async throws -> AgentChatPanes {
        guard AgentChatTarget.validID(workspaceID), AgentChatTarget.validID(tabID) else {
            throw PhrenKitError.validation("This workspace has no usable chat destination.")
        }
        let data = try await fetchData(host: host, key: .init(rawRepresentation: privateKey), request: .panes(workspaceID, tabID))
        return try AgentChatPanes.read(data, workspaceID: workspaceID, tabID: tabID)
    }

    /// A bounded recent-history snapshot from the hook's WebSocket, then close.
    /// The visible chat refreshes it; no transcript is persisted or synced.
    public static func chatTranscript(host: LiveHost, privateKey: Data, target: AgentChatTarget) async throws -> AgentChatTranscript {
        guard target.hostID == host.id else { throw PhrenKitError.validation("The chat belongs to another computer.") }
        let data = try await fetchData(host: host, key: .init(rawRepresentation: privateKey), request: .transcript(target))
        return try AgentChatTranscript.read(data, source: target.source)
    }

    public static func sendChat(host: LiveHost, privateKey: Data, target: AgentChatTarget, text: String) async throws {
        guard target.hostID == host.id else { throw PhrenKitError.validation("The chat belongs to another computer.") }
        guard !text.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty, text.utf8.count <= 32_768,
              !text.unicodeScalars.contains(where: { CharacterSet.controlCharacters.contains($0) && $0 != "\n" && $0 != "\t" }) else {
            throw PhrenKitError.validation("Enter a message up to 32 KB without terminal control characters.")
        }
        let panes = try await chatPanes(host: host, privateKey: privateKey, workspaceID: target.workspaceID, tabID: target.tabID)
        _ = try panes.validate(target, sending: true)
        try Task.checkCancellation()
        let request = try GatewayRequest.prompt(target, text: text)
        // Exactly one attempt. An interrupted reply must not replay terminal input.
        let data = try await fetchData(host: host, key: .init(rawRepresentation: privateKey), request: request)
        guard let result = try JSONSerialization.jsonObject(with: data) as? [String: Any], result["ok"] as? Bool == true else {
            throw PhrenKitError.validation("Delivery was not confirmed. Check the conversation before sending again.")
        }
    }
}

struct GatewayRequest: Sendable {
    let path: String
    var body: Data? = nil
    var webSocket = false
    static let workspaces = Self(path: "/v1/workspaces")
    static func panes(_ workspace: String, _ tab: String) -> Self {
        Self(path: path("/v1/workspaces/panes", ["groupId": workspace, "childId": tab]))
    }
    static func transcript(_ target: AgentChatTarget) -> Self {
        Self(path: path("/v1/transcripts", ["source": target.source, "session": target.sessionID, "limit": "200"]), webSocket: true)
    }
    static func prompt(_ target: AgentChatTarget, text: String) throws -> Self {
        Self(path: "/v1/prompt", body: try JSONSerialization.data(withJSONObject: [
            "source": target.source, "sessionId": target.sessionID, "pane": target.paneID,
            "tab": target.tabID, "text": text,
        ], options: [.sortedKeys]))
    }
    private static func path(_ path: String, _ query: [String: String]) -> String {
        var parts = URLComponents()
        parts.path = path
        parts.queryItems = query.sorted { $0.key < $1.key }.map { URLQueryItem(name: $0.key, value: $0.value) }
        return parts.string!
    }
}

func installTranscriptHandlers(channel: Channel, exchange: Exchange, request: GatewayRequest) -> EventLoopFuture<Void> {
    let handshake = TranscriptHandshake(exchange: exchange, path: request.path)
    let upgrader = NIOWebSocketClientUpgrader(maxFrameSize: 8_388_608, upgradePipelineHandler: { channel, _ in
        channel.pipeline.addHandler(TranscriptFrames(exchange: exchange))
    })
    let config: NIOHTTPClientUpgradeConfiguration = (upgraders: [upgrader], completionHandler: { context in
        context.pipeline.removeHandler(handshake, promise: nil)
    })
    return channel.pipeline.addHandler(SSHHTTPBytes()).flatMap {
        channel.pipeline.addHTTPClientHandlers(withClientUpgrade: config)
    }.flatMap { channel.pipeline.addHandler(handshake) }
}

// Handler state is confined to the channel's event loop.
private final class TranscriptHandshake: ChannelInboundHandler, RemovableChannelHandler, @unchecked Sendable {
    typealias InboundIn = HTTPClientResponsePart
    typealias OutboundOut = HTTPClientRequestPart
    let exchange: Exchange
    let path: String
    init(exchange: Exchange, path: String) { self.exchange = exchange; self.path = path }
    func channelActive(context: ChannelHandlerContext) {
        let head = HTTPRequestHead(version: .http1_1, method: .GET, uri: path,
                                   headers: HTTPHeaders([("Host", "127.0.0.1:24543")]))
        context.write(wrapOutboundOut(.head(head)), promise: nil)
        context.writeAndFlush(wrapOutboundOut(.end(nil))).whenFailure { [exchange] in exchange.finish(.failure($0)) }
    }
    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        if case .head(let head) = unwrapInboundIn(data) { exchange.finish(.failure(LiveConnectionError.response(Int(head.status.code)))) }
    }
    func errorCaught(context: ChannelHandlerContext, error: Error) { exchange.finish(.failure(error)) }
    func channelInactive(context: ChannelHandlerContext) { exchange.finish(.failure(LiveConnectionError.disconnected)) }
}

final class TranscriptFrames: ChannelInboundHandler, @unchecked Sendable {
    typealias InboundIn = WebSocketFrame
    typealias OutboundOut = WebSocketFrame
    let exchange: Exchange
    private var body = Data()
    private var receiving = false
    init(exchange: Exchange) { self.exchange = exchange }
    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        let frame = unwrapInboundIn(data)
        guard !exchange.finished else { return }
        switch frame.opcode {
        case .ping:
            context.writeAndFlush(wrapOutboundOut(WebSocketFrame(fin: true, opcode: .pong, maskKey: .random(), data: frame.data)), promise: nil)
            return
        case .pong: return
        case .text:
            guard !receiving else { exchange.finish(.failure(LiveConnectionError.disconnected)); return }
            receiving = true
        case .continuation:
            guard receiving else { exchange.finish(.failure(LiveConnectionError.disconnected)); return }
        default: exchange.finish(.failure(LiveConnectionError.disconnected)); return
        }
        guard body.count + frame.data.readableBytes <= 8_388_608 else {
            exchange.finish(.failure(LiveConnectionError.oversized)); return
        }
        body.append(contentsOf: frame.data.readableBytesView)
        if frame.fin { exchange.finish(.success(body)) }
    }
    func errorCaught(context: ChannelHandlerContext, error: Error) { exchange.finish(.failure(error)) }
    func channelInactive(context: ChannelHandlerContext) { exchange.finish(.failure(LiveConnectionError.disconnected)) }
}
