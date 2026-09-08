import Crypto
import Foundation
import NIOCore
import NIOEmbedded
import NIOPosix
import NIOSSH
import NIOWebSocket
import PhrenKit
import XCTest
@testable import PhrenLive

final class AgentChatConnectionTests: XCTestCase {
    func testDifferentComputerRejectsBeforeConnectingOrUsingItsKey() async throws {
        let host = try LiveHost(name: "Other computer", address: "fixture.invalid", username: "fixture")
        let target = try AgentChatTarget(hostID: UUID(), workspaceID: "w7", tabID: "w7:t1", paneID: "w7:p1", source: "codex", sessionID: "fixture")
        do {
            try await MoshiConnection.sendChat(host: host, privateKey: Data(), target: target, text: "Must not send")
            XCTFail("A different computer must reject delivery")
        } catch { XCTAssertTrue(error.localizedDescription.contains("another computer")) }
        do {
            _ = try await MoshiConnection.chatTranscript(host: host, privateKey: Data(), target: target)
            XCTFail("A different computer must reject the transcript")
        } catch { XCTAssertTrue(error.localizedDescription.contains("another computer")) }
    }

    func testPromptEncodingKeepsTextOutOfTerminalCommands() throws {
        let target = try AgentChatTarget(hostID: UUID(), workspaceID: "w7", tabID: "w7:t1", paneID: "w7:p2", source: "claude", sessionID: "fixture")
        let text = "Review `file.swift`\n$(not-a-command) \"quoted\""
        let request = try GatewayRequest.prompt(target, text: text)
        let body = try XCTUnwrap(JSONSerialization.jsonObject(with: XCTUnwrap(request.body)) as? [String: String])
        XCTAssertEqual(request.path, "/v1/prompt")
        XCTAssertEqual(body, ["source": "claude", "sessionId": "fixture", "pane": "w7:p2", "tab": "w7:t1", "text": text])
    }

    func testFragmentedTranscriptAndPingFramesReassembleOneBoundedSnapshot() throws {
        let loop = EmbeddedEventLoop()
        let promise = loop.makePromise(of: Data.self)
        let exchange = Exchange(result: promise)
        let channel = EmbeddedChannel(handler: TranscriptFrames(exchange: exchange), loop: loop)
        try channel.writeInbound(WebSocketFrame(fin: false, opcode: .text, data: ByteBuffer(string: "{\"type\":")))
        try channel.writeInbound(WebSocketFrame(fin: true, opcode: .ping, data: ByteBuffer(string: "alive")))
        let pong = try XCTUnwrap(channel.readOutbound(as: WebSocketFrame.self))
        XCTAssertEqual(pong.opcode, .pong)
        XCTAssertNotNil(pong.maskKey)
        try channel.writeInbound(WebSocketFrame(fin: true, opcode: .continuation, data: ByteBuffer(string: "\"backlog\"}")))
        XCTAssertEqual(try promise.futureResult.wait(), Data(#"{"type":"backlog"}"#.utf8))
        _ = try channel.finish()
    }

    func testInvalidFrameSequenceAndOversizedTranscriptFailClosed() throws {
        for oversized in [false, true] {
            let loop = EmbeddedEventLoop()
            let promise = loop.makePromise(of: Data.self)
            let exchange = Exchange(result: promise)
            let channel = EmbeddedChannel(handler: TranscriptFrames(exchange: exchange), loop: loop)
            if oversized {
                try channel.writeInbound(WebSocketFrame(fin: false, opcode: .text, data: ByteBuffer(bytes: repeatElement(UInt8(65), count: 8_388_608))))
                try channel.writeInbound(WebSocketFrame(fin: true, opcode: .continuation, data: ByteBuffer(string: "x")))
            } else {
                try channel.writeInbound(WebSocketFrame(fin: true, opcode: .continuation, data: ByteBuffer(string: "x")))
            }
            XCTAssertThrowsError(try promise.futureResult.wait())
            _ = try channel.finish()
        }
    }

    /// Opt-in fixture: an inert echo executable in its own Herdr pane. The
    /// caller supplies its exact IDs; this never chooses or prompts a real agent.
    func testInstalledHelperThroughPinnedSSHReadsSendsAndReadsReply() async throws {
        guard let path = ProcessInfo.processInfo.environment["PHREN_CHAT_E2E_FIXTURE"] else {
            throw XCTSkip("Requires the disposable local echo fixture")
        }
        let metadata = try XCTUnwrap(JSONSerialization.jsonObject(with: Data(contentsOf: URL(fileURLWithPath: path))) as? [String: String])
        let server = try await ChatRelaySSH.start()
        defer { Task { try await server.close() } }
        let host = try server.host()
        let target = try AgentChatTarget(hostID: host.id, workspaceID: XCTUnwrap(metadata["workspace"]), tabID: XCTUnwrap(metadata["tab"]),
                                         paneID: XCTUnwrap(metadata["pane"]), source: metadata["source"] ?? "claude", sessionID: XCTUnwrap(metadata["session"]))
        let key = server.deviceKey.rawRepresentation
        let panes = try await MoshiConnection.chatPanes(host: host, privateKey: key, workspaceID: target.workspaceID, tabID: target.tabID)
        _ = try panes.validate(target)
        let before = try await MoshiConnection.chatTranscript(host: host, privateKey: key, target: target)
        XCTAssertTrue(before.messages.contains { $0.text == "Phren transport fixture ready." })
        let message = "Phren SSH fixture " + UUID().uuidString
        try await MoshiConnection.sendChat(host: host, privateKey: key, target: target, text: message)
        let after = try await MoshiConnection.chatTranscript(host: host, privateKey: key, target: target)
        XCTAssertTrue(after.messages.contains { $0.role == .user && $0.text == message })
        XCTAssertTrue(after.messages.contains { $0.role == .assistant && $0.text == "Echo: " + message })

        let wrong = try AgentChatTarget(hostID: host.id, workspaceID: target.workspaceID, tabID: target.tabID, paneID: target.paneID,
                                        source: target.source, sessionID: UUID().uuidString)
        do { try await MoshiConnection.sendChat(host: host, privateKey: key, target: wrong, text: "MUST NOT ARRIVE"); XCTFail("Changed identity must reject before delivery") }
        catch { XCTAssertTrue(error.localizedDescription.contains("changed")) }
        let final = try await MoshiConnection.chatTranscript(host: host, privateKey: key, target: target)
        XCTAssertFalse(final.messages.contains { $0.text.contains("MUST NOT ARRIVE") })
        try await server.close()
    }
}

/// SSH terminates in a test server with a fresh host/device key. Its only
/// forwarding destination is the already-installed loopback Moshi helper.
private final class ChatRelaySSH: @unchecked Sendable {
    let deviceKey: Curve25519.Signing.PrivateKey
    let hostKey: Curve25519.Signing.PrivateKey
    let listener: Channel
    init(deviceKey: Curve25519.Signing.PrivateKey, hostKey: Curve25519.Signing.PrivateKey, listener: Channel) {
        self.deviceKey = deviceKey; self.hostKey = hostKey; self.listener = listener
    }
    func host() throws -> LiveHost {
        try LiveHost(name: "Chat fixture", address: "127.0.0.1", port: listener.localAddress!.port!, username: "fixture",
                     fingerprint: MoshiConnection.fingerprint(publicKey: String(openSSHPublicKey: NIOSSHPrivateKey(ed25519Key: hostKey).publicKey)))
    }
    static func start() async throws -> ChatRelaySSH {
        let loop = MultiThreadedEventLoopGroup.singleton.next()
        let device = Curve25519.Signing.PrivateKey(), host = Curve25519.Signing.PrivateKey()
        let listener = try await ServerBootstrap(group: loop).childChannelInitializer { parent in
            parent.eventLoop.makeCompletedFuture {
                try parent.pipeline.syncOperations.addHandler(NIOSSHHandler(role: .server(.init(hostKeys: [.init(ed25519Key: host)], userAuthDelegate: ChatRelayAuth(key: device))),
                allocator: parent.allocator, inboundChildChannelInitializer: { child, type in
                    guard case .directTCPIP(let target) = type, target.targetHost == "127.0.0.1", target.targetPort == 24543 else {
                        return child.eventLoop.makeFailedFuture(LiveConnectionError.disconnected)
                    }
                    return ClientBootstrap(group: child.eventLoop).channelInitializer { tcp in
                        tcp.pipeline.addHandler(ChatRelayTCP(peer: child))
                    }.connect(host: "127.0.0.1", port: 24543).flatMap { tcp in
                        child.closeFuture.whenComplete { _ in tcp.close(promise: nil) }
                        return child.pipeline.addHandler(ChatRelayChild(peer: tcp))
                    }
                }))
            }
        }.bind(host: "127.0.0.1", port: 0).get()
        return ChatRelaySSH(deviceKey: device, hostKey: host, listener: listener)
    }
    func close() async throws { if listener.isActive { try await listener.close() } }
}
private final class ChatRelayAuth: NIOSSHServerUserAuthenticationDelegate, @unchecked Sendable {
    let key: NIOSSHPublicKey
    var supportedAuthenticationMethods: NIOSSHAvailableUserAuthenticationMethods { .publicKey }
    init(key: Curve25519.Signing.PrivateKey) { self.key = NIOSSHPrivateKey(ed25519Key: key).publicKey }
    func requestReceived(request: NIOSSHUserAuthenticationRequest, responsePromise: EventLoopPromise<NIOSSHUserAuthenticationOutcome>) {
        if request.username == "fixture", case .publicKey(let offered) = request.request, offered.publicKey == key { responsePromise.succeed(.success) }
        else { responsePromise.succeed(.failure) }
    }
}
private final class ChatRelayTCP: ChannelInboundHandler {
    typealias InboundIn = ByteBuffer
    let peer: Channel
    init(peer: Channel) { self.peer = peer }
    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        peer.writeAndFlush(SSHChannelData(type: .channel, data: .byteBuffer(unwrapInboundIn(data))), promise: nil)
    }
    func channelInactive(context: ChannelHandlerContext) { peer.close(promise: nil) }
}
private final class ChatRelayChild: ChannelInboundHandler {
    typealias InboundIn = SSHChannelData
    let peer: Channel
    init(peer: Channel) { self.peer = peer }
    func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        if case .byteBuffer(let buffer) = unwrapInboundIn(data).data { peer.writeAndFlush(buffer, promise: nil) }
    }
}
