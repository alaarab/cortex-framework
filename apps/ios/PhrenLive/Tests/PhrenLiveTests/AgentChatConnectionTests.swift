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
    func testNamedHerdrTerminalAndWorkspaceControlsThroughPinnedSSH() async throws {
        guard ProcessInfo.processInfo.environment["PHREN_HERDR_FIXTURE"] == "phren-phone-fixture" else { throw XCTSkip("Requires our isolated Herdr server") }
        let server = try await ChatRelaySSH.start()
        defer { Task { try await server.close() } }
        var host = try server.host(); host.herdrSession = "phren-phone-fixture"
        let key = server.deviceKey.rawRepresentation
        let muxes = try await MoshiConnection.herdrServers(host: host, privateKey: key)
        XCTAssertTrue(muxes.contains { $0.id == host.muxID })
        let before = try await MoshiConnection.fetch(host: host, privateKey: key)
        let label = "Phren SSH " + UUID().uuidString
        try await MoshiConnection.herdrAction(host: host, privateKey: key, operation: .create, label: label, cwd: "/tmp/phren-herdr-integration-fixture/repo")
        let created = try await MoshiConnection.fetch(host: host, privateKey: key)
        let group = try XCTUnwrap(created.groups.first { !before.groups.map(\.id).contains($0.id) })
        defer { Task { try? await MoshiConnection.herdrAction(host: host, privateKey: key, operation: .close, workspaceID: group.id) } }
        XCTAssertEqual(group.label, label)
        let tab = try XCTUnwrap(group.children.first)
        try await MoshiConnection.herdrAction(host: host, privateKey: key, operation: .rename, workspaceID: group.id, label: "Renamed phone fixture")
        let renamed = try await MoshiConnection.fetch(host: host, privateKey: key)
        XCTAssertEqual(renamed.groups.first { $0.id == group.id }?.label, "Renamed phone fixture")
        let panes = try await MoshiConnection.chatPanes(host: host, privateKey: key, workspaceID: group.id, tabID: tab.id)
        let pane = try XCTUnwrap(panes.panes.first)
        try await MoshiConnection.herdrAction(host: host, privateKey: key, operation: .focus, workspaceID: group.id, tabID: tab.id, paneID: pane.id)
        let socket = HerdrTerminalSocket()
        let ready = expectation(description: "Native PTY output"), echoed = expectation(description: "Input reached our shell")
        let marker = "PHREN_PTY_" + UUID().uuidString.replacingOccurrences(of: "-", with: "")
        let reader = Task {
            var received = false, didEcho = false, text = ""
            do {
                for try await bytes in MoshiConnection.herdrTerminal(host: host, privateKey: key, socket: socket) {
                    text += String(decoding: bytes, as: UTF8.self)
                    if !received { received = true; ready.fulfill() }
                    if text.contains(marker), !didEcho { didEcho = true; echoed.fulfill() }
                    try await socket.acknowledge(bytes.count)
                }
            } catch { if !Task.isCancelled { XCTFail("PTY failed: \(error)") } }
        }
        defer { reader.cancel() }
        await fulfillment(of: [ready], timeout: 10)
        try await socket.resize(columns: 72, rows: 30)
        // Only the workspace created above receives shell input.
        try await socket.input("printf 'PHREN_PTY_%s\\n' '\(marker.dropFirst(10))'\r")
        await fulfillment(of: [echoed], timeout: 10)
        reader.cancel(); await reader.value
        let afterDetach = try await MoshiConnection.chatPanes(host: host, privateKey: key, workspaceID: group.id, tabID: tab.id)
        XCTAssertTrue(afterDetach.panes.contains { $0.id == pane.id }, "Disconnect must preserve the shell")
        try await MoshiConnection.herdrAction(host: host, privateKey: key, operation: .close, workspaceID: group.id)
        let closed = try await MoshiConnection.fetch(host: host, privateKey: key)
        XCTAssertFalse(closed.groups.contains { $0.id == group.id })
        try await server.close()
    }

    func testRealHistoricalBlobAndIndependentRepositoryDiffSessions() async throws {
        guard let repo = ProcessInfo.processInfo.environment["PHREN_HERDR_FIXTURE_REPO"] else { throw XCTSkip("Requires the isolated repository fixture") }
        let server = try await ChatRelaySSH.start()
        defer { Task { try await server.close() } }
        let host = try server.host(), key = server.deviceKey
        let start = try await MoshiConnection.fetchData(host: host, key: key, request: .init(path: "/v1/diff/start", body: JSONSerialization.data(withJSONObject: ["cwd": repo])))
        let path = try AgentRepositoryDiff.statusPath(start)
        let data = try await MoshiConnection.fetchData(host: host, key: key, request: .init(path: path, maximumResponseBytes: 8_388_608))
        let diff = try AgentRepositoryDiff.read(data)
        XCTAssertTrue(diff.files.contains { $0.path == "example.txt" })
        XCTAssertTrue(diff.files.flatMap(\.sections).contains { $0.patch?.contains("Changed from the phone fixture") == true })
        _ = try await MoshiConnection.fetchData(host: host, key: key, request: .init(path: "/v1/diff/start", body: JSONSerialization.data(withJSONObject: ["cwd": "/Users/squidbot/Projects/phren"])))
        let still = try await MoshiConnection.fetchData(host: host, key: key, request: .init(path: path, maximumResponseBytes: 8_388_608))
        XCTAssertEqual(try AgentRepositoryDiff.read(still).root, diff.root)

        let id = UUID().uuidString.lowercased()
        let folder = FileManager.default.homeDirectoryForCurrentUser.appendingPathComponent(".codex/sessions/2026/09/08")
        try FileManager.default.createDirectory(at: folder, withIntermediateDirectories: true)
        let transcript = folder.appendingPathComponent("rollout-2026-09-08T00-00-00-\(id).jsonl")
        defer { try? FileManager.default.removeItem(at: transcript) }
        let image = try XCTUnwrap(Data(base64Encoded: "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+j3ioAAAAASUVORK5CYII="))
        let records: [[String: Any]] = [
            ["type": "session_meta", "payload": ["id": id, "cwd": repo, "timestamp": "2026-09-08T00:00:00Z"]],
            ["type": "response_item", "payload": ["type": "message", "role": "user", "content": [["type": "input_text", "text": "Fixture image"], ["type": "input_image", "image_url": "data:image/png;base64," + image.base64EncodedString()]]]]
        ]
        try Data(records.map { String(decoding: try! JSONSerialization.data(withJSONObject: $0), as: UTF8.self) }.joined(separator: "\n").appending("\n").utf8).write(to: transcript)
        let target = try AgentChatTarget(hostID: host.id, workspaceID: "w1", tabID: "w1:t1", paneID: "w1:p1", source: "codex", sessionID: id)
        let downloaded = try await MoshiConnection.transcriptImage(host: host, privateKey: key.rawRepresentation, target: target, line: 1, block: 1)
        XCTAssertEqual(downloaded, image)
        let stale = try JSONSerialization.data(withJSONObject: ["source": "codex", "sessionId": id, "actionId": "not-a-real-approval", "decision": "approve"])
        do {
            _ = try await MoshiConnection.fetchData(host: host, key: key, request: .init(path: "/v1/approvals/answer", body: stale))
            XCTFail("A missing approval must fail closed")
        } catch {
            guard case .gatewayRejection(status: 409, reason: _) = error as? LiveConnectionError else { return XCTFail("Expected the helper's stale-approval rejection: \(error)") }
        }
        try await server.close()
    }

    func testNamedServerIsolationBeforeTransportAndWorkspaceQueryScoping() async throws {
        var host = try LiveHost(name: "Fixture", address: "fixture.invalid", username: "fixture")
        host.herdrSession = "work"
        let target = try AgentChatTarget(hostID: host.id, workspaceID: "w1", tabID: "w1:t1", paneID: "w1:p1", source: "codex", sessionID: "fixture")
        do { _ = try await MoshiConnection.transcriptImage(host: host, privateKey: Data(), target: target, line: 0, block: 0); XCTFail("Must reject a different Herdr server") }
        catch { XCTAssertTrue(error.localizedDescription.contains("another computer or Herdr server")) }
        let scoped = GatewayRequest.panes("w1", "w1:t1").scoped(to: host)
        XCTAssertEqual(URLComponents(string: scoped.path)?.queryItems?.first { $0.name == "mux" }?.value, "herdr:work")
    }

    func testInstalledHelperStreamsHistoryUploadsAndStopsFixture() async throws {
        guard let path = ProcessInfo.processInfo.environment["PHREN_CHAT_ITERATION_FIXTURE"] else { throw XCTSkip("Requires the inert upload/stream/stop fixture") }
        let metadata = try XCTUnwrap(JSONSerialization.jsonObject(with: Data(contentsOf: URL(fileURLWithPath: path))) as? [String: String])
        let server = try await ChatRelaySSH.start()
        defer { Task { try await server.close() } }
        let host = try server.host(), key = server.deviceKey.rawRepresentation
        let target = try AgentChatTarget(hostID: host.id, workspaceID: XCTUnwrap(metadata["workspace"]), tabID: XCTUnwrap(metadata["tab"]), paneID: XCTUnwrap(metadata["pane"]), source: "codex", sessionID: XCTUnwrap(metadata["session"]))
        let initial = expectation(description: "Initial backlog"), reply = expectation(description: "Live reply"), stopped = expectation(description: "Stop reached exact fixture")
        let marker = "Image prompt " + UUID().uuidString
        let reader = Task {
            var gotInitial = false, gotReply = false, gotStop = false
            do {
                for try await frame in MoshiConnection.chatUpdates(host: host, privateKey: key, target: target) {
                    if frame.kind == .backlog && !gotInitial { gotInitial = true; initial.fulfill() }
                    if frame.kind == .append && frame.messages.contains(where: { $0.text.contains("Echo: " + marker) }) && !gotReply { gotReply = true; reply.fulfill() }
                    if frame.kind == .append && frame.messages.contains(where: { $0.text == "Fixture stop received" }) && !gotStop { gotStop = true; stopped.fulfill() }
                }
            } catch { if !Task.isCancelled { XCTFail("Stream failed: \(error)") } }
        }
        defer { reader.cancel() }
        await fulfillment(of: [initial], timeout: 8)
        let image = try AgentAttachment(name: "Screenshot.png", data: XCTUnwrap(Data(base64Encoded: "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+j3ioAAAAASUVORK5CYII=")), isImage: true)
        let uploaded = try await MoshiConnection.uploadChatAttachment(host: host, privateKey: key, target: target, attachment: image)
        let file = URL(fileURLWithPath: uploaded)
        XCTAssertEqual(file.lastPathComponent, image.uploadName)
        XCTAssertEqual(try Data(contentsOf: file), image.data)
        defer {
            if file.lastPathComponent == image.uploadName, file.deletingLastPathComponent().lastPathComponent.hasPrefix("moshi-upload-") {
                try? FileManager.default.removeItem(at: file)
                try? FileManager.default.removeItem(at: file.deletingLastPathComponent())
            }
        }
        try await MoshiConnection.sendChat(host: host, privateKey: key, target: target, text: marker + " " + uploaded)
        await fulfillment(of: [reply], timeout: 8)
        let history = try await MoshiConnection.chatHistory(host: host, privateKey: key, target: target, beforeLine: 8)
        XCTAssertEqual(history.kind, .older)
        XCTAssertTrue(history.messages.contains { $0.text == "Fixture message 0" })
        XCTAssertTrue(history.messages.allSatisfy { $0.line < 8 })
        try await MoshiConnection.stopChatTurn(host: host, privateKey: key, target: target)
        await fulfillment(of: [stopped], timeout: 8)
        reader.cancel(); await reader.value
        try await server.close()
    }

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
        do {
            let image = try AgentAttachment(name: "image.png", data: Data([1]), isImage: true)
            _ = try await MoshiConnection.uploadChatAttachment(host: host, privateKey: Data(), target: target, attachment: image)
            XCTFail("A different computer must reject an upload")
        } catch { XCTAssertTrue(error.localizedDescription.contains("another computer")) }
        do {
            try await MoshiConnection.stopChatTurn(host: host, privateKey: Data(), target: target)
            XCTFail("A different computer must reject stopping")
        } catch { XCTAssertTrue(error.localizedDescription.contains("another computer")) }
        do {
            for try await _ in MoshiConnection.chatUpdates(host: host, privateKey: Data(), target: target) { XCTFail("Must not subscribe") }
            XCTFail("A different computer must reject streaming")
        } catch { XCTAssertTrue(error.localizedDescription.contains("another computer")) }
    }

    func testPromptEncodingKeepsTextOutOfTerminalCommands() throws {
        let target = try AgentChatTarget(hostID: UUID(), workspaceID: "w7", tabID: "w7:t1", paneID: "w7:p2", source: "claude", sessionID: "fixture")
        let text = "Review `file.swift`\n$(not-a-command) \"quoted\""
        let request = try GatewayRequest.prompt(target, text: text)
        let body = try XCTUnwrap(JSONSerialization.jsonObject(with: XCTUnwrap(request.body)) as? [String: String])
        XCTAssertEqual(URLComponents(string: request.path)?.path, "/v1/prompt")
        XCTAssertEqual(URLComponents(string: request.path)?.queryItems, [URLQueryItem(name: "mux", value: target.muxID)])
        XCTAssertEqual(body, ["source": "claude", "pane": "w7:p2", "text": text])
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
        var host = try server.host()
        if let mux = metadata["mux"], mux.hasPrefix("herdr:") { host.herdrSession = String(mux.dropFirst(6)) }
        let target = try AgentChatTarget(hostID: host.id, workspaceID: XCTUnwrap(metadata["workspace"]), tabID: XCTUnwrap(metadata["tab"]),
                                         paneID: XCTUnwrap(metadata["pane"]), source: metadata["source"] ?? "claude", sessionID: XCTUnwrap(metadata["session"]), muxID: host.muxID)
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
                                        source: target.source, sessionID: UUID().uuidString, muxID: host.muxID)
        do { try await MoshiConnection.sendChat(host: host, privateKey: key, target: wrong, text: "MUST NOT ARRIVE"); XCTFail("Changed identity must reject before delivery") }
        catch { XCTAssertTrue(error.localizedDescription.contains("changed")) }
        let final = try await MoshiConnection.chatTranscript(host: host, privateKey: key, target: target)
        XCTAssertFalse(final.messages.contains { $0.text.contains("MUST NOT ARRIVE") })
        try await server.close()
    }
}

/// SSH terminates in a test server with a fresh host/device key. Its only
/// forwarding destination defaults to the installed helper; web tests provide
/// an explicit loopback port map for isolated application fixtures.
final class ChatRelaySSH: @unchecked Sendable {
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
    static func start(forwardPorts: [Int: Int] = [24543: 24543]) async throws -> ChatRelaySSH {
        let loop = MultiThreadedEventLoopGroup.singleton.next()
        let device = Curve25519.Signing.PrivateKey(), host = Curve25519.Signing.PrivateKey()
        let listener = try await ServerBootstrap(group: loop).childChannelInitializer { parent in
            parent.eventLoop.makeCompletedFuture {
                try parent.pipeline.syncOperations.addHandler(NIOSSHHandler(role: .server(.init(hostKeys: [.init(ed25519Key: host)], userAuthDelegate: ChatRelayAuth(key: device))),
                allocator: parent.allocator, inboundChildChannelInitializer: { child, type in
                    guard case .directTCPIP(let target) = type, target.targetHost == "127.0.0.1", let port = forwardPorts[target.targetPort] else {
                        return child.eventLoop.makeFailedFuture(LiveConnectionError.disconnected)
                    }
                    return ClientBootstrap(group: child.eventLoop).channelInitializer { tcp in
                        tcp.pipeline.addHandler(ChatRelayTCP(peer: child))
                    }.connect(host: "127.0.0.1", port: port).flatMap { tcp in
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
