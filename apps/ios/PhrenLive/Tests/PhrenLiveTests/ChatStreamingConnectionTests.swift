import Foundation
import NIOCore
import NIOSSH
import PhrenKit
import XCTest
@testable import PhrenLive

final class ChatStreamingConnectionTests: XCTestCase {
    func testInstalledHelperStreamsSeparateTextAndUsageUpdatesThroughSSH() async throws {
        guard ProcessInfo.processInfo.environment["PHREN_STREAM_E2E"] == "1" else { throw XCTSkip("Optional installed helper stream check") }
        let id = UUID().uuidString.lowercased()
        let directory = FileManager.default.homeDirectoryForCurrentUser.appendingPathComponent(".codex/sessions/2026/09/08")
        try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
        let path = directory.appendingPathComponent("rollout-2026-09-08T12-00-00-\(id).jsonl")
        defer { try? FileManager.default.removeItem(at: path) }
        func record(_ value: [String: Any]) throws -> Data {
            var data = try JSONSerialization.data(withJSONObject: value); data.append(10); return data
        }
        func append(_ value: [String: Any]) throws {
            let file = try FileHandle(forWritingTo: path); defer { try? file.close() }
            try file.seekToEnd(); try file.write(contentsOf: record(value))
        }
        try record(["type": "session_meta", "payload": ["id": id, "cwd": "/tmp", "timestamp": "2026-09-08T12:00:00Z"]]).write(to: path)
        let template = try AgentChatTarget(hostID: UUID(), workspaceID: "fixture", tabID: "fixture:t1", paneID: "fixture:p1", source: "codex", sessionID: id)
        let ssh = try await ChatRelaySSH.start(progressCommand: MoshiConnection.chatProgressCommand(template))
        defer { Task { try await ssh.close() } }
        let host = try ssh.host()
        let target = try AgentChatTarget(hostID: host.id, workspaceID: "fixture", tabID: "fixture:t1", paneID: "fixture:p1", source: "codex", sessionID: id)
        let ready = expectation(description: "Initial backlog"), text = expectation(description: "Live text"), usage = expectation(description: "Live token counters")
        let progressReady = expectation(description: "Initial progress snapshot")
        let reader = Task {
            var sawText = false
            for try await frame in MoshiConnection.chatUpdates(host: host, privateKey: ssh.deviceKey.rawRepresentation, target: target) {
                if frame.kind == .backlog { ready.fulfill() }
                if !sawText, frame.messages.contains(where: { $0.text == "Words arrive from the host." }) {
                    XCTAssertEqual(frame.kind, .append); sawText = true; text.fulfill()
                }
                if sawText { return }
            }
        }
        defer { reader.cancel() }
        let counters = Task {
            for try await frame in MoshiConnection.chatProgress(host: host, privateKey: ssh.deviceKey.rawRepresentation, target: target) {
                if frame.kind == .backlog { progressReady.fulfill() }
                if frame.progressEvents.contains(where: { if case .usage(let value) = $0.value { return value.output == 17 && value.input == 123 }; return false }) {
                    XCTAssertEqual(frame.kind, .append); usage.fulfill(); return
                }
            }
        }
        defer { counters.cancel() }
        await fulfillment(of: [ready, progressReady], timeout: 8)
        try append(["type": "response_item", "payload": ["type": "message", "role": "assistant", "content": "Words arrive from the host."]])
        await fulfillment(of: [text], timeout: 8)
        try append(["type": "event_msg", "payload": ["type": "token_count", "info": ["last_token_usage": ["input_tokens": 123, "output_tokens": 17]]]])
        await fulfillment(of: [usage], timeout: 8)
        try await reader.value
        try await counters.value
        try await ssh.close()
    }
}

/// Opt-in only, exact fixed-reader command, disposable session files, no agent.
final class ChatProgressFixtureProcess: ChannelInboundHandler {
    typealias InboundIn = SSHChannelData
    let command: String
    private var process: Process?
    private var output: Pipe?
    init(command: String) { self.command = command }
    func userInboundEventTriggered(context: ChannelHandlerContext, event: Any) {
        guard let request = event as? SSHChannelRequestEvent.ExecRequest,
              request.command == command || (command == "phren-copilot-chat" && request.command.hasPrefix("phren-copilot-chat ")),
              process == nil else {
            context.fireUserInboundEventTriggered(event); return
        }
        let process = Process(), pipe = Pipe(), channel = context.channel
        process.executableURL = URL(fileURLWithPath: "/usr/bin/env")
        process.arguments = ["python3", "-u", MoshiConnection.progressReaderURL.path]
        process.environment = ProcessInfo.processInfo.environment.merging(["SSH_ORIGINAL_COMMAND": request.command]) { _, new in new }
        process.standardInput = Pipe(); process.standardOutput = pipe; process.standardError = FileHandle.nullDevice
        self.process = process; output = pipe
        pipe.fileHandleForReading.readabilityHandler = { handle in
            let bytes = handle.availableData
            guard !bytes.isEmpty else { handle.readabilityHandler = nil; return }
            channel.eventLoop.execute {
                channel.writeAndFlush(SSHChannelData(type: .channel, data: .byteBuffer(ByteBuffer(bytes: bytes))), promise: nil)
            }
        }
        do {
            try process.run()
            context.triggerUserOutboundEvent(ChannelSuccessEvent(), promise: nil)
        } catch { context.close(promise: nil) }
    }
    func channelRead(context: ChannelHandlerContext, data: NIOAny) {}
    func channelInactive(context: ChannelHandlerContext) {
        output?.fileHandleForReading.readabilityHandler = nil
        if let process, process.isRunning { process.terminate() }
    }
}
