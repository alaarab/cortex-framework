import Foundation
import PhrenKit
import XCTest
@testable import PhrenLive

final class PhrenHookEndToEndTests: XCTestCase {
    /// Read-only opt-in check against the caller's exact live pane through the
    /// installed helper. No prompt or terminal input reaches that conversation.
    func testInstalledHelperReadsLiveConversationThroughPinnedSSH() async throws {
        let env = ProcessInfo.processInfo.environment
        guard env["PHREN_HOOK_LIVE_READ"] == "1", let fixture = env["PHREN_HOOK_SSH_FIXTURE"],
              let workspace = env["HERDR_WORKSPACE_ID"], let tab = env["HERDR_TAB_ID"], let paneID = env["HERDR_PANE_ID"] else {
            throw XCTSkip("Optional read-only installed helper check")
        }
        let directory = URL(fileURLWithPath: fixture)
        let key = try Data(contentsOf: directory.appendingPathComponent("device.raw"))
        let publicKey = try String(contentsOf: directory.appendingPathComponent("host_key.pub"), encoding: .utf8)
        let host = try LiveHost(name: "Installed helper", address: "127.0.0.1", port: 22866, username: NSUserName(),
                               fingerprint: PhrenConnection.fingerprint(publicKey: publicKey))
        let panes = try await PhrenConnection.chatPanes(host: host, privateKey: key, workspaceID: workspace, tabID: tab)
        let pane = try XCTUnwrap(panes.panes.first { $0.id == paneID })
        let target = try pane.target(hostID: host.id, workspaceID: workspace, tabID: tab, muxID: host.muxID)
        let transcript = try await PhrenConnection.chatTranscript(host: host, privateKey: key, target: target)
        XCTAssertEqual(transcript.kind, .backlog)
        XCTAssertFalse(transcript.messages.isEmpty)
        XCTAssertGreaterThan(transcript.totalLines, 0)
    }

    /// Uses a disposable sshd and named Herdr server. No user's agent receives input.
    func testStandaloneHookAndSSHPTYWithSlowRenderer() async throws {
        guard let fixture = ProcessInfo.processInfo.environment["PHREN_HOOK_SSH_FIXTURE"] else {
            throw XCTSkip("Requires the isolated Phren Hook SSH fixture")
        }
        let directory = URL(fileURLWithPath: fixture)
        let key = try Data(contentsOf: directory.appendingPathComponent("device.raw"))
        let publicKey = try String(contentsOf: directory.appendingPathComponent("host_key.pub"), encoding: .utf8)
        let host = try LiveHost(name: "Phren Hook fixture", address: "127.0.0.1", port: 22866, username: NSUserName(),
                               fingerprint: PhrenConnection.fingerprint(publicKey: publicKey), herdrSession: "phren-hook-standalone")
        let before = try await PhrenConnection.fetch(host: host, privateKey: key)
        try await PhrenConnection.herdrAction(host: host, privateKey: key, operation: .create, label: "Phren SSH verification", cwd: "/tmp")
        let after = try await PhrenConnection.fetch(host: host, privateKey: key)
        let workspace = try XCTUnwrap(after.groups.first { !before.groups.map(\.id).contains($0.id) })
        defer { Task { try? await PhrenConnection.herdrAction(host: host, privateKey: key, operation: .close, workspaceID: workspace.id) } }
        try await PhrenConnection.herdrAction(host: host, privateKey: key, operation: .focus, workspaceID: workspace.id)
        let socket = HerdrTerminalSocket()
        let ready = expectation(description: "SSH PTY output"), echoed = expectation(description: "Output after resize and input")
        let idleMarker = "AFTER_IDLE_" + UUID().uuidString.replacingOccurrences(of: "-", with: "")
        let trace = directory.appendingPathComponent("terminal-output.bin")
        FileManager.default.createFile(atPath: trace.path, contents: nil)
        let traceFile = try FileHandle(forWritingTo: trace); defer { try? traceFile.close() }
        let idle = expectation(description: "Terminal remains interactive after idle")
        let marker = "PHREN_STANDALONE_" + UUID().uuidString.replacingOccurrences(of: "-", with: "")
        let reader = Task {
            var first = true, tail = "", sawEcho = false, sawIdle = false
            do {
                for try await bytes in PhrenConnection.herdrTerminal(host: host, privateKey: key, socket: socket, columns: 80, rows: 30) {
                    try traceFile.write(contentsOf: bytes)
                    if first { first = false; ready.fulfill() }
                    tail = String((tail + String(decoding: bytes, as: UTF8.self)).suffix(100_000))
                    if tail.contains(marker), !sawEcho { sawEcho = true; echoed.fulfill() }
                    if tail.contains(String(idleMarker.suffix(12))), !sawIdle { sawIdle = true; idle.fulfill() }
                    try await Task.sleep(for: .milliseconds(100))
                    try await socket.acknowledge(bytes.count)
                }
            } catch { if !Task.isCancelled { XCTFail("Standalone terminal failed: \(error)") } }
        }
        defer { reader.cancel() }
        await fulfillment(of: [ready], timeout: 15)
        try await socket.resize(columns: 120, rows: 45)
        // Burst output exceeds receive credit while the renderer deliberately lags.
        try await socket.input("python3 -c \"import sys; [sys.stdout.write('x'*200+'\\n') for _ in range(20000)]\"\r")
        // The full marker appears only in output, never in the entered command.
        try await socket.input("printf 'PHREN_STANDALONE_%s\\n' '\(marker.dropFirst(17))'\r")
        await fulfillment(of: [echoed], timeout: 40)
        try await Task.sleep(for: .seconds(46))
        try await socket.input("printf 'AFTER_IDLE_%s\\n' '\(idleMarker.dropFirst(11))'\r")
        await fulfillment(of: [idle], timeout: 15)
        reader.cancel(); await reader.value
        let retained = try await PhrenConnection.fetch(host: host, privateKey: key)
        XCTAssertTrue(retained.groups.contains { $0.id == workspace.id })
        try await PhrenConnection.herdrAction(host: host, privateKey: key, operation: .close, workspaceID: workspace.id)
    }
}
