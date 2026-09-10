import Foundation
import PhrenKit
import XCTest
@testable import PhrenLive

final class CopilotConnectionTests: XCTestCase {
    func testDiscoveryRestoresCopilotWithoutChangingOtherTabs() throws {
        let workspaces = Data(#"{"kind":"herdr","groups":[{"id":"w2","label":"Work","children":[{"id":"w2:t1","label":"Copilot tab"},{"id":"w2:t2","label":"Other","agent":"codex","agentStatus":"working"}]}]}"#.utf8)
        let panes = Data(#"{"kind":"herdr","groupId":"w2","childId":"w2:t1","panes":[{"id":"w2:p1","label":"Pane"}]}"#.utf8)
        let extra = Data(#"{"panes":[{"id":"w2:p1","workspaceID":"w2","tabID":"w2:t1","label":"Copilot","agent":"copilot","agentStatus":"idle","sessionId":"00000000-0000-0000-0000-000000000023"},{"id":"w3:p1","workspaceID":"w3","tabID":"w3:t1","label":"Other computer","agent":"copilot"}]}"#.utf8)
        let list = try MoshiWorkspaces.read(CopilotDiscovery.mergeWorkspaces(workspaces, copilot: extra))
        XCTAssertEqual(list.groups.count, 1)
        XCTAssertEqual(list.groups[0].children.map(\.agent), ["copilot", "codex"])
        XCTAssertEqual(list.groups[0].children[1].agentStatus, "working")
        XCTAssertEqual(try CopilotDiscovery.mergeWorkspaces(workspaces, copilot: Data("unsupported".utf8)), workspaces)
        let resolved = try AgentChatPanes.read(CopilotDiscovery.mergePanes(panes, copilot: extra, workspace: "w2", tab: "w2:t1"), workspaceID: "w2", tabID: "w2:t1")
        XCTAssertEqual(resolved.panes.count, 1)
        XCTAssertEqual(resolved.panes[0].agent, "copilot")
        XCTAssertEqual(resolved.panes[0].sessionId, "00000000-0000-0000-0000-000000000023")
    }

    func testCopilotRequestEncodesLiteralTextAndExactServer() throws {
        let target = try AgentChatTarget(hostID: UUID(), workspaceID: "w1", tabID: "w1:t1", paneID: "w1:p1", source: "copilot", sessionID: UUID().uuidString.lowercased(), muxID: "herdr:work")
        let text = "/custom:command `literal` $(literal)\nmore"
        let request = try GatewayRequest.copilot(target, action: "send", text: text)
        let parts = try XCTUnwrap(request.progressCommand).split(separator: " ")
        XCTAssertEqual(parts.count, 2)
        XCTAssertEqual(parts[0], "phren-copilot-chat")
        let value = try JSONSerialization.jsonObject(with: XCTUnwrap(Data(base64Encoded: String(parts[1])))) as? [String: Any]
        XCTAssertEqual(value?["text"] as? String, text)
        XCTAssertEqual(value?["server"] as? String, "work")
        XCTAssertEqual(value?["pane"] as? String, "w1:p1")
    }

    func testInstalledBridgeDiscoversReadsSendsAndStreamsThroughPinnedSSH() async throws {
        guard let fixturePath = ProcessInfo.processInfo.environment["PHREN_COPILOT_E2E"] else { throw XCTSkip("Requires the isolated Copilot terminal fixture") }
        let fixture = try JSONDecoder().decode([String: String].self, from: Data(contentsOf: URL(fileURLWithPath: fixturePath)))
        guard fixture["server"] == "phren-copilot-fixture" else { return XCTFail("Use the isolated test server") }
        let relay = try await ChatRelaySSH.start(progressCommand: "phren-copilot-chat")
        defer { Task { try await relay.close() } }
        var host = try relay.host(); host.herdrSession = fixture["server"]
        let key = relay.deviceKey.rawRepresentation
        let workspace = try XCTUnwrap(fixture["workspace"]), tab = try XCTUnwrap(fixture["tab"])
        let snapshot = try await MoshiConnection.fetch(host: host, privateKey: key)
        XCTAssertTrue(snapshot.groups.flatMap(\.children).contains { $0.id == tab && $0.agent == "copilot" })
        let panes = try await MoshiConnection.chatPanes(host: host, privateKey: key, workspaceID: workspace, tabID: tab)
        let pane = try XCTUnwrap(panes.panes.first { $0.id == fixture["pane"] })
        let target = try pane.target(hostID: host.id, workspaceID: workspace, tabID: tab, muxID: host.muxID)
        XCTAssertEqual(target.sessionID, fixture["session"])
        let backlog = try await MoshiConnection.chatTranscript(host: host, privateKey: key, target: target)
        XCTAssertTrue(backlog.messages.contains { $0.text == "Copilot transport fixture ready." })
        let marker = "Phren SSH test " + UUID().uuidString
        let ready = expectation(description: "Copilot backlog"), received = expectation(description: "Copilot live reply")
        let reader = Task {
            for try await frame in MoshiConnection.chatUpdates(host: host, privateKey: key, target: target) {
                if frame.kind == .backlog { ready.fulfill() }
                if frame.messages.contains(where: { $0.role == .assistant && $0.text == marker }) { received.fulfill(); return }
            }
        }
        defer { reader.cancel() }
        await fulfillment(of: [ready], timeout: 8)
        try await MoshiConnection.sendChat(host: host, privateKey: key, target: target, text: marker)
        await fulfillment(of: [received], timeout: 8)
        reader.cancel()
        _ = try? await reader.value
        let stale = try AgentChatTarget(hostID: host.id, workspaceID: workspace, tabID: tab, paneID: target.paneID, source: "copilot", sessionID: UUID().uuidString.lowercased(), muxID: host.muxID)
        do {
            try await MoshiConnection.sendChat(host: host, privateKey: key, target: stale, text: "must not be sent")
            XCTFail("Replaced sessions must reject input")
        } catch { XCTAssertTrue(error.localizedDescription.contains("changed")) }
    }
}
