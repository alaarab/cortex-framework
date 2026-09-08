import Foundation
import XCTest
@testable import PhrenKit

final class AgentChatTests: XCTestCase {
    func testAttachmentsUseGeneratedNamesAndRejectUnsafeOrOversizedData() throws {
        let item = try AgentAttachment(name: "../../a screenshot.PNG", data: Data([1, 2, 3]), isImage: true)
        XCTAssertTrue(item.uploadName.hasSuffix(".png"))
        XCTAssertFalse(item.uploadName.contains("/"))
        XCTAssertThrowsError(try AgentAttachment(name: "empty", data: Data()))
        XCTAssertThrowsError(try AgentAttachment(name: "huge", data: Data(repeating: 0, count: AgentAttachment.maximumBytes + 1)))
        XCTAssertEqual(try AgentAttachment.uploadedPath(from: Data(#"{"ok":true,"path":"/tmp/moshi-upload-fixture/image.png"}"#.utf8)), "/tmp/moshi-upload-fixture/image.png")
        XCTAssertThrowsError(try AgentAttachment.uploadedPath(from: Data(#"{"ok":true,"path":"/tmp/image\ncommand"}"#.utf8)))
    }

    func testStreamMergesOlderPagesAndReconnectsWithoutDuplicates() throws {
        func page(_ kind: String, _ lines: [Int], total: Int = 12) throws -> AgentChatTranscript {
            let rows = lines.map { line in ["line": line, "raw": ["type": "response_item", "payload": ["type": "message", "role": "assistant", "content": "Message \(line)"]]] as [String: Any] }
            return try AgentChatTranscript.read(JSONSerialization.data(withJSONObject: ["type": kind, "source": "codex", "entries": rows, "startLine": lines.min() ?? 0, "totalLines": total, "hasMore": (lines.min() ?? 0) > 0]), source: "codex")
        }
        var history = AgentChatHistory()
        history.receive(try page("backlog", [8, 9]))
        history.receive(try page("append", [10, 11]))
        history.receive(try page("older", [0, 1, 2, 3]))
        history.receive(try page("backlog", [8, 9, 10, 11]))
        XCTAssertEqual(history.messages.map(\.line), [0, 1, 2, 3, 8, 9, 10, 11])
        XCTAssertEqual(history.startLine, 0)
        XCTAssertFalse(history.hasMore)
        history.receive(try page("backlog", [0, 1], total: 2))
        XCTAssertEqual(history.messages.map(\.line), [0, 1])
    }

    func testPaneIdentityRequiresTheExactAgentAndConversation() throws {
        let host = UUID()
        let list = try panes()
        let target = try list.panes[0].target(hostID: host, workspaceID: "w7", tabID: "w7:t1")
        XCTAssertEqual(try list.validate(target).id, "w7:p1")
        let replaced = try AgentChatTarget(hostID: host, workspaceID: "w7", tabID: "w7:t1", paneID: "w7:p1", source: "codex", sessionID: "different-session")
        XCTAssertThrowsError(try list.validate(replaced))
        let otherPane = try AgentChatTarget(hostID: host, workspaceID: "w7", tabID: "w7:t1", paneID: "w7:p2", source: "codex", sessionID: "session-one")
        XCTAssertThrowsError(try list.validate(otherPane))
        XCTAssertThrowsError(try panes(status: "blocked").validate(target, sending: true))
        XCTAssertThrowsError(try AgentChatTarget(hostID: host, workspaceID: "w7&tab=w8", tabID: "w7:t1", paneID: "w7:p1", source: "codex", sessionID: "session-one"))
    }

    func testHistoryLimitStaysClosedAfterAReconnect() throws {
        func page(_ lines: Range<Int>) throws -> AgentChatTranscript {
            let rows = lines.map { line in ["line": line, "raw": ["type": "response_item", "payload": ["type": "message", "role": "assistant", "content": "Message \(line)"]]] as [String: Any] }
            return try AgentChatTranscript.read(JSONSerialization.data(withJSONObject: ["type": "backlog", "source": "codex", "entries": rows, "startLine": lines.lowerBound, "totalLines": 5_000, "hasMore": true]), source: "codex")
        }
        var history = AgentChatHistory()
        for start in stride(from: 0, to: 5_000, by: 1_000) { history.receive(try page(start..<(start + 1_000))) }
        XCTAssertEqual(history.messages.count, 4_000)
        XCTAssertTrue(history.reachedLimit)
        XCTAssertFalse(history.hasMore)
        history.receive(try page(1_000..<2_000))
        XCTAssertFalse(history.hasMore)
    }

    func testPaneListsRejectMismatchedLocationsAndDuplicateIDs() throws {
        let data = Data(#"{"kind":"herdr","groupId":"w7","childId":"w7:t1","panes":[{"id":"w7:p1","label":"1"},{"id":"w7:p1","label":"2"}]}"#.utf8)
        XCTAssertThrowsError(try AgentChatPanes.read(data, workspaceID: "w8", tabID: "w8:t1"))
        XCTAssertThrowsError(try AgentChatPanes.read(data, workspaceID: "w7", tabID: "w7:t1"))
    }

    func testCodexMessagesAndToolsExcludeSystemAndEncryptedReasoning() throws {
        let rows: [[String: Any]] = [
            ["type": "response_item", "payload": ["type": "message", "role": "system", "content": [["type": "text", "text": "private setup"]]]],
            ["type": "response_item", "payload": ["type": "reasoning", "encrypted_content": "private reasoning"]],
            ["type": "response_item", "payload": ["type": "message", "role": "user", "content": [["type": "input_text", "text": "Fix the screen"]]]],
            ["type": "response_item", "payload": ["type": "custom_tool_call", "name": "apply_patch", "input": "Edit the view"]],
            ["type": "response_item", "payload": ["type": "custom_tool_call_output", "output": "Applied"]],
            ["type": "event_msg", "payload": ["type": "agent_message", "message": "Done"]],
            ["type": "response_item", "payload": ["type": "message", "role": "assistant", "content": [["type": "output_text", "text": "Done"]]]],
        ]
        let transcript = try AgentChatTranscript.read(frame(rows, source: "codex"), source: "codex")
        XCTAssertEqual(transcript.messages.map(\.role), [.user, .tool, .tool, .assistant])
        XCTAssertEqual(transcript.messages.map(\.text), ["Fix the screen", "Edit the view", "Applied", "Done"])
        XCTAssertTrue(transcript.hasMore)
        XCTAssertThrowsError(try AgentChatTranscript.read(frame(rows, source: "codex"), source: "claude"))
    }

    func testClaudeBlocksSeparateVisibleTextToolCallsAndResults() throws {
        let rows: [[String: Any]] = [
            ["type": "user", "message": ["role": "user", "content": "Review this"]],
            ["type": "assistant", "message": ["role": "assistant", "content": [["type": "thinking", "thinking": "hidden"], ["type": "text", "text": "Checking"], ["type": "tool_use", "name": "Read", "input": ["path": "app.swift"]]]]],
            ["type": "user", "message": ["role": "user", "content": [["type": "tool_result", "content": [["type": "text", "text": "File contents"]]]]]],
            ["type": "user", "isMeta": true, "message": ["role": "user", "content": "hook metadata"]],
        ]
        let value = try AgentChatTranscript.read(frame(rows, source: "claude"), source: "claude")
        XCTAssertEqual(value.messages.map(\.role), [.user, .assistant, .tool, .tool])
        XCTAssertEqual(value.messages.last?.text, "File contents")
        XCTAssertFalse(value.messages.contains { $0.text.contains("hidden") || $0.text.contains("metadata") })
    }

    private func panes(status: String = "idle") throws -> AgentChatPanes {
        let body: [String: Any] = ["kind": "herdr", "groupId": "w7", "childId": "w7:t1", "panes": [
            ["id": "w7:p1", "label": "1", "agent": "codex", "agentStatus": status, "sessionId": "session-one"],
            ["id": "w7:p2", "label": "2", "agent": "claude", "sessionId": "session-two"],
        ]]
        return try AgentChatPanes.read(JSONSerialization.data(withJSONObject: body), workspaceID: "w7", tabID: "w7:t1")
    }
    private func frame(_ rows: [[String: Any]], source: String) throws -> Data {
        try JSONSerialization.data(withJSONObject: ["type": "backlog", "source": source, "entries": rows.enumerated().map { ["line": $0.offset, "raw": $0.element] }, "hasMore": true, "totalLines": rows.count])
    }
}
