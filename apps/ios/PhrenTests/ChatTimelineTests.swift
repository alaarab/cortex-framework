import XCTest
import PhrenKit
@testable import Phren

final class ChatTimelineTests: XCTestCase {
    func testGroupingRetainsEveryMessageAndNeverCrossesAReply() throws {
        let messages = try read([
            ["type": "function_call_output", "output": "Older result"],
            ["type": "message", "role": "assistant", "content": "Checking the change"],
            ["type": "function_call", "name": "exec_command", "arguments": "{\"cmd\":\"git diff\"}"],
            ["type": "function_call_output", "output": "Patch"],
            ["type": "function_call", "name": "exec_command", "arguments": "{\"cmd\":\"swift test\"}"],
            ["type": "message", "role": "user", "content": "Wait"],
            ["type": "function_call_output", "output": "Test output"]
        ])
        let groups = ChatTimelineEntry.group(messages)
        XCTAssertEqual(groups.flatMap(\.messages), messages)
        XCTAssertEqual(groups.map { $0.messages.count }, [1, 1, 3, 1, 1])
        XCTAssertEqual(groups.map(\.isActivity), [true, false, true, false, true])
        XCTAssertEqual(ChatToolSummary(groups[2].messages).count, 2)
        XCTAssertEqual(ChatToolSummary(groups[2].messages).preview, "swift test")
        XCTAssertTrue(ChatTimelineEntry.group([]).isEmpty)
    }

    func testAppendingResultsKeepsTheExpandedGroupIdentity() throws {
        let messages = try read([
            ["type": "function_call", "name": "exec_command", "arguments": "git status"],
            ["type": "function_call_output", "output": "Clean"],
            ["type": "function_call", "name": "exec_command", "arguments": "swift test"]
        ])
        XCTAssertEqual(ChatTimelineEntry.group(Array(messages.prefix(1))).first?.id, ChatTimelineEntry.group(messages).first?.id)
    }

    func testMixedAndResultOnlyGroupsHaveHonestSummaries() throws {
        let messages = try read([
            ["type": "function_call", "name": "exec_command", "arguments": "pwd"],
            ["type": "function_call", "name": "web_search", "arguments": "{\"query\":\"SwiftUI layout\"}"],
            ["type": "function_call_output", "output": "Search output"]
        ])
        let mixed = ChatToolSummary(messages)
        XCTAssertEqual(mixed.title, "Activity")
        XCTAssertEqual(mixed.count, 2)
        XCTAssertEqual(mixed.preview, "SwiftUI layout")
        let result = ChatToolSummary(Array(messages.suffix(1)))
        XCTAssertEqual(result.title, "Tool results")
        XCTAssertEqual(result.preview, "Search output")
        XCTAssertEqual(result.count, 1)
    }

    func testLongPreviewDoesNotTruncateTheActualCommand() throws {
        let command = String(repeating: "echo 👩🏽‍💻; ", count: 100)
        let arguments = String(decoding: try JSONSerialization.data(withJSONObject: ["cmd": command]), as: UTF8.self)
        let messages = try read([["type": "function_call", "name": "functions.exec_command", "arguments": arguments]])
        XCTAssertEqual(ChatToolSummary(messages).title, "Shell")
        XCTAssertEqual(ChatToolSummary(messages).preview, String(command.prefix(180)))
        XCTAssertEqual(ChatTimelineEntry.group(messages).first?.messages.first?.text, arguments)
    }

    private func read(_ payloads: [[String: Any]]) throws -> [AgentChatMessage] {
        try AgentChatTranscript.read(JSONSerialization.data(withJSONObject: ["type": "backlog", "source": "codex", "totalLines": payloads.count,
            "entries": payloads.enumerated().map { ["line": $0.offset, "raw": ["type": "response_item", "payload": $0.element]] }]), source: "codex").messages
    }
}
