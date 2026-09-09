import Foundation
import XCTest
@testable import PhrenKit

final class AgentChatProgressTests: XCTestCase {
    func testNewConversationCanHaveAnEmptyBacklogWithoutEntries() throws {
        let frame = try AgentChatTranscript.read(Data(#"{"source":"codex","type":"backlog","totalLines":1}"#.utf8), source: "codex")
        XCTAssertTrue(frame.messages.isEmpty); XCTAssertTrue(frame.progressEvents.isEmpty)
        for entries in ["null", "true", "{}"] {
            XCTAssertThrowsError(try AgentChatTranscript.read(Data("{\"source\":\"codex\",\"type\":\"backlog\",\"entries\":\(entries)}".utf8), source: "codex"))
        }
    }
    func testCodexPublishesTurnStateAndActualUsageWithoutExposingReasoning() throws {
        let frame = try frame("backlog", [
            row(2, "task_started", ["started_at": 1_700_000_000]),
            row(3, "agent_reasoning", ["text": "private reasoning"]),
            row(4, "token_count", ["info": ["last_token_usage": ["input_tokens": 1500, "cached_input_tokens": 1200, "output_tokens": 83]]]),
        ], total: 5)
        XCTAssertTrue(frame.messages.isEmpty)
        var progress = AgentChatProgress(); progress.receive(frame)
        XCTAssertEqual(progress.phase, .working)
        XCTAssertEqual(progress.startedAt, Date(timeIntervalSince1970: 1_700_000_000))
        XCTAssertEqual(progress.usage?.input, 1500)
        XCTAssertEqual(progress.usage?.output, 83)
        XCTAssertEqual(progress.usage?.cachedInput, 1200)
        progress.receive(try self.frame("append", [row(5, "task_complete", ["completed_at": 1_700_000_020])], total: 6))
        XCTAssertEqual(progress.phase, .finished)
        XCTAssertEqual(progress.finishedAt, Date(timeIntervalSince1970: 1_700_000_020))
    }

    func testOlderAndRepeatedFramesDoNotReplayProgressAndTruncationClearsIt() throws {
        var progress = AgentChatProgress()
        let begin = try frame("backlog", [row(8, "task_started", ["started_at": 1000]),
                                              row(9, "token_count", ["info": ["last_token_usage": ["input_tokens": 30, "output_tokens": 12]]])], total: 10)
        progress.receive(begin)
        progress.receive(try frame("append", [row(10, "turn_aborted")], total: 11))
        progress.receive(try frame("backlog", [row(8, "task_started", ["started_at": 1000])], total: 11))
        progress.receive(try frame("older", [row(2, "task_started")], total: 11))
        XCTAssertEqual(progress.phase, .stopped)
        progress.receive(try frame("append", [row(11, "task_started", ["started_at": 2000])], total: 12))
        XCTAssertNil(progress.usage)
        XCTAssertEqual(progress.startedAt, Date(timeIntervalSince1970: 2000))
        progress.receive(try frame("backlog", [], total: 1))
        XCTAssertNil(progress.phase); XCTAssertNil(progress.usage); XCTAssertNil(progress.startedAt)
    }

    func testClaudeUsageSkipsSidechainsAndMalformedCounters() throws {
        func claude(_ line: Int, usage: [String: Any], sidechain: Bool = false) -> [String: Any] {
            ["line": line, "raw": ["type": "assistant", "isSidechain": sidechain,
                "message": ["role": "assistant", "content": [], "usage": usage]]]
        }
        let valid: [String: Any] = ["input_tokens": 40, "cache_read_input_tokens": 25, "output_tokens": 9]
        let frames = try frame("backlog", [claude(0, usage: valid), claude(1, usage: valid, sidechain: true),
            claude(2, usage: ["input_tokens": -1, "output_tokens": 9]), claude(3, usage: ["input_tokens": true, "output_tokens": 2]),
            claude(4, usage: ["input_tokens": 1.5, "output_tokens": 9])], total: 5, source: "claude")
        XCTAssertEqual(frames.progressEvents.count, 1)
        var progress = AgentChatProgress(); progress.receive(frames)
        XCTAssertEqual(progress.usage?.output, 9); XCTAssertEqual(progress.usage?.cachedInput, 25)
        XCTAssertNil(progress.phase)
    }

    func testLiveStatusIncludesActivityAndModelOnlyForChosenConversation() throws {
        let target = try AgentChatTarget(hostID: UUID(), workspaceID: "w1", tabID: "w1:t1", paneID: "w1:p1", source: "codex", sessionID: "session")
        let data = Data(#"{"agentStatus":{"source":"codex","session":"session","status":"working","modelName":"Example model"}}"#.utf8)
        let status = try AgentInteractionStatus.read(data, target: target)
        XCTAssertEqual(status?.activity, "working"); XCTAssertEqual(status?.modelName, "Example model")
    }

    private func row(_ line: Int, _ type: String, _ fields: [String: Any] = [:]) -> [String: Any] {
        var payload = fields; payload["type"] = type
        return ["line": line, "raw": ["type": "event_msg", "payload": payload]]
    }
    private func frame(_ kind: String, _ rows: [[String: Any]], total: Int, source: String = "codex") throws -> AgentChatTranscript {
        try AgentChatTranscript.read(JSONSerialization.data(withJSONObject: ["type": kind, "source": source, "entries": rows, "totalLines": total]), source: source)
    }
}
