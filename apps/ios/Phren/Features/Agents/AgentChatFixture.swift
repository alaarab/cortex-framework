#if DEBUG && targetEnvironment(simulator)
import Foundation
import PhrenKit
import PhrenLive

/// Isolated, in-memory conversations for UI tests; never active on an iPhone.
@MainActor enum AgentChatFixture {
    static var enabled: Bool { AppModel.isUITesting && ProcessInfo.processInfo.arguments.contains("--native-chat-fixture") }
    static var sent: [(String, String)] = []
    static var reads = 0
    static func panes(_ session: DiscoveredMoshiSession) throws -> AgentChatPanes {
        reads += 1
        if flag("--chat-offline") && reads > 1 { throw LiveConnectionError.disconnected }
        var panes: [[String: Any]] = [["id": "\(session.workspaceID):p1", "label": "1", "title": "Polish the phone app", "agent": "codex",
                                     "agentStatus": flag("--chat-blocked") ? "blocked" : "idle", "sessionId": "fixture-codex-session", "cwd": "/work/phone"]]
        if flag("--chat-multiple") {
            panes.append(["id": "\(session.workspaceID):p2", "label": "2", "title": "Review the changes", "agent": "claude", "agentStatus": "idle", "sessionId": "fixture-claude-session"])
        }
        return try AgentChatPanes.read(JSONSerialization.data(withJSONObject: ["kind": "herdr", "groupId": session.workspaceID, "childId": session.tab.id, "panes": panes]),
                                       workspaceID: session.workspaceID, tabID: session.tab.id)
    }
    static func transcript(_ target: AgentChatTarget) throws -> AgentChatTranscript {
        var entries: [[String: Any]] = []
        func append(_ role: String, _ text: String) {
            let raw: [String: Any] = target.source == "codex"
                ? ["type": "response_item", "payload": ["type": "message", "role": role, "content": [["type": "text", "text": text]]]]
                : ["type": role, "message": ["role": role, "content": [["type": "text", "text": text]]]]
            entries.append(["line": entries.count, "raw": raw])
        }
        append("user", "Can you refine the project screen?")
        append("assistant", target.source == "codex" ? "The project screen is ready. What would you like to change?" : "I reviewed the changes. The project navigation looks consistent.")
        for (id, text) in sent where id == target.id {
            append("user", text)
            append("assistant", "Received in \(target.source) on \(target.paneID): \(text)")
        }
        return try AgentChatTranscript.read(JSONSerialization.data(withJSONObject: ["type": "backlog", "source": target.source,
                                                                                    "entries": entries, "totalLines": entries.count, "hasMore": false]), source: target.source)
    }
    static func send(_ target: AgentChatTarget, text: String) async throws {
        try await Task.sleep(for: .milliseconds(250))
        if flag("--chat-send-fails") { throw LiveConnectionError.disconnected }
        sent.append((target.id, text))
    }
    private static func flag(_ flag: String) -> Bool { ProcessInfo.processInfo.arguments.contains(flag) }
}
#endif
