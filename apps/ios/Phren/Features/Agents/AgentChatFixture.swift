#if DEBUG && targetEnvironment(simulator)
import Foundation
import PhrenKit
import PhrenLive
import UIKit

/// Isolated, in-memory conversations for UI tests; never active on an iPhone.
@MainActor enum AgentChatFixture {
    static var enabled: Bool { AppModel.isUITesting && ProcessInfo.processInfo.arguments.contains("--native-chat-fixture") }
    static var sent: [(String, String)] = []
    static var reads = 0
    static var stopped = false
    static var uploads = 0
    static var image: AgentAttachment {
        let data = UIGraphicsImageRenderer(size: CGSize(width: 120, height: 90)).pngData { context in
            UIColor.cyan.setFill(); context.fill(CGRect(x: 0, y: 0, width: 120, height: 90))
        }
        return try! AgentAttachment(name: "Screenshot.png", data: data, isImage: true)
    }
    static func upload(_ attachment: AgentAttachment) throws -> String {
        uploads += 1
        if flag("--chat-upload-fails") { throw LiveConnectionError.disconnected }
        return "/tmp/phren-fixture/" + attachment.uploadName
    }
    static func older(_ target: AgentChatTarget) throws -> AgentChatTranscript {
        let raw: [String: Any] = ["type": "response_item", "payload": ["type": "message", "role": "assistant", "content": [["type": "output_text", "text": "Earlier project discussion"]]]]
        return try AgentChatTranscript.read(JSONSerialization.data(withJSONObject: ["type": "older", "source": target.source, "entries": [["line": 0, "raw": raw]], "startLine": 0, "totalLines": 22, "hasMore": false]), source: target.source)
    }
    static func panes(_ session: DiscoveredMoshiSession) throws -> AgentChatPanes {
        reads += 1
        if flag("--chat-offline") && reads > 1 { throw LiveConnectionError.disconnected }
        var panes: [[String: Any]] = [["id": "\(session.workspaceID):p1", "label": "1", "title": "Polish the phone app", "agent": "codex",
                                     "agentStatus": flag("--chat-blocked") ? "blocked" : (flag("--chat-working") && !stopped ? "working" : "idle"), "sessionId": "fixture-codex-session", "cwd": "/work/phone"]]
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
            entries.append(["line": (flag("--chat-history") ? 20 : 0) + entries.count, "raw": raw])
        }
        append("user", "Can you refine the project screen?")
        append("assistant", target.source == "codex" ? "The project screen is ready. What would you like to change?" : "I reviewed the changes. The project navigation looks consistent.")
        if flag("--chat-markdown") { append("assistant", "# Changes\nHere is the fix:\n```swift\nlet color = \"cyan\"\n```\nReady to test.") }
        if stopped { append("assistant", "Turn stopped in the selected pane.") }
        for (id, text) in sent where id == target.id {
            append("user", text)
            append("assistant", "Received in \(target.source) on \(target.paneID): \(text)")
        }
        return try AgentChatTranscript.read(JSONSerialization.data(withJSONObject: ["type": "backlog", "source": target.source,
                                                                                    "entries": entries, "startLine": flag("--chat-history") ? 20 : 0, "totalLines": (flag("--chat-history") ? 20 : 0) + entries.count, "hasMore": flag("--chat-history")]), source: target.source)
    }
    static func send(_ target: AgentChatTarget, text: String) async throws {
        try await Task.sleep(for: .milliseconds(250))
        if flag("--chat-send-fails") { throw LiveConnectionError.disconnected }
        sent.append((target.id, text))
    }
    private static func flag(_ flag: String) -> Bool { ProcessInfo.processInfo.arguments.contains(flag) }
}
#endif
