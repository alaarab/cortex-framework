import Foundation

/// An agent conversation belongs to a specific pane on a specific computer.
/// Workspace labels and the newest transcript are never used for routing.
public struct AgentChatTarget: Equatable, Hashable, Sendable, Identifiable {
    public let hostID: UUID
    public let workspaceID: String
    public let tabID: String
    public let paneID: String
    public let source: String
    public let sessionID: String
    public let muxID: String
    public var id: String { [hostID.uuidString, muxID, workspaceID, tabID, paneID, source, sessionID].joined(separator: "/") }

    public init(hostID: UUID, workspaceID: String, tabID: String, paneID: String, source: String, sessionID: String, muxID: String = "herdr:default") throws {
        guard [workspaceID, tabID, paneID, sessionID, muxID].allSatisfy(Self.validID), muxID.hasPrefix("herdr:"), ["codex", "claude"].contains(source) else {
            throw PhrenKitError.validation("Native chat needs a recognized Codex or Claude Code conversation in this pane.")
        }
        self.hostID = hostID; self.workspaceID = workspaceID; self.tabID = tabID
        self.paneID = paneID; self.source = source; self.sessionID = sessionID
        self.muxID = muxID
    }

    public static func validID(_ value: String) -> Bool {
        !value.isEmpty && value.utf8.count <= 200
            && value.range(of: #"^[A-Za-z0-9_%:.-]+$"#, options: .regularExpression) != nil
    }
}

public struct AgentChatPanes: Decodable, Equatable, Sendable {
    public struct Pane: Decodable, Equatable, Sendable, Identifiable {
        public let id: String
        public let label: String
        public let agent: String?
        public let agentStatus: String?
        public let sessionId: String?
        public let title: String?
        public let cwd: String?
        public var displayTitle: String { title?.isEmpty == false ? title! : label }
        public var needsAnswer: Bool { ["blocked", "waiting"].contains(agentStatus ?? "") }
        public func target(hostID: UUID, workspaceID: String, tabID: String, muxID: String = "herdr:default") throws -> AgentChatTarget {
            try AgentChatTarget(hostID: hostID, workspaceID: workspaceID, tabID: tabID,
                                paneID: id, source: agent ?? "", sessionID: sessionId ?? "", muxID: muxID)
        }
    }
    public let kind: String
    public let groupId: String
    public let childId: String
    public let panes: [Pane]

    public static func read(_ data: Data, workspaceID: String, tabID: String) throws -> Self {
        guard data.count <= 1_048_576 else { throw PhrenKitError.validation("The agent list is too large.") }
        let value = try JSONDecoder().decode(Self.self, from: data)
        guard value.kind == "herdr", value.groupId == workspaceID, value.childId == tabID,
              Set(value.panes.map(\.id)).count == value.panes.count,
              value.panes.allSatisfy({ AgentChatTarget.validID($0.id) }) else {
            throw PhrenKitError.validation("The computer returned a different or invalid agent location. Refresh the session.")
        }
        return value
    }

    public func validate(_ target: AgentChatTarget, sending: Bool = false) throws -> Pane {
        guard groupId == target.workspaceID, childId == target.tabID,
              let pane = panes.first(where: { $0.id == target.paneID }),
              pane.agent == target.source, pane.sessionId == target.sessionID else {
            throw PhrenKitError.validation("The agent in this pane changed. Reopen chat to choose its current conversation.")
        }
        if sending && pane.needsAnswer {
            throw PhrenKitError.validation("This agent needs an approval or answer in the terminal before another message can be sent.")
        }
        return pane
    }
}

public struct AgentChatMessage: Equatable, Sendable, Identifiable {
    public enum Role: String, Sendable { case user, assistant, tool }
    public let id: String
    public let line: Int
    public let role: Role
    public let title: String?
    public let text: String
    public var imageBlocks: [Int] = []
}

/// Normalize only visible conversation content. Encrypted reasoning, system
/// prompts, hook metadata, and terminal escape sequences are never rendered.
public struct AgentChatTranscript: Equatable, Sendable {
    public enum Kind: String, Sendable { case backlog, append, older }
    public let kind: Kind
    public let messages: [AgentChatMessage]
    public let hasMore: Bool
    public let totalLines: Int
    public let startLine: Int?
    public var questionEvents: [AgentQuestionEvent] = []

    public static func read(_ data: Data, source: String) throws -> Self {
        guard ["codex", "claude"].contains(source), data.count <= 8_388_608,
              let frame = try JSONSerialization.jsonObject(with: data) as? [String: Any],
              let kind = Kind(rawValue: frame["type"] as? String ?? ""), frame["source"] as? String == source,
              let entries = frame["entries"] as? [[String: Any]], entries.count <= 2_000 else {
            throw PhrenKitError.validation("The computer returned an unsupported chat transcript.")
        }
        var messages: [AgentChatMessage] = []
        var questionEvents: [AgentQuestionEvent] = []
        var seen: Set<String> = []
        for entry in entries {
            guard let line = entry["line"] as? Int, line >= 0, let raw = entry["raw"] as? [String: Any] else { continue }
            questionEvents += AgentQuestionEvent.read(raw, source: source)
            let parts = source == "codex" ? codex(raw) : claude(raw)
            for (index, part) in parts.enumerated() {
                let id = "\(line):\(index)"
                guard !part.text.isEmpty, seen.insert(id).inserted else { continue }
                messages.append(.init(id: id, line: line, role: part.role, title: part.title,
                                      text: String(part.text.prefix(64_000)), imageBlocks: part.imageBlocks))
            }
        }
        return Self(kind: kind, messages: messages.sorted { $0.line < $1.line }, hasMore: frame["hasMore"] as? Bool ?? false,
                    totalLines: frame["totalLines"] as? Int ?? 0,
                    startLine: frame["startLine"] as? Int ?? entries.compactMap { $0["line"] as? Int }.min(), questionEvents: questionEvents)
    }

    private struct Part { let role: AgentChatMessage.Role; var title: String? = nil; let text: String; var imageBlocks: [Int] = [] }
    private static func text(_ value: Any?) -> String {
        if let value = value as? String { return value }
        guard let blocks = value as? [[String: Any]] else { return "" }
        return blocks.compactMap { block -> String? in
            switch block["type"] as? String {
            case "text", "input_text", "output_text": return block["text"] as? String
            case "image", "input_image": return "[Image attachment]"
            default: return nil
            }
        }.joined(separator: "\n\n")
    }
    private static func readable(_ value: Any?) -> String {
        if let value = value as? String { return value }
        guard let value, JSONSerialization.isValidJSONObject(value),
              let data = try? JSONSerialization.data(withJSONObject: value, options: [.prettyPrinted, .sortedKeys]) else { return "" }
        return String(decoding: data, as: UTF8.self)
    }
    private static func codex(_ raw: [String: Any]) -> [Part] {
        guard raw["type"] as? String == "response_item", let payload = raw["payload"] as? [String: Any] else { return [] }
        switch payload["type"] as? String {
        case "message":
            guard let role = AgentChatMessage.Role(rawValue: payload["role"] as? String ?? ""), role != .tool else { return [] }
            let images = (payload["content"] as? [[String: Any]] ?? []).enumerated().compactMap { index, block in
                ["input_image", "image"].contains(block["type"] as? String ?? "") ? index : nil
            }
            return [Part(role: role, text: text(payload["content"]), imageBlocks: images)]
        case "function_call", "custom_tool_call":
            return [Part(role: .tool, title: payload["name"] as? String ?? "Tool", text: readable(payload["arguments"] ?? payload["input"]))]
        case "function_call_output", "custom_tool_call_output":
            return [Part(role: .tool, title: "Tool result", text: readable(payload["output"]))]
        default: return []
        }
    }
    private static func claude(_ raw: [String: Any]) -> [Part] {
        guard raw["isMeta"] as? Bool != true, raw["isSidechain"] as? Bool != true,
              let message = raw["message"] as? [String: Any],
              let role = AgentChatMessage.Role(rawValue: message["role"] as? String ?? ""), role != .tool else { return [] }
        if let content = message["content"] as? String { return [Part(role: role, text: content)] }
        guard let blocks = message["content"] as? [[String: Any]] else { return [] }
        return blocks.enumerated().compactMap { index, block in
            switch block["type"] as? String {
            case "text": return Part(role: role, text: block["text"] as? String ?? "")
            case "image": return Part(role: role, text: "[Image attachment]", imageBlocks: [index])
            case "tool_use": return Part(role: .tool, title: block["name"] as? String ?? "Tool", text: readable(block["input"]))
            case "tool_result": return Part(role: .tool, title: "Tool result", text: text(block["content"]))
            default: return nil
            }
        }
    }
}
