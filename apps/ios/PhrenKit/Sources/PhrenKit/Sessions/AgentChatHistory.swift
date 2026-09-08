import Foundation

/// Merge reconnect snapshots, live appends, and older pages by absolute line.
public struct AgentChatHistory: Sendable {
    public private(set) var messages: [AgentChatMessage] = []
    public private(set) var startLine: Int?
    public private(set) var totalLines = 0
    public private(set) var hasMore = false
    public private(set) var reachedLimit = false
    public init() {}

    public mutating func receive(_ frame: AgentChatTranscript) {
        if frame.kind == .backlog && frame.totalLines < totalLines { self = Self() }
        var merged = Dictionary(uniqueKeysWithValues: messages.map { ($0.id, $0) })
        for message in frame.messages { merged[message.id] = message }
        messages = merged.values.sorted { ($0.line, $0.id) < ($1.line, $1.id) }
        totalLines = max(totalLines, frame.totalLines)
        if frame.kind != .append, let start = frame.startLine, start <= (startLine ?? Int.max) {
            startLine = start; hasMore = frame.hasMore
        }
        var bytes = 0, keep = 0
        for message in messages.reversed() {
            bytes += message.text.utf8.count
            if bytes > 12 * 1_024 * 1_024 || keep >= 4_000 { break }
            keep += 1
        }
        if keep < messages.count {
            messages = Array(messages.suffix(keep)); startLine = messages.first?.line
            reachedLimit = true; hasMore = false
        }
        if reachedLimit { hasMore = false }
    }
}
