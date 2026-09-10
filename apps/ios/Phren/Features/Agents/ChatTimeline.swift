import PhrenKit
import SwiftUI

struct ChatTimelineEntry: Identifiable {
    let messages: [AgentChatMessage]
    var id: String { messages[0].id }
    var isActivity: Bool { messages[0].role == .tool }

    static func group(_ messages: [AgentChatMessage]) -> [Self] {
        var entries: [Self] = [], activity: [AgentChatMessage] = []
        for message in messages {
            if message.role == .tool { activity.append(message) }
            else {
                if !activity.isEmpty { entries.append(.init(messages: activity)); activity = [] }
                entries.append(.init(messages: [message]))
            }
        }
        if !activity.isEmpty { entries.append(.init(messages: activity)) }
        return entries
    }
}

struct ChatToolSummary {
    let title: String
    let icon: String
    let preview: String
    let count: Int

    init(_ messages: [AgentChatMessage]) {
        let calls = messages.filter { $0.title != "Tool result" }
        let names = calls.map { Self.name($0.title ?? "Tool") }
        title = Set(names).count == 1 ? names[0] : calls.isEmpty ? "Tool results" : "Activity"
        icon = title == "Shell" ? "terminal" : title == "Browse" ? "globe" : "wrench.and.screwdriver"
        count = max(1, calls.isEmpty ? messages.count : calls.count)
        let message = calls.last ?? messages.last
        var text = message?.text ?? ""
        if let data = text.data(using: .utf8), let fields = (try? JSONSerialization.jsonObject(with: data)) as? [String: Any] {
            text = ["cmd", "command", "query", "q", "file_path", "path", "description"].compactMap { fields[$0] as? String }.first ?? text
        }
        preview = String(text.split(whereSeparator: \.isNewline).first?.trimmingCharacters(in: .whitespaces).prefix(180) ?? "")
    }

    private static func name(_ raw: String) -> String {
        let name = raw.split(separator: ".").last.map(String.init) ?? raw
        if ["exec_command", "bash", "shell", "Bash", "Shell", "write_stdin"].contains(name) { return "Shell" }
        if ["exec", "parallel"].contains(name) { return "Tools" }
        if name.contains("search") || name.contains("web") { return "Browse" }
        return name.replacingOccurrences(of: "_", with: " ").capitalized
    }
}

struct ChatToolActivity: View {
    let messages: [AgentChatMessage]
    @State private var expanded = false
    @Environment(\.accessibilityReduceMotion) private var reduceMotion
    private var summary: ChatToolSummary { .init(messages) }
    var body: some View {
        VStack(alignment: .leading, spacing: 0) {
            Button {
                withAnimation(reduceMotion ? nil : .easeInOut(duration: 0.18)) { expanded.toggle() }
            } label: {
                HStack(spacing: 7) {
                    Image(systemName: summary.icon).foregroundStyle(PhrenTheme.textDim).frame(width: 14)
                    Text(summary.title).fontWeight(.semibold).foregroundStyle(PhrenTheme.textSecondary).lineLimit(1)
                    if summary.count > 1 { Text("×\(summary.count)").foregroundStyle(PhrenTheme.textDim) }
                    Text(summary.preview).foregroundStyle(PhrenTheme.textMuted).lineLimit(1).truncationMode(.middle)
                        .frame(maxWidth: .infinity, alignment: .leading)
                    Image(systemName: "chevron.down").font(.system(size: 10, weight: .semibold))
                        .rotationEffect(.degrees(expanded ? 180 : 0)).foregroundStyle(PhrenTheme.textDim)
                }
                .font(.system(.caption2, design: .monospaced))
                .padding(.horizontal, 12).padding(.vertical, 4).frame(minHeight: 34)
                .contentShape(Rectangle().inset(by: -5))
            }.buttonStyle(.plain)
                .accessibilityLabel("\(summary.title), \(summary.count) \(summary.count == 1 ? "operation" : "operations")")
                .accessibilityValue(expanded ? "Expanded" : "Collapsed")
                .accessibilityHint("Show commands and results")
                .accessibilityIdentifier("chat-tool-group:\(messages[0].id)")
            if expanded {
                VStack(alignment: .leading, spacing: 14) {
                    ForEach(messages) { message in
                        VStack(alignment: .leading, spacing: 6) {
                            HStack {
                                Text(message.title ?? "Tool activity").font(.caption.weight(.semibold))
                                Spacer()
                                Button("Copy tool details", systemImage: "doc.on.doc") { UIPasteboard.general.string = message.text }
                                    .labelStyle(.iconOnly).frame(width: 36, height: 32)
                            }.foregroundStyle(PhrenTheme.textMuted)
                            ScrollView([.horizontal, .vertical]) {
                                Text(message.text).font(.system(.caption, design: .monospaced)).textSelection(.enabled)
                                    .frame(maxWidth: .infinity, alignment: .leading)
                            }.defaultScrollAnchor(.topLeading).frame(maxHeight: 220)
                        }
                    }
                }.padding(.horizontal, 14).padding(.bottom, 14)
            }
        }
        .background(PhrenTheme.toolPanel, in: RoundedRectangle(cornerRadius: 16, style: .continuous))
        .overlay(RoundedRectangle(cornerRadius: 16).strokeBorder(PhrenTheme.border, lineWidth: 0.5))
    }
}
