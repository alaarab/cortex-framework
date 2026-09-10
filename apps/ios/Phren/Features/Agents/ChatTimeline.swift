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
        let presentations = calls.map { ToolPresentation(title: $0.title ?? "Tool", text: $0.text) }
        let names = presentations.map(\.title)
        title = Set(names).count == 1 ? names[0] : calls.isEmpty ? "Tool results" : "Activity"
        icon = title == "Shell" ? "terminal" : title == "Browse" ? "globe" : title == "Patch" ? "pencil.line" : "wrench.and.screwdriver"
        count = max(1, calls.isEmpty ? messages.count : calls.count)
        preview = presentations.last?.preview ?? messages.last.map { ToolPresentation(title: $0.title ?? "Tool result", text: $0.text).preview } ?? ""
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
                VStack(alignment: .leading, spacing: 8) {
                    ForEach(messages) { message in
                        ToolDetailView(presentation: .init(title: message.title ?? "Tool activity", text: message.text))
                    }
                }.padding(.horizontal, 10).padding(.bottom, 10)
            }
        }
        .background(PhrenTheme.toolPanel, in: RoundedRectangle(cornerRadius: 16, style: .continuous))
        .overlay(RoundedRectangle(cornerRadius: 16).strokeBorder(PhrenTheme.border, lineWidth: 0.5))
    }
}

private struct ToolDetailView: View {
    let presentation: ToolPresentation
    @State private var showAll = false
    var body: some View {
        VStack(alignment: .leading, spacing: 4) {
            if let patch = presentation.patch { CodeDiffView(patch: patch) }
            else {
                HStack {
                    Text(presentation.title == "Tool Result" ? "Output" : presentation.title).fontWeight(.medium)
                    Spacer()
                    Button("Copy tool details", systemImage: "doc.on.doc") { UIPasteboard.general.string = presentation.body }
                        .labelStyle(.iconOnly).frame(width: 36, height: 32)
                }.font(.caption2).foregroundStyle(PhrenTheme.textMuted)
                Text(showAll ? presentation.body : String(presentation.body.prefix(2_400)))
                    .font(.system(.caption, design: .monospaced)).foregroundStyle(PhrenTheme.text)
                    .textSelection(.enabled).frame(maxWidth: .infinity, alignment: .leading)
                if presentation.body.count > 2_400 {
                    Button(showAll ? "Show less" : "Show full output") { showAll.toggle() }
                        .font(.caption).foregroundStyle(PhrenTheme.accent).padding(.vertical, 4)
                }
            }
            if presentation.raw != presentation.body {
                DisclosureGroup("Raw details") {
                    ScrollView([.horizontal, .vertical]) {
                        Text(presentation.raw).font(.system(.caption2, design: .monospaced)).textSelection(.enabled)
                    }.frame(maxHeight: 180)
                }.font(.caption2).foregroundStyle(PhrenTheme.textDim).padding(.vertical, 4)
            }
        }
    }
}
