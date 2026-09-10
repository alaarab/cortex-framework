import PhrenKit
import SwiftUI

struct ChatTimelineEntry: Identifiable {
    var messages: [AgentChatMessage]
    var id: String { messages[0].id }
    var isActivity: Bool { messages[0].role == .tool }

    static func group(_ messages: [AgentChatMessage]) -> [Self] {
        var entries: [Self] = []
        var previousMessageID: String?
        var calls: [String: Int] = [:], ambiguous: Set<String> = []
        for message in messages {
            guard message.role == .tool else {
                entries.append(.init(messages: [message]))
                calls.removeAll(keepingCapacity: true); ambiguous.removeAll(keepingCapacity: true)
                previousMessageID = message.id
                continue
            }
            if message.isToolResult {
                if let key = message.toolCallID, !key.isEmpty, !ambiguous.contains(key), let index = calls[key] {
                    entries[index].messages.append(message)
                    previousMessageID = message.id
                    continue
                }
                // Older transcripts lack IDs. Only pair an immediately adjacent,
                // unidentified call/result; never guess among parallel calls.
                if message.toolCallID == nil, let previous = entries.last,
                   previous.messages.count == 1, let call = previous.messages.first,
                   call.role == .tool, !call.isToolResult, call.toolCallID == nil,
                   call.id == previousMessageID {
                    entries[entries.count - 1].messages.append(message)
                    previousMessageID = message.id
                    continue
                }
            } else if let key = message.toolCallID, !key.isEmpty {
                if calls[key] != nil { ambiguous.insert(key) }
                else { calls[key] = entries.count }
            }
            entries.append(.init(messages: [message]))
            previousMessageID = message.id
        }
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
                    if messages.contains(where: \.isToolResult) {
                        Image(systemName: "checkmark").font(.system(size: 10, weight: .medium)).foregroundStyle(PhrenTheme.textDim)
                    }
                    Image(systemName: "chevron.down").font(.system(size: 10, weight: .semibold))
                        .rotationEffect(.degrees(expanded ? 180 : 0)).foregroundStyle(PhrenTheme.textDim)
                }
                .font(.system(.caption2, design: .monospaced))
                .padding(.horizontal, 12).padding(.vertical, 4).frame(minHeight: 34)
                .contentShape(Rectangle().inset(by: -5))
            }.buttonStyle(.plain)
                .accessibilityLabel("\(summary.title), \(summary.count) \(summary.count == 1 ? "operation" : "operations")")
                .accessibilityValue(expanded ? "Expanded" : "Collapsed")
                .accessibilityHint("Expand this call and its output")
                .accessibilityIdentifier("chat-tool-group:\(messages[0].id)")
            if expanded {
                VStack(alignment: .leading, spacing: 8) {
                    ForEach(messages) { message in
                        let presentation = ToolPresentation(title: message.title ?? "Tool activity", text: message.text)
                        if !message.isToolResult, presentation.patch == nil, messages.contains(where: \.isToolResult) {
                            DisclosureGroup("Input details") {
                                ToolDetailView(presentation: presentation, id: message.id)
                            }.font(.caption2).foregroundStyle(PhrenTheme.textMuted)
                        } else {
                            ToolDetailView(presentation: presentation, id: message.id)
                        }
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
    let id: String
    @State private var fullOutput: FullToolOutput?
    var body: some View {
        VStack(alignment: .leading, spacing: 4) {
            if let patch = presentation.patch { CodeDiffView(patch: patch, previewLineLimit: 8) }
            else {
                HStack(spacing: 8) {
                    Text(presentation.title == "Tool Result" ? "Output" : presentation.title).fontWeight(.medium)
                    Spacer()
                    Button("View full output", systemImage: "arrow.up.left.and.arrow.down.right") {
                        fullOutput = .init(title: presentation.title, text: presentation.body)
                    }.frame(width: 36, height: 32).contentShape(Rectangle())
                        .accessibilityIdentifier("chat-tool-output:\(id)")
                    Button("Copy tool details", systemImage: "doc.on.doc") { UIPasteboard.general.string = presentation.body }
                        .frame(width: 36, height: 32).contentShape(Rectangle())
                }.font(.caption2).foregroundStyle(PhrenTheme.textMuted)
                    .labelStyle(.iconOnly).buttonStyle(.plain).frame(minHeight: 32)
                Text(presentation.body.isEmpty ? "No output" : ToolOutputPreview(presentation.body).text)
                    .font(.system(.caption, design: .monospaced)).foregroundStyle(PhrenTheme.text)
                    .lineLimit(6).textSelection(.enabled).frame(maxWidth: .infinity, alignment: .leading)
                    .accessibilityIdentifier("chat-tool-preview:\(id)")
            }
            if presentation.raw != presentation.body {
                Button("Raw details") { fullOutput = .init(title: "Raw details", text: presentation.raw) }
                    .font(.caption2).foregroundStyle(PhrenTheme.textDim).padding(.vertical, 4)
            }
        }
        .sheet(item: $fullOutput) { output in
            NavigationStack {
                ScrollView([.horizontal, .vertical]) {
                    Text(output.text).font(.system(.caption, design: .monospaced))
                        .foregroundStyle(PhrenTheme.text).textSelection(.enabled)
                        .fixedSize(horizontal: true, vertical: true).padding(16)
                }
                .background(PhrenTheme.chatPanel).navigationTitle(output.title).navigationBarTitleDisplayMode(.inline)
                .toolbar {
                    ToolbarItem(placement: .cancellationAction) {
                        Button("Done") { fullOutput = nil }.accessibilityIdentifier("chat-tool-output-done")
                    }
                    ToolbarItem(placement: .primaryAction) {
                        Button("Copy output", systemImage: "doc.on.doc") { UIPasteboard.general.string = output.text }
                    }
                }
                .accessibilityIdentifier("chat-full-tool-output")
            }.presentationDetents([.large])
        }
    }
}

private struct FullToolOutput: Identifiable {
    let id = UUID()
    let title: String
    let text: String
}

/// Bound layout work as well as visible height. Full provider text is retained
/// separately, so expanding a row never lays out thousands of output lines.
struct ToolOutputPreview {
    let text: String
    init(_ output: String) {
        let prefix = String(output.prefix(640))
        let lines = prefix.components(separatedBy: .newlines)
        let visible = lines.prefix(6).joined(separator: "\n")
        text = visible + (visible.count < output.count ? "…" : "")
    }
}
