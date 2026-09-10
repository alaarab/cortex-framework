import SwiftUI

/// Shared by repository changes and tool patches, with semantic diff colors
/// independent of the user's action-button theme.
struct CodeDiffView: View {
    let patch: String
    var previewLineLimit = 36
    @State private var showAll = false
    private var preview: DiffPreview { DiffPreview(patch) }
    var body: some View {
        let diff = preview
        let numbered = diff.lines.contains { $0.old != nil || $0.new != nil }
        VStack(alignment: .leading, spacing: 6) {
            HStack(spacing: 8) {
                Text("+\(diff.added)").foregroundStyle(PhrenTheme.success)
                Text("−\(diff.removed)").foregroundStyle(PhrenTheme.danger)
                if diff.truncated { Text("Preview").foregroundStyle(PhrenTheme.textMuted) }
                Spacer()
                Button("Copy patch", systemImage: "doc.on.doc") { UIPasteboard.general.string = patch }
                    .labelStyle(.iconOnly).foregroundStyle(PhrenTheme.textMuted).frame(width: 36, height: 32)
            }.font(.system(.caption2, design: .monospaced)).padding(.horizontal, 10)
            ScrollView(.horizontal) {
                VStack(alignment: .leading, spacing: 0) {
                    ForEach(showAll ? diff.lines : Array(diff.lines.prefix(previewLineLimit))) { line in
                        HStack(alignment: .top, spacing: 8) {
                            if numbered && (line.kind == .context || line.kind == .added || line.kind == .removed) {
                                Text(line.old.map(String.init) ?? "").frame(width: 32, alignment: .trailing)
                                Text(line.new.map(String.init) ?? "").frame(width: 32, alignment: .trailing)
                            }
                            Text(line.text.isEmpty ? " " : line.text)
                                .foregroundStyle(line.kind == .hunk ? PhrenTheme.accent : PhrenTheme.text)
                                .fixedSize(horizontal: true, vertical: false)
                                .frame(maxWidth: .infinity, alignment: .leading)
                        }
                        .font(.system(.caption, design: .monospaced)).foregroundStyle(PhrenTheme.textDim)
                        .padding(.horizontal, 8).padding(.vertical, 2)
                        .background(line.kind == .added ? PhrenTheme.success.opacity(0.15)
                                    : line.kind == .removed ? PhrenTheme.danger.opacity(0.13)
                                    : line.kind == .hunk ? PhrenTheme.accent.opacity(0.06) : .clear)
                        .accessibilityElement(children: .combine)
                    }
                }.textSelection(.enabled)
            }.defaultScrollAnchor(.topLeading)
            if diff.lines.count > previewLineLimit {
                Button(showAll ? "Collapse patch" : "Show \(diff.lines.count - previewLineLimit) more lines") { showAll.toggle() }
                    .font(.caption).foregroundStyle(PhrenTheme.accent).padding(10)
            }
            if diff.truncated { Text("Preview truncated. Copy the patch for all supplied lines.").font(.caption).foregroundStyle(PhrenTheme.textMuted).padding(10) }
        }
        .background(PhrenTheme.toolPanel, in: RoundedRectangle(cornerRadius: 12))
        .clipShape(RoundedRectangle(cornerRadius: 12))
    }
}
