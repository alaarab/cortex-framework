import SwiftUI

/// Native Markdown paragraphs and fenced code; no remote web content is loaded.
struct ChatRichText: View {
    let text: String
    private struct Block: Identifiable {
        let id: Int
        let text: String
        let language: String?
        let heading: Bool
    }
    private var blocks: [Block] {
        var result: [Block] = [], lines: [String] = []
        var language: String?
        func flush() {
            guard !lines.isEmpty else { return }
            result.append(.init(id: result.count, text: lines.joined(separator: "\n"), language: language, heading: false))
            lines = []
        }
        for line in text.components(separatedBy: "\n") {
            if line.hasPrefix("```") {
                flush(); language = language == nil ? String(line.dropFirst(3)).trimmingCharacters(in: .whitespaces) : nil
            } else if language == nil && line.range(of: #"^#{1,6} "#, options: .regularExpression) != nil {
                flush()
                result.append(.init(id: result.count, text: String(line.drop(while: { $0 == "#" || $0 == " " })), language: nil, heading: true))
            } else { lines.append(line) }
        }
        flush()
        return result
    }
    var body: some View {
        VStack(alignment: .leading, spacing: 12) {
            ForEach(blocks) { block in
                if let language = block.language {
                    VStack(alignment: .leading, spacing: 8) {
                        HStack {
                            Text(language.isEmpty ? "Code" : language).font(.caption)
                            Spacer()
                            Button("Copy code", systemImage: "doc.on.doc") { UIPasteboard.general.string = block.text }
                                .font(.caption).labelStyle(.iconOnly).frame(minWidth: 44, minHeight: 32)
                        }.foregroundStyle(PhrenTheme.textMuted)
                        ScrollView(.horizontal) {
                            Text(block.text).font(.callout.monospaced()).textSelection(.enabled).fixedSize(horizontal: true, vertical: false)
                        }
                    }.padding(12).background(PhrenTheme.chatPanel, in: RoundedRectangle(cornerRadius: 14))
                        .overlay(RoundedRectangle(cornerRadius: 14).strokeBorder(PhrenTheme.border, lineWidth: 1))
                } else {
                    Text((try? AttributedString(markdown: block.text, options: .init(interpretedSyntax: .inlineOnlyPreservingWhitespace))) ?? AttributedString(block.text))
                        .font(block.heading ? .system(.headline, design: .monospaced) : .system(.callout, design: .monospaced))
                        .lineSpacing(4).textSelection(.enabled).tint(PhrenTheme.cyan)
                        .frame(maxWidth: .infinity, alignment: .leading)
                }
            }
        }
    }
}
