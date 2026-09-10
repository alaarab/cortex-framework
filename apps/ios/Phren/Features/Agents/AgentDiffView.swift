import PhrenKit
import PhrenLive
import SwiftUI

struct AgentDiffView: View {
    let session: LiveAgentSession
    let target: AgentChatTarget
    @Environment(\.scenePhase) private var scenePhase
    @AppStorage("sessions.live.preferences.v1") private var hostData = Data()
    @State private var diff: AgentRepositoryDiff?
    @State private var error: String?
    @State private var visible = false
    @State private var refresh = UUID()
    private var active: Bool { visible && scenePhase == .active && (try? LiveSessionPreferences.read(hostData))?.hosts.first(where: { $0.id == session.host.id }) == session.host }
    var body: some View {
        PhrenList {
            Section { Text("\(session.host.name) · \(session.workspaceName)").font(.caption).foregroundStyle(PhrenTheme.textMuted) }
            if let diff {
                Section {
                    LabeledContent("Branch", value: diff.branch ?? "Unborn branch")
                    Text(diff.root).font(.caption.monospaced()).textSelection(.enabled)
                }
                Section("\(diff.files.count) changed files") {
                    if diff.files.isEmpty { Label("Working tree is clean", systemImage: "checkmark.circle") }
                    ForEach(diff.files) { file in
                        NavigationLink {
                            ScrollView([.horizontal, .vertical]) {
                                LazyVStack(alignment: .leading, spacing: 14) {
                                    ForEach(file.sections) { section in
                                        Text(section.kind.capitalized).font(.headline).foregroundStyle(PhrenTheme.textMuted)
                                        if section.binary == true { Text("Binary file changed").font(.subheadline) }
                                        else if let patch = section.patch, !patch.isEmpty {
                                            Text(AgentDiffText.make(patch)).font(.system(size: 12, design: .monospaced)).textSelection(.enabled)
                                        } else { Text("This change needs the full terminal view.").font(.footnote) }
                                    }
                                }.padding(16)
                            }.background(PhrenTheme.bgSunken).navigationTitle(file.path).navigationBarTitleDisplayMode(.inline)
                        } label: {
                            VStack(alignment: .leading, spacing: 4) {
                                Text(file.path).font(.subheadline.monospaced()).lineLimit(3)
                                Text(file.status.capitalized).font(.caption).foregroundStyle(PhrenTheme.textMuted)
                            }.padding(.vertical, 4)
                        }
                    }
                }
            } else if error == nil { ProgressView("Loading repository changes…") }
            if let error { Text(error).font(.footnote).foregroundStyle(PhrenTheme.warning) }
            Section { NavigationLink { HerdrTerminalView(host: session.host, session: session, target: target) } label: { Label("Open Herdr terminal", systemImage: "terminal") } }
        }
        .navigationTitle("Repository changes").navigationBarTitleDisplayMode(.inline)
        .toolbar { Button("Refresh diff", systemImage: "arrow.clockwise") { refresh = UUID() } }
        .onAppear { visible = true }.onDisappear { visible = false }
        .task(id: Run(active: active, refresh: refresh)) {
            guard active else { return }
            error = nil
            do {
                let result: AgentRepositoryDiff
                #if DEBUG && targetEnvironment(simulator)
                if AgentChatFixture.enabled {
                    result = try AgentRepositoryDiff.read(Data(#"{"root":"/work/phone","launchPath":"/work/phone","branch":"main","files":[{"path":"Theme.swift","status":"modified","sections":[{"id":"theme","kind":"unstaged","binary":false,"patch":"@@ -1 +1 @@\n-let accent = purple\n+let accent = cyan"}]}]}"#.utf8))
                } else { result = try await PhrenConnection.repositoryDiff(host: session.host, privateKey: DeviceSSHKey.load(session.host.id), target: target) }
                #else
                result = try await PhrenConnection.repositoryDiff(host: session.host, privateKey: DeviceSSHKey.load(session.host.id), target: target)
                #endif
                try Task.checkCancellation(); diff = result
            } catch { if !Task.isCancelled { self.error = error.localizedDescription } }
        }
    }
    private struct Run: Equatable { let active: Bool; let refresh: UUID }
}

private enum AgentDiffText {
    static func make(_ patch: String) -> AttributedString {
        var result = AttributedString()
        // Bound rendering independently of the network response; preserve copyable raw content below this limit.
        for line in String(patch.prefix(160_000)).components(separatedBy: "\n") {
            var part = AttributedString(line + "\n")
            part.foregroundColor = line.hasPrefix("+") ? PhrenTheme.cyan : line.hasPrefix("-") ? PhrenTheme.warning : line.hasPrefix("@@") ? PhrenTheme.lavender : PhrenTheme.text
            result.append(part)
        }
        if patch.count > 160_000 { result.append(AttributedString("\nPreview truncated. Open Herdr for the complete diff.")) }
        return result
    }
}
