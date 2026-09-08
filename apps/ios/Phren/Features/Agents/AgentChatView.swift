import PhrenKit
import PhrenLive
import SwiftUI

@MainActor private enum AgentChatDrafts {
    static var values: [String: String] = [:]
}

/// Shared entry point; the preference changes where a normal agent tap opens.
struct AgentConversationLink<LabelContent: View>: View {
    let session: DiscoveredMoshiSession
    var honorsPreference = true
    @ViewBuilder var label: LabelContent
    @AppStorage("agents.preferMoshi.v1") private var preferMoshi = false
    @Environment(\.openURL) private var openURL
    @State private var showingChat = false
    @State private var error: String?

    var body: some View {
        Button {
            if honorsPreference && preferMoshi, let url = try? session.link().url() {
                openURL(url) { accepted in if !accepted { error = "Moshi couldn't be opened. Choose Phren chat in Settings or open Moshi on this iPhone." } }
            } else { showingChat = true }
        } label: { label }
        .sheet(isPresented: $showingChat) { AgentChatSheet(session: session) }
        .modifier(MoshiLaunchAlert(error: $error))
    }
}

struct AgentChatSheet: View {
    let session: DiscoveredMoshiSession
    @Environment(\.dismiss) private var dismiss
    var body: some View {
        NavigationStack {
            AgentChatView(session: session)
                .toolbar { ToolbarItem(placement: .cancellationAction) { Button("Done") { dismiss() } } }
        }
    }
}

@Observable @MainActor
final class AgentChatModel {
    var panes: [AgentChatPanes.Pane] = []
    var target: AgentChatTarget?
    var messages: [AgentChatMessage] = []
    var error: String?
    var deliveryError: String?
    var loading = true
    var connected = false
    var sending = false
    var needsAnswer = false
    var hasMore = false
    var receivedAt: Date?
    var draft = ""
    private var generation = UUID()

    func choose(_ pane: AgentChatPanes.Pane, session: DiscoveredMoshiSession) {
        do {
            target = try pane.target(hostID: session.host.id, workspaceID: session.workspaceID, tabID: session.tab.id)
            draft = target.flatMap { AgentChatDrafts.values[$0.id] } ?? ""
            messages = []; connected = false; error = nil; deliveryError = nil
        } catch { self.error = error.localizedDescription }
    }

    func run(_ session: DiscoveredMoshiSession) async {
        let run = UUID(); generation = run
        loading = true
        defer { if generation == run { connected = false; loading = false } }
        while !Task.isCancelled {
            do {
                let list = try await Self.fetchPanes(session)
                try Task.checkCancellation()
                guard generation == run else { return }
                panes = list.panes
                if target == nil {
                    let supported = panes.compactMap { try? $0.target(hostID: session.host.id, workspaceID: session.workspaceID, tabID: session.tab.id) }
                    if supported.count == 1 { target = supported[0]; draft = AgentChatDrafts.values[supported[0].id] ?? "" }
                }
                if let target {
                    let pane = try list.validate(target)
                    let transcript = try await Self.fetchTranscript(session, target: target)
                    try Task.checkCancellation()
                    guard generation == run, self.target == target else { continue }
                    messages = transcript.messages
                    hasMore = transcript.hasMore
                    needsAnswer = pane.needsAnswer
                    connected = true; receivedAt = Date(); error = nil
                }
                loading = false
            } catch {
                guard !Task.isCancelled, generation == run else { return }
                connected = false; loading = false
                self.error = error.localizedDescription
            }
            do { try await Task.sleep(for: .seconds(3)) } catch { return }
        }
    }

    /// Only the explicit send action calls this. Never replay on reconnect.
    func send(_ session: DiscoveredMoshiSession) async {
        guard !sending, connected, !needsAnswer, let target, !draft.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty else { return }
        let submitted = draft
        sending = true; deliveryError = nil
        defer { sending = false }
        do {
            #if DEBUG && targetEnvironment(simulator)
            if AgentChatFixture.enabled { try await AgentChatFixture.send(target, text: submitted) }
            else { try await MoshiConnection.sendChat(host: session.host, privateKey: DeviceSSHKey.load(session.host.id), target: target, text: submitted) }
            #else
            try await MoshiConnection.sendChat(host: session.host, privateKey: DeviceSSHKey.load(session.host.id), target: target, text: submitted)
            #endif
            if draft == submitted { draft = "" }
        } catch {
            deliveryError = "Delivery wasn't confirmed. Check the conversation before trying again. \(error.localizedDescription)"
            return
        }
        if self.target == target {
            do {
                let transcript = try await Self.fetchTranscript(session, target: target)
                messages = transcript.messages; hasMore = transcript.hasMore
            } catch {
                self.error = "Message sent. Reconnecting to read the reply…"
            }
        }
    }

    private static func fetchPanes(_ session: DiscoveredMoshiSession) async throws -> AgentChatPanes {
        #if DEBUG && targetEnvironment(simulator)
        if AgentChatFixture.enabled { return try AgentChatFixture.panes(session) }
        #endif
        return try await MoshiConnection.chatPanes(host: session.host, privateKey: DeviceSSHKey.load(session.host.id),
                                                  workspaceID: session.workspaceID, tabID: session.tab.id)
    }
    private static func fetchTranscript(_ session: DiscoveredMoshiSession, target: AgentChatTarget) async throws -> AgentChatTranscript {
        #if DEBUG && targetEnvironment(simulator)
        if AgentChatFixture.enabled { return try AgentChatFixture.transcript(target) }
        #endif
        return try await MoshiConnection.chatTranscript(host: session.host, privateKey: DeviceSSHKey.load(session.host.id), target: target)
    }
}

struct AgentChatView: View {
    let session: DiscoveredMoshiSession
    @Environment(AppModel.self) private var appModel
    @Environment(\.scenePhase) private var scenePhase
    @AppStorage("sessions.live.preferences.v1") private var hostData = Data()
    @State private var model = AgentChatModel()
    @State private var sendTask: Task<Void, Never>?
    @State private var visible = false
    @State private var refresh = UUID()
    @State private var showingContext = false
    @State private var atBottom = true
    @State private var scrollHeight: CGFloat = 0
    @FocusState private var composing: Bool

    private var currentHost: LiveHost? {
        (try? LiveSessionPreferences.read(hostData))?.hosts.first { $0.id == session.host.id }
    }
    private var project: SessionProject? {
        let pane = model.panes.first { $0.id == model.target?.paneID }
        let cwd = pane?.cwd ?? (model.panes.count == 1 ? session.tab.cwd : nil)
        return (try? LiveSessionPreferences.read(hostData))?.projectMatch(hostID: session.host.id, cwd: cwd, projects: appModel.sessionProjects)?.project
    }
    private var active: Bool { visible && scenePhase == .active && currentHost == session.host }
    private var selectedPane: AgentChatPanes.Pane? { model.panes.first { $0.id == model.target?.paneID } }

    var body: some View {
        VStack(spacing: 0) {
            HStack(spacing: 8) {
                Circle().fill(model.connected && active ? PhrenTheme.cyan : PhrenTheme.textDim).frame(width: 6, height: 6)
                VStack(alignment: .leading, spacing: 4) {
                    Text("\(session.host.name) · \(session.workspaceName)").lineLimit(1)
                    if let pane = selectedPane {
                        Text(pane.displayTitle).font(.subheadline.weight(.medium))
                            .foregroundStyle(PhrenTheme.text).lineLimit(2)
                    }
                }
                Spacer(minLength: 4)
                if let target = model.target { Text(target.source == "codex" ? "Codex" : "Claude").foregroundStyle(PhrenTheme.lavender) }
            }
            .font(.caption).foregroundStyle(PhrenTheme.textMuted).padding(.horizontal, 18).padding(.vertical, 10)
            .accessibilityIdentifier("chat-location")

            ScrollViewReader { proxy in
                ScrollView {
                    LazyVStack(alignment: .leading, spacing: 16) {
                        if model.loading && model.messages.isEmpty { ProgressView("Opening conversation…").frame(maxWidth: .infinity).padding(.top, 40) }
                        if model.target == nil && !model.loading {
                            Text("Choose an agent").font(.title2.weight(.semibold))
                            ForEach(model.panes) { pane in
                                if (try? pane.target(hostID: session.host.id, workspaceID: session.workspaceID, tabID: session.tab.id)) != nil {
                                    Button {
                                        model.choose(pane, session: session); refresh = UUID()
                                    } label: {
                                        HStack { VStack(alignment: .leading) { Text(pane.displayTitle); Text(pane.agent ?? "").font(.caption) }; Spacer(); Image(systemName: "chevron.right") }
                                            .padding(16).phrenCard()
                                    }.buttonStyle(.plain).accessibilityIdentifier("chat-pane:\(pane.id)")
                                }
                            }
                            Text("Native chat supports Codex and Claude Code sessions recognized on this computer.")
                                .font(.footnote).foregroundStyle(PhrenTheme.textMuted)
                        }
                        if let error = model.error { connectionIssue(error) }
                        if currentHost != session.host { connectionIssue("This computer's connection changed. Reopen chat from the current session list.") }
                        if model.hasMore { Text("Recent conversation · earlier history stays on your computer").font(.caption).foregroundStyle(PhrenTheme.textDim) }
                        ForEach(model.messages) { message in ChatMessageRow(message: message) }
                        if model.connected && model.messages.isEmpty { Text("Ready for your message.").foregroundStyle(PhrenTheme.textMuted).padding(.top, 40) }
                        GeometryReader { geometry in
                            Color.clear.preference(key: ChatBottomPosition.self, value: geometry.frame(in: .named("chat-scroll")).maxY)
                        }.frame(height: 1).id("chat-bottom")
                    }
                    .padding(18)
                }
                .scrollDismissesKeyboard(.interactively)
                .defaultScrollAnchor(.bottom)
                .coordinateSpace(name: "chat-scroll")
                .background(GeometryReader { geometry in
                    Color.clear.onAppear { scrollHeight = geometry.size.height }
                        .onChange(of: geometry.size.height) { _, height in scrollHeight = height }
                })
                .onPreferenceChange(ChatBottomPosition.self) { bottom in atBottom = bottom <= scrollHeight + 60 }
                .overlay(alignment: .bottomTrailing) {
                    if !atBottom {
                        Button { withAnimation { proxy.scrollTo("chat-bottom", anchor: .bottom) } } label: {
                            Image(systemName: "arrow.down").frame(width: 40, height: 40).background(PhrenTheme.surfaceRaised, in: Circle())
                        }.accessibilityLabel("Latest messages").padding(12)
                    }
                }
                .onChange(of: model.target?.id) { _, _ in proxy.scrollTo("chat-bottom", anchor: .bottom) }
                .onChange(of: model.messages.last?.id) { _, _ in
                    if atBottom { withAnimation { proxy.scrollTo("chat-bottom", anchor: .bottom) } }
                }
            }
            composer
        }
        .background(PhrenTheme.bg)
        .navigationTitle("Agent chat")
        .navigationBarTitleDisplayMode(.inline)
        .toolbar {
            ToolbarItem(placement: .primaryAction) {
                Menu {
                    if let destination = try? session.link().url() {
                        Link("Open terminal in Moshi", destination: destination)
                    }
                    if let project {
                        NavigationLink("Project memory") { ProjectDetailView(storeId: project.storeID, project: project.name) }
                        NavigationLink("Project skills") { SkillsView(project: project.name, storeId: project.storeID) }
                        NavigationLink("Explore graph") { GraphView(focusProject: project.name, initialStoreId: project.storeID) }
                    }
                    Button("Refresh conversation") { refresh = UUID() }
                    if model.panes.filter({ (try? $0.target(hostID: session.host.id, workspaceID: session.workspaceID, tabID: session.tab.id)) != nil }).count > 1 {
                        Button("Choose another agent") {
                            model.target = nil; model.messages = []; model.connected = false
                            model.draft = ""; model.deliveryError = nil; model.needsAnswer = false
                            refresh = UUID()
                        }.disabled(model.sending)
                    }
                } label: { Image(systemName: "ellipsis") }
                .accessibilityLabel("Chat options")
            }
        }
        .onAppear { visible = true }
        .onDisappear { visible = false; sendTask?.cancel() }
        .onChange(of: scenePhase) { _, phase in if phase != .active { sendTask?.cancel() } }
        .onChange(of: currentHost) { _, _ in sendTask?.cancel() }
        .onChange(of: model.draft) { _, value in
            if let target = model.target { AgentChatDrafts.values[target.id] = value }
        }
        .sheet(isPresented: $showingContext) {
            if let project {
                ChatContextPicker(project: project) { context in
                    model.draft += (model.draft.isEmpty ? "" : "\n\n") + context
                }
            }
        }
        .task(id: RunIdentity(active: active, refresh: refresh)) {
            guard active else { return }
            await model.run(session)
        }
    }

    private func connectionIssue(_ message: String) -> some View {
        Label(message, systemImage: "wifi.exclamationmark")
            .font(.footnote).foregroundStyle(PhrenTheme.warning).padding(12).phrenCard()
    }

    private var composer: some View {
        VStack(alignment: .leading, spacing: 8) {
            if model.needsAnswer {
                Text("The agent needs an approval or answer in the terminal.").font(.caption).foregroundStyle(PhrenTheme.warning)
            }
            if let error = model.deliveryError { Text(error).font(.caption).foregroundStyle(PhrenTheme.warning).accessibilityIdentifier("chat-delivery-error") }
            HStack(alignment: .bottom, spacing: 10) {
                if project != nil {
                    Button { showingContext = true } label: {
                        Image(systemName: "plus").font(.system(size: 20)).frame(width: 32, height: 44)
                    }.accessibilityLabel("Add project context").disabled(model.target == nil || model.sending)
                }
                TextField("Message this agent…", text: $model.draft, axis: .vertical)
                    .lineLimit(1...6).focused($composing).font(.body)
                    .padding(.horizontal, 14).padding(.vertical, 12)
                    .background(PhrenTheme.surface, in: RoundedRectangle(cornerRadius: 20))
                    .accessibilityIdentifier("chat-composer")
                    .disabled(model.target == nil)
                Button {
                    composing = false
                    sendTask = Task { await model.send(session) }
                } label: {
                    if model.sending { ProgressView().frame(width: 44, height: 44) }
                    else { Image(systemName: "arrow.up").font(.system(size: 20, weight: .semibold)).frame(width: 44, height: 44) }
                }
                .foregroundStyle(canSend ? PhrenTheme.bgSunken : PhrenTheme.textDim)
                .background(canSend ? PhrenTheme.cyan : PhrenTheme.surfaceRaised, in: Circle())
                .disabled(!canSend)
                .accessibilityLabel("Send message").accessibilityIdentifier("chat-send")
            }
        }
        .padding(.horizontal, 16).padding(.top, 10).padding(.bottom, 8)
        .background(PhrenTheme.bgSunken)
    }
    private struct RunIdentity: Equatable { let active: Bool; let refresh: UUID }
    private var canSend: Bool {
        active && model.connected && !model.sending && !model.needsAnswer && !model.draft.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty
    }
}

private struct ChatBottomPosition: PreferenceKey {
    static let defaultValue: CGFloat = 0
    static func reduce(value: inout CGFloat, nextValue: () -> CGFloat) { value = nextValue() }
}

private struct ChatMessageRow: View {
    let message: AgentChatMessage
    var body: some View {
        if message.role == .tool {
            DisclosureGroup {
                Text(message.text).font(.caption.monospaced()).textSelection(.enabled)
                    .frame(maxWidth: .infinity, alignment: .leading).padding(.top, 8)
            } label: {
                Label(message.title ?? "Tool activity", systemImage: "terminal")
                    .font(.subheadline).foregroundStyle(PhrenTheme.textMuted)
            }
            .padding(12).background(PhrenTheme.bgSunken, in: RoundedRectangle(cornerRadius: 14))
        } else {
            HStack {
                if message.role == .user { Spacer(minLength: 32) }
                VStack(alignment: .leading, spacing: 6) {
                    Text(message.role == .user ? "You" : "Agent").font(.caption.weight(.medium))
                        .foregroundStyle(message.role == .user ? PhrenTheme.cyan : PhrenTheme.lavender)
                    Text(.init(message.text)).font(.body).textSelection(.enabled)
                        .frame(maxWidth: .infinity, alignment: .leading)
                }
                .padding(14)
                .background(message.role == .user ? PhrenTheme.cyan.opacity(0.10) : PhrenTheme.surface, in: RoundedRectangle(cornerRadius: 18))
                if message.role != .user { Spacer(minLength: 12) }
            }
            .accessibilityIdentifier("chat-message:\(message.id)")
        }
    }
}
