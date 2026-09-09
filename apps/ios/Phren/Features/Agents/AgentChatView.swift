import PhrenKit
import PhrenLive
import SwiftUI

/// Shared entry point; the preference changes where a normal agent tap opens.
struct AgentConversationLink<LabelContent: View>: View {
    let session: DiscoveredMoshiSession
    var honorsPreference = true
    var onOpenInPhren: (() -> Void)? = nil
    @ViewBuilder var label: LabelContent
    @AppStorage("agents.preferMoshi.v1") private var preferMoshi = false
    @Environment(\.openURL) private var openURL
    @State private var showingChat = false
    @State private var error: String?

    var body: some View {
        Button {
            if honorsPreference && preferMoshi, let url = try? session.link().url() {
                openURL(url) { accepted in if !accepted { error = "Moshi couldn't be opened. Choose Phren chat in Settings or open Moshi on this iPhone." } }
            } else if let onOpenInPhren { onOpenInPhren() }
            else { showingChat = true }
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
    @State private var showingAttachments = false
    @State private var showingDictation = false
    @State private var previewImage: ChatAttachmentDraft?
    @State private var historyTask: Task<Void, Never>?
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
            .dynamicTypeSize(...DynamicTypeSize.accessibility1)

            ScrollViewReader { proxy in
                ScrollView {
                    LazyVStack(alignment: .leading, spacing: 16) {
                        if model.loading && model.messages.isEmpty { ProgressView("Opening conversation…").frame(maxWidth: .infinity).padding(.top, 40) }
                        if model.target == nil && !model.loading {
                            Text("Choose an agent").font(.title2.weight(.semibold))
                            ForEach(model.panes) { pane in
                                if (try? pane.target(hostID: session.host.id, workspaceID: session.workspaceID, tabID: session.tab.id, muxID: session.host.muxID)) != nil {
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
                        if model.hasMore {
                            Button {
                                let anchor = model.messages.first?.id
                                historyTask = Task {
                                    await model.loadOlder(session)
                                    if let anchor { proxy.scrollTo(anchor, anchor: .top) }
                                }
                            } label: {
                                if model.loadingHistory { ProgressView() }
                                else { Label("Load earlier messages", systemImage: "clock.arrow.circlepath") }
                            }.disabled(model.loadingHistory || !active).accessibilityIdentifier("chat-history")
                        }
                        if model.history.reachedLimit {
                            Text("Showing the most recent loaded history to keep this chat responsive.").font(.caption).foregroundStyle(PhrenTheme.textDim)
                        }
                        ForEach(model.messages) { message in
                            ChatMessageRow(message: message, images: model.sentImages.filter { item in
                                message.role == .user && item.path.map { message.text.contains($0) } == true
                            }, preview: { previewImage = $0 }, historical: {
                                if let target = model.target {
                                    ForEach(message.imageBlocks, id: \.self) { block in
                                        ChatHistoricalImage(session: session, target: target, line: message.line, block: block, active: active, preview: { previewImage = $0 })
                                    }
                                }
                            }).id(message.id)
                        }
                        if let prompt = model.question, model.needsAnswer {
                            ChatQuestionCard(prompt: prompt, busy: model.answering || !active || !model.connected) { selections in
                                sendTask = Task { await model.answer(session, question: prompt, selections: selections) }
                            }.id(prompt.id)
                        } else if let approval = model.approval {
                            ChatApprovalCard(approval: approval, busy: model.answering || !active || !model.interactionConnected) { approve in
                                sendTask = Task { await model.answer(session, approval: approval, approve: approve) }
                            }.id(approval.id)
                        }
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
                .dynamicTypeSize(...DynamicTypeSize.accessibility1)
        }
        .background(PhrenTheme.bg)
        .navigationTitle("Agent chat")
        .navigationBarTitleDisplayMode(.inline)
        .toolbar {
            ToolbarItem(placement: .primaryAction) {
                Menu {
                    NavigationLink { HerdrTerminalView(host: session.host, session: session, target: model.target) } label: { Label("Herdr terminal", systemImage: "terminal") }
                    NavigationLink { HerdrWorkspacesView(hostID: session.host.id) } label: { Label("Herdr workspaces", systemImage: "rectangle.split.3x1") }
                    if let target = model.target {
                        NavigationLink { AgentDiffView(session: session, target: target) } label: { Label("Repository changes", systemImage: "arrow.triangle.branch") }
                    }
                    if let destination = try? session.link().url() {
                        Link("Open terminal in Moshi", destination: destination)
                    }
                    if let project {
                        NavigationLink("Project memory") { ProjectDetailView(storeId: project.storeID, project: project.name) }
                        NavigationLink("Project skills") { SkillsView(project: project.name, storeId: project.storeID) }
                        NavigationLink("Explore graph") { GraphView(focusProject: project.name, initialStoreId: project.storeID) }
                    }
                    Button("Refresh conversation") { refresh = UUID() }
                    Button("Dictate message", systemImage: "mic") { showingDictation = true }
                        .disabled(model.target == nil || model.sending)
                    if project != nil {
                        Button("Add project context", systemImage: "brain") { showingContext = true }
                    }
                    if model.panes.filter({ (try? $0.target(hostID: session.host.id, workspaceID: session.workspaceID, tabID: session.tab.id, muxID: session.host.muxID)) != nil }).count > 1 {
                        Button("Choose another agent") {
                            model.chooseAnother()
                            refresh = UUID()
                        }.disabled(model.sending)
                    }
                } label: { Image(systemName: "ellipsis") }
                .accessibilityLabel("Chat options")
            }
        }
        .onAppear { visible = true }
        .onDisappear { visible = false; sendTask?.cancel(); historyTask?.cancel() }
        .onChange(of: scenePhase) { _, phase in if phase != .active { sendTask?.cancel(); historyTask?.cancel() } }
        .onChange(of: currentHost) { _, _ in sendTask?.cancel(); historyTask?.cancel() }
        .sheet(isPresented: $showingAttachments) {
            if let openingTarget = model.target {
                ChatAttachmentPicker(canAdd: model.attachments.count < 4, add: { item in
                    if model.target == openingTarget { model.add(item) }
                }, context: project == nil ? nil : {
                    Task { try? await Task.sleep(for: .milliseconds(350)); showingContext = true }
                })
            }
        }
        .sheet(isPresented: $showingDictation) {
            if let openingTarget = model.target {
                ChatDictationView { text in
                    if model.target == openingTarget { model.draft += (model.draft.isEmpty ? "" : "\n\n") + text }
                }
            }
        }
        .sheet(item: $previewImage) { item in
            NavigationStack {
                if let image = UIImage(data: item.attachment.data) {
                    Image(uiImage: image).resizable().scaledToFit().padding()
                        .navigationTitle(item.attachment.name).navigationBarTitleDisplayMode(.inline)
                        .toolbar { ToolbarItem(placement: .confirmationAction) { Button("Done") { previewImage = nil } } }
                }
            }
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
            if !model.attachments.isEmpty {
                ScrollView(.horizontal) {
                    HStack(spacing: 10) {
                        ForEach(model.attachments) { item in
                            VStack(spacing: 4) {
                                HStack(spacing: 6) {
                                    Button { if item.attachment.isImage { previewImage = item } } label: {
                                        if item.attachment.isImage, let image = UIImage(data: item.attachment.data) {
                                            Image(uiImage: image).resizable().scaledToFill().frame(width: 56, height: 56).clipped().clipShape(RoundedRectangle(cornerRadius: 10))
                                        } else { Image(systemName: "doc").frame(width: 56, height: 56) }
                                    }.accessibilityLabel("Preview \(item.attachment.name)")
                                    Button {
                                        model.attachments.removeAll { $0.id == item.id }
                                    } label: { Image(systemName: "xmark.circle.fill").font(.system(size: 20)).frame(width: 44, height: 44) }
                                        .disabled(model.sending).accessibilityLabel("Remove \(item.attachment.name)")
                                }
                                Text(item.attachment.name).font(.caption).lineLimit(1).frame(maxWidth: 120)
                            }.padding(8).background(PhrenTheme.surface, in: RoundedRectangle(cornerRadius: 14))
                        }
                    }
                }.accessibilityIdentifier("chat-attachments")
            }
            if let status = model.deliveryStatus { Text(status).font(.caption).foregroundStyle(PhrenTheme.cyan) }
            if active, model.target != nil, !model.connected, !model.loading {
                HStack {
                    Label("Reconnecting…", systemImage: "wifi.exclamationmark").font(.caption)
                    Spacer()
                    Button("Reconnect") { refresh = UUID() }
                        .font(.caption.weight(.semibold)).accessibilityIdentifier("chat-reconnect")
                }.foregroundStyle(PhrenTheme.warning)
            } else if selectedPane?.agentStatus == "working", !model.needsAnswer {
                HStack {
                    Label("Agent is working", systemImage: "waveform").font(.caption).foregroundStyle(PhrenTheme.cyan)
                    Spacer()
                    Button("Stop", systemImage: "stop.circle") { sendTask = Task { await model.stop(session) } }
                        .disabled(!active || !model.connected || model.sending || model.stopping).accessibilityIdentifier("chat-stop")
                }
            }
            if model.needsAnswer {
                NavigationLink { HerdrTerminalView(host: session.host, session: session, target: model.target) } label: {
                    Label(model.approval != nil || model.question != nil ? "Or answer in Herdr" : "Answer in Herdr terminal", systemImage: "terminal")
                        .font(.caption).foregroundStyle(PhrenTheme.warning)
                }.accessibilityIdentifier("chat-answer-terminal")
            }
            if let error = model.deliveryError { Text(error).font(.caption).foregroundStyle(PhrenTheme.warning).accessibilityIdentifier("chat-delivery-error") }
            if let error = model.draftStorageError { Text(error).font(.caption).foregroundStyle(PhrenTheme.warning).accessibilityIdentifier("chat-draft-storage-error") }
            HStack(alignment: .bottom, spacing: 10) {
                    Button { showingAttachments = true } label: {
                        Image(systemName: "plus").font(.system(size: 20)).frame(width: 32, height: 44)
                    }.accessibilityLabel("Add attachment").disabled(model.target == nil || model.sending)
                TextField("Message this agent…", text: $model.draft, axis: .vertical)
                    .lineLimit(1...6).focused($composing).font(.body)
                    .padding(.horizontal, 14).padding(.vertical, 12)
                    .background(PhrenTheme.surface, in: RoundedRectangle(cornerRadius: 20))
                    .accessibilityIdentifier("chat-composer")
                    .disabled(model.target == nil)
                Button { showingDictation = true } label: {
                    Image(systemName: "mic").font(.system(size: 20)).frame(width: 32, height: 44)
                }.accessibilityLabel("Dictate message").disabled(model.target == nil || model.sending)
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
                .keyboardShortcut(.return, modifiers: .command)
            }
        }
        .padding(.horizontal, 16).padding(.top, 10).padding(.bottom, 8)
        .background(PhrenTheme.bgSunken)
    }
    private struct RunIdentity: Equatable { let active: Bool; let refresh: UUID }
    private var canSend: Bool {
        active && model.connected && !model.sending && !model.stopping && !model.answering && !model.needsAnswer && model.approval == nil
            && (!model.draft.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty || !model.attachments.isEmpty)
    }
}

private struct ChatBottomPosition: PreferenceKey {
    static let defaultValue: CGFloat = 0
    static func reduce(value: inout CGFloat, nextValue: () -> CGFloat) { value = nextValue() }
}

private struct ChatMessageRow<Historical: View>: View {
    let message: AgentChatMessage
    let images: [ChatAttachmentDraft]
    let preview: (ChatAttachmentDraft) -> Void
    @ViewBuilder let historical: () -> Historical
    private var displayText: String {
        let marker = "\n\nAttached files on this computer:\n"
        guard !images.isEmpty, let section = message.text.range(of: marker, options: .backwards) else { return message.text }
        let paths = message.text[section.upperBound...].components(separatedBy: "\n")
        let previewPaths = Set(images.compactMap(\.path))
        // Hide only our complete image attachment suffix when previews replace it.
        guard paths.allSatisfy({ previewPaths.contains($0) }) else { return message.text }
        return String(message.text[..<section.lowerBound])
    }
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
                    ForEach(images) { item in
                        if let image = UIImage(data: item.attachment.data) {
                            Button { preview(item) } label: {
                                Image(uiImage: image).resizable().scaledToFit().frame(maxHeight: 220).clipShape(RoundedRectangle(cornerRadius: 12))
                            }.accessibilityLabel("View attached \(item.attachment.name)")
                        }
                    }
                    historical()
                    if !displayText.isEmpty && !(displayText == "[Image attachment]" && !message.imageBlocks.isEmpty) { ChatRichText(text: displayText) }
                }
                .padding(14)
                .background(message.role == .user ? PhrenTheme.cyan.opacity(0.10) : PhrenTheme.surface, in: RoundedRectangle(cornerRadius: 18))
                if message.role != .user { Spacer(minLength: 12) }
            }
            .accessibilityIdentifier("chat-message:\(message.id)")
            .contextMenu {
                Button("Copy message", systemImage: "doc.on.doc") { UIPasteboard.general.string = message.text }
                ShareLink(item: message.text)
            }
        }
    }
}
