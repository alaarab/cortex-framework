import PhrenKit
import PhrenLive
import SwiftUI

/// Every agent opens in Phren with its exact computer and conversation.
struct AgentConversationLink<LabelContent: View>: View {
    let session: LiveAgentSession
    var onOpenInPhren: (() -> Void)? = nil
    @ViewBuilder var label: LabelContent
    @State private var showingChat = false

    var body: some View {
        Button {
            if let onOpenInPhren { onOpenInPhren() }
            else { showingChat = true }
        } label: { label }
        .sheet(isPresented: $showingChat) { AgentChatSheet(session: session) }
    }
}

struct AgentChatSheet: View {
    let session: LiveAgentSession
    var body: some View {
        NavigationStack { AgentChatView(session: session) }
    }
}

struct AgentChatView: View {
    let session: LiveAgentSession
    @Environment(\.dismiss) private var dismiss
    @Environment(AppModel.self) private var appModel
    @Environment(\.scenePhase) private var scenePhase
    @Environment(\.accessibilityReduceMotion) private var reduceMotion
    @Environment(\.accessibilityVoiceOverEnabled) private var voiceOver
    @AppStorage("sessions.live.preferences.v1") private var hostData = Data()
    @State private var model = AgentChatModel()
    @State private var sendTask: Task<Void, Never>?
    @State private var visible = false
    @State private var refresh = UUID()
    @State private var showingContext = false
    @State private var commandDestination: CommandDestination?
    @State private var showingAttachments = false
    @State private var showingDictation = false
    @State private var previewImage: ChatAttachmentDraft?
    @State private var historyTask: Task<Void, Never>?
    @State private var atBottom = true
    @State private var nearHistoryTop = false
    @State private var paginationReady = false
    @State private var requestedHistoryLine: Int?
    @State private var scrollHeight: CGFloat = 0
    @ScaledMetric(relativeTo: .body) private var composerTextSize = 14.0
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
            chatHeader

            ScrollViewReader { proxy in
                ScrollView {
                    LazyVStack(alignment: .leading, spacing: 18) {
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
                                } else {
                                    NavigationLink {
                                        HerdrTerminalView(host: session.host, session: session, paneID: pane.id).toolbar(.visible, for: .navigationBar)
                                    } label: {
                                        Label("\(pane.displayTitle) · Open terminal", systemImage: "terminal").font(.subheadline)
                                    }
                                }
                            }
                            Text("Native chat supports Codex, Claude Code, and GitHub Copilot sessions recognized on this computer.")
                                .font(.footnote).foregroundStyle(PhrenTheme.textMuted)
                            if model.panes.contains(where: { $0.agent == "copilot" }) {
                                Link("Set up Copilot chat", destination: URL(string: "https://alaarab.github.io/phren/phren-hook.html")!)
                                    .font(.footnote)
                            }
                        }
                        if let error = model.error { connectionIssue(error) }
                        if currentHost != session.host { connectionIssue("This computer's connection changed. Reopen chat from the current session list.") }
                        if model.hasMore {
                            VStack(spacing: 8) {
                                if model.loadingHistory { ProgressView().accessibilityLabel("Loading earlier messages") }
                                else if model.historyError != nil {
                                    Button("Retry loading earlier messages") {
                                        requestedHistoryLine = nil
                                        loadHistoryIfNeeded(proxy)
                                    }.font(.caption)
                                }
                            }
                            .frame(maxWidth: .infinity, minHeight: 24)
                            .background(GeometryReader { geometry in
                                Color.clear.preference(key: ChatHistoryPosition.self, value: geometry.frame(in: .named("chat-scroll")).minY)
                            })
                            .accessibilityIdentifier("chat-history")
                        }
                        if model.history.reachedLimit {
                            Text("Showing the most recent loaded history to keep this chat responsive.").font(.caption).foregroundStyle(PhrenTheme.textDim)
                        }
                        ForEach(ChatTimelineEntry.group(model.messages)) { entry in
                            if entry.isActivity {
                                ChatToolActivity(messages: entry.messages).id(entry.id)
                            } else if let message = entry.messages.first {
                                ChatMessageRow(message: message, revealedText: model.reveal.visible[message.id], images: model.sentImages.filter { item in
                                    message.role == .user && item.path.map { message.text.contains($0) } == true
                                }, preview: { previewImage = $0 }, historical: {
                                    if let target = model.target {
                                        ForEach(message.imageBlocks, id: \.self) { block in
                                            ChatHistoricalImage(session: session, target: target, line: message.line, block: block, active: active, preview: { previewImage = $0 })
                                        }
                                    }
                                }).id(message.id)
                            }
                        }
                        if let prompt = model.question, model.needsAnswer, model.questionsSupported {
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
                .accessibilityIdentifier("chat-transcript")
                .contentShape(Rectangle())
                .simultaneousGesture(TapGesture().onEnded { composing = false })
                .modifier(ChatHistoryScrollObserver { near in
                    nearHistoryTop = near
                    loadHistoryIfNeeded(proxy)
                })
                .task(id: active && model.connected) {
                    paginationReady = false
                    guard active, model.connected else { return }
                    // Let the first backlog settle at the bottom before deciding
                    // whether the viewport needs an earlier page.
                    do { try await Task.sleep(for: .milliseconds(350)) } catch { return }
                    paginationReady = true
                    loadHistoryIfNeeded(proxy)
                }
                .onChange(of: model.history.startLine) { _, _ in loadHistoryIfNeeded(proxy) }
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
                .onChange(of: model.target?.id) { _, _ in
                    historyTask?.cancel(); requestedHistoryLine = nil
                    proxy.scrollTo("chat-bottom", anchor: .bottom)
                }
                .onChange(of: model.messages.last?.id) { _, _ in
                    if atBottom && !model.loadingHistory { withAnimation { proxy.scrollTo("chat-bottom", anchor: .bottom) } }
                }
                .onChange(of: model.reveal.revision) { _, _ in
                    if atBottom && !model.loadingHistory { proxy.scrollTo("chat-bottom", anchor: .bottom) }
                }
            }
            composer
                .dynamicTypeSize(...DynamicTypeSize.accessibility1)
        }
        .background(PhrenTheme.chatCanvas)
        .interactiveDismissDisabled(model.hasMore || model.loadingHistory)
        .navigationTitle("Agent chat")
        .navigationBarTitleDisplayMode(.inline)
        .toolbar(.hidden, for: .navigationBar)
        .onAppear { visible = true }
        .onDisappear { visible = false; sendTask?.cancel(); historyTask?.cancel() }
        .onChange(of: scenePhase) { _, phase in if phase != .active { sendTask?.cancel(); historyTask?.cancel() } }
        .onChange(of: currentHost) { _, _ in sendTask?.cancel(); historyTask?.cancel() }
        .onChange(of: reduceMotion || voiceOver, initial: true) { _, instant in
            model.animateReplies = !instant
            if instant { model.reveal.finish() }
        }
        .task(id: active && model.reveal.isRevealing) {
            guard active else { return }
            while model.reveal.isRevealing && !Task.isCancelled {
                do { try await Task.sleep(for: .milliseconds(33)) } catch { return }
                model.reveal.advance()
            }
        }
        .navigationDestination(item: $commandDestination) { destination in
            HerdrTerminalView(host: session.host, session: session, target: destination.menu ? model.target : nil,
                              paneID: destination.paneID, commandMenu: destination.menu)
                .toolbar(.visible, for: .navigationBar)
        }
        .onChange(of: commandDestination) { previous, current in
            if previous != nil && current == nil {
                // Commands chosen in the live menu can replace the session too.
                model.chooseAnother(); refresh = UUID()
            }
        }
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

    private func loadHistoryIfNeeded(_ proxy: ScrollViewProxy) {
        guard active, paginationReady, model.connected, model.hasMore, !model.loadingHistory,
              historyTask == nil, nearHistoryTop,
              let line = model.history.startLine, line > 0, requestedHistoryLine != line else { return }
        requestedHistoryLine = line
        let anchor = ChatTimelineEntry.group(model.messages).first?.id
        let target = model.target
        historyTask = Task {
            defer { historyTask = nil }
            await model.loadOlder(session)
            guard !Task.isCancelled, model.target == target else { return }
            await Task.yield()
            if let anchor {
                var transaction = Transaction(); transaction.disablesAnimations = true
                withTransaction(transaction) { proxy.scrollTo(anchor, anchor: .top) }
            }
        }
    }

    private var chatHeader: some View {
        HStack(spacing: 10) {
            Button { dismiss() } label: {
                Image(systemName: "xmark").font(.system(size: 15, weight: .medium)).frame(width: 36, height: 44).contentShape(Rectangle())
            }.accessibilityLabel("Done").accessibilityIdentifier("chat-close")
            VStack(alignment: .leading, spacing: 4) {
                HStack(spacing: 7) {
                    Circle().fill(model.connected && active ? PhrenTheme.cyan : PhrenTheme.textDim).frame(width: 6, height: 6)
                    Text(selectedPane?.displayTitle ?? session.workspaceName)
                        .font(.system(.subheadline, design: .monospaced).weight(.semibold)).lineLimit(1)
                }
                Text("\(session.host.name) · \(session.workspaceName) · \(model.target?.providerName ?? "Agent")")
                    .font(.system(.caption2, design: .monospaced)).foregroundStyle(PhrenTheme.textMuted)
                    .lineLimit(1).accessibilityIdentifier("chat-location")
            }.frame(maxWidth: .infinity, alignment: .leading)
            NavigationLink {
                HerdrTerminalView(host: session.host, session: session, target: model.target).toolbar(.visible, for: .navigationBar)
            } label: {
                Image(systemName: "terminal").font(.system(size: 17)).frame(width: 40, height: 44).contentShape(Rectangle())
                    .foregroundStyle(PhrenTheme.cyan)
            }.accessibilityLabel("Open Herdr terminal").accessibilityIdentifier("chat-terminal")
            chatOptions
        }
        .buttonStyle(.plain).foregroundStyle(PhrenTheme.text)
        .padding(.horizontal, 8).padding(.vertical, 8)
        .background(PhrenTheme.chatPanel, in: RoundedRectangle(cornerRadius: 22, style: .continuous))
        .overlay(RoundedRectangle(cornerRadius: 22).strokeBorder(PhrenTheme.borderStrong, lineWidth: 1))
        .padding(.horizontal, 12).padding(.top, 8).padding(.bottom, 4)
        .dynamicTypeSize(...DynamicTypeSize.accessibility1)
    }

    private var chatOptions: some View {
        Menu {
            NavigationLink { HerdrTerminalView(host: session.host, session: session, target: model.target).toolbar(.visible, for: .navigationBar) } label: { Label("Herdr terminal", systemImage: "terminal") }
            NavigationLink { HerdrWorkspacesView(hostID: session.host.id).toolbar(.visible, for: .navigationBar) } label: { Label("Herdr workspaces", systemImage: "rectangle.split.3x1") }
            if let target = model.target {
                NavigationLink { AgentDiffView(session: session, target: target).toolbar(.visible, for: .navigationBar) } label: { Label("Repository changes", systemImage: "arrow.triangle.branch") }
            }
            if let project {
                NavigationLink("Project memory") { ProjectDetailView(storeId: project.storeID, project: project.name).toolbar(.visible, for: .navigationBar) }
                NavigationLink("Project skills") { SkillsView(project: project.name, storeId: project.storeID).toolbar(.visible, for: .navigationBar) }
                NavigationLink("Explore graph") { GraphView(focusProject: project.name, initialStoreId: project.storeID).toolbar(.visible, for: .navigationBar) }
            }
            Button("Slash commands", systemImage: "slash.circle") { openCommandMenu() }
                .disabled(model.target == nil || model.sending)
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
        } label: { Image(systemName: "ellipsis").frame(width: 36, height: 44).contentShape(Rectangle()) }
        .accessibilityLabel("Chat options")
    }

    private func connectionIssue(_ message: String) -> some View {
        Label(message, systemImage: "wifi.exclamationmark")
            .font(.footnote).foregroundStyle(PhrenTheme.warning).padding(12).phrenCard()
    }

    @ViewBuilder private var tokenUsage: some View {
        if let usage = model.progress.usage {
            Menu {
                Text("Latest reported model response")
                Text("Input: \(usage.input.formatted()) tokens")
                Text("Output: \(usage.output.formatted()) tokens")
                if let cached = usage.cachedInput { Text("Cached input: \(cached.formatted()) tokens") }
                if let modelName = model.modelName { Text(modelName) }
            } label: {
                Text("\(usage.output.formatted(.number.notation(.compactName))) out · \(usage.input.formatted(.number.notation(.compactName))) in")
                    .font(.caption2.monospacedDigit()).foregroundStyle(PhrenTheme.textMuted)
            }.accessibilityIdentifier("chat-token-usage").accessibilityLabel("Latest reported usage: \(usage.output) output tokens, \(usage.input) input tokens")
        } else if model.progressUnavailable {
            Menu {
                Link("Set up token counts on this computer", destination: URL(string: "https://github.com/alaarab/phren/blob/main/apps/ios/README.md#live-token-counts")!)
            } label: {
                Text("Tokens unavailable").font(.caption2).foregroundStyle(PhrenTheme.textMuted)
            }
        }
    }

    private var composer: some View {
        VStack(alignment: .leading, spacing: 6) {
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

            if composing, AgentSlashCommand.isCommand(model.draft) {
                SlashCommandMenu(source: model.target?.source ?? "", draft: model.draft,
                                 choose: { model.draft = $0 + " " }, openAll: openCommandMenu)
            }
            if let status = model.deliveryStatus { Text(status).font(.caption).foregroundStyle(PhrenTheme.cyan) }
            HStack(spacing: 8) {
                if active, model.target != nil, !model.connected, !model.loading {
                    HStack {
                        Label("Reconnecting…", systemImage: "wifi.exclamationmark").font(.caption)
                        Spacer()
                        Button("Reconnect") { refresh = UUID() }
                            .font(.caption.weight(.semibold)).accessibilityIdentifier("chat-reconnect")
                    }.foregroundStyle(PhrenTheme.warning)
                } else if model.connected, model.awaitingReply || model.reveal.isRevealing || model.needsAnswer || model.approval != nil || model.progress.phase != nil || selectedPane?.agentStatus == "working" || model.liveActivity == "working" {
                    ChatActivityIndicator(waiting: model.awaitingReply, revealing: model.reveal.isRevealing,
                                          needsAnswer: model.needsAnswer || model.approval != nil,
                                          working: selectedPane?.agentStatus == "working" || (model.interactionConnected && model.liveActivity == "working"),
                                          progress: model.progress, sentAt: model.sentAt)
                }
                Spacer(minLength: 2)
                tokenUsage
            }.lineLimit(1).padding(.horizontal, 6)
            if model.needsAnswer {
                NavigationLink { HerdrTerminalView(host: session.host, session: session, target: model.target).toolbar(.visible, for: .navigationBar) } label: {
                    Label(model.approval != nil || model.question != nil ? "Or answer in Herdr" : "Answer in Herdr terminal", systemImage: "terminal")
                        .font(.caption).foregroundStyle(PhrenTheme.warning)
                }.accessibilityIdentifier("chat-answer-terminal")
            }
            if let error = model.deliveryError { Text(error).font(.caption).foregroundStyle(PhrenTheme.warning).accessibilityIdentifier("chat-delivery-error") }
            if let error = model.draftStorageError { Text(error).font(.caption).foregroundStyle(PhrenTheme.warning).accessibilityIdentifier("chat-draft-storage-error") }
            VStack(spacing: 0) {
                TextField("Message \(model.target?.providerName ?? "agent")…", text: $model.draft, axis: .vertical)
                    .lineLimit(1...4).focused($composing).font(.system(size: composerTextSize, design: .monospaced))
                    .tint(PhrenTheme.cyan).padding(.vertical, 8).padding(.horizontal, 12)
                    .frame(maxWidth: .infinity, minHeight: 36, alignment: .leading)
                    .accessibilityIdentifier("chat-composer").disabled(model.target == nil)
                HStack(alignment: .bottom, spacing: 4) {
                    Button { showingAttachments = true } label: {
                        Image(systemName: "plus").font(.system(size: 21, weight: .light)).frame(width: 36, height: 44).contentShape(Rectangle())
                    }.accessibilityLabel("Add attachment").disabled(model.target == nil || model.sending)
                    NavigationLink {
                        HerdrTerminalView(host: session.host, session: session, target: model.target).toolbar(.visible, for: .navigationBar)
                    } label: {
                        Image(systemName: "terminal").font(.system(size: 18)).frame(width: 44, height: 44).contentShape(Rectangle())
                    }.accessibilityLabel("Open Herdr terminal").accessibilityIdentifier("chat-composer-terminal")
                        .disabled(model.target == nil)
                    Spacer(minLength: 4)
                    Button { showingDictation = true } label: {
                        Image(systemName: "mic").font(.system(size: 20)).frame(width: 40, height: 44).contentShape(Rectangle())
                    }.accessibilityLabel("Dictate message").disabled(model.target == nil || model.sending)
                    if selectedPane?.agentStatus == "working", !model.needsAnswer {
                        Button { sendTask = Task { await model.stop(session) } } label: {
                            Image(systemName: "stop.circle").font(.system(size: 21)).frame(width: 40, height: 44).contentShape(Rectangle())
                        }.accessibilityLabel("Stop").accessibilityIdentifier("chat-stop")
                            .disabled(!active || !model.connected || model.sending || model.stopping)
                    }
                    Button {
                        composing = false
                        if model.draft.trimmingCharacters(in: .whitespacesAndNewlines) == "/", model.attachments.isEmpty {
                            openCommandMenu()
                        } else {
                            let isCommand = AgentSlashCommand.isCommand(model.draft), pane = model.target?.paneID
                            sendTask = Task {
                                await model.send(session)
                                if isCommand, model.deliveryError == nil, let pane {
                                    commandDestination = .init(paneID: pane, menu: false)
                                    // /new, /clear and /resume may change the session ID.
                                    model.chooseAnother()
                                }
                            }
                        }
                    } label: {
                        if model.sending { ProgressView().frame(width: 44, height: 44) }
                        else { Image(systemName: "arrow.up").font(.system(size: 22, weight: .semibold)).frame(width: 44, height: 44) }
                    }
                    .foregroundStyle(canSend ? PhrenTheme.chatPanel : PhrenTheme.textDim)
                    .background(canSend ? PhrenTheme.cyan : PhrenTheme.borderStrong, in: Circle())
                    .disabled(!canSend)
                    .accessibilityLabel("Send message").accessibilityIdentifier("chat-send")
                    .keyboardShortcut(.return, modifiers: .command)
                }
                .padding(.horizontal, 6).padding(.bottom, 4)
            }
            .padding(.top, 2)
            .background(PhrenTheme.chatPanel, in: RoundedRectangle(cornerRadius: 22))
            .overlay { RoundedRectangle(cornerRadius: 22).strokeBorder(PhrenTheme.border, lineWidth: 0.5) }
            .accessibilityElement(children: .contain)
            .accessibilityIdentifier("chat-message-box")
        }
        .buttonStyle(.plain).foregroundStyle(PhrenTheme.textSecondary)
        .padding(.horizontal, 10).padding(.top, 6).padding(.bottom, 2)
        .background(PhrenTheme.chatCanvas.ignoresSafeArea(.container, edges: .bottom))
    }
    private struct CommandDestination: Hashable { let paneID: String; let menu: Bool }
    private func openCommandMenu() {
        guard let target = model.target else { return }
        composing = false
        commandDestination = .init(paneID: target.paneID, menu: true)
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
    var revealedText: String? = nil
    let images: [ChatAttachmentDraft]
    let preview: (ChatAttachmentDraft) -> Void
    @ViewBuilder let historical: () -> Historical
    private var displayText: String {
        if let revealedText { return revealedText }
        let marker = "\n\nAttached files on this computer:\n"
        guard !images.isEmpty, let section = message.text.range(of: marker, options: .backwards) else { return message.text }
        let paths = message.text[section.upperBound...].components(separatedBy: "\n")
        let previewPaths = Set(images.compactMap(\.path))
        // Hide only our complete image attachment suffix when previews replace it.
        guard paths.allSatisfy({ previewPaths.contains($0) }) else { return message.text }
        return String(message.text[..<section.lowerBound])
    }
    var body: some View {
        HStack(alignment: .top, spacing: 0) {
            if message.role == .user { Spacer(minLength: 30) }
            VStack(alignment: .leading, spacing: 8) {
                ForEach(images) { item in
                    if let image = UIImage(data: item.attachment.data) {
                        Button { preview(item) } label: {
                            Image(uiImage: image).resizable().scaledToFit().frame(maxHeight: 220).clipShape(RoundedRectangle(cornerRadius: 12))
                        }.accessibilityLabel("View attached \(item.attachment.name)")
                    }
                }
                historical()
                if !displayText.isEmpty && !(displayText == "[Image attachment]" && !message.imageBlocks.isEmpty) { ChatRichText(text: displayText) }
                if revealedText != nil {
                    Capsule().fill(PhrenTheme.cyan).frame(width: 4, height: 13).accessibilityHidden(true)
                }
            }
            .padding(message.role == .user ? 14 : 0)
            .frame(maxWidth: .infinity, alignment: .leading)
            .background(message.role == .user ? PhrenTheme.accent.opacity(0.12) : .clear, in: RoundedRectangle(cornerRadius: 20, style: .continuous))
        }
        .accessibilityElement(children: .contain)
        .accessibilityLabel(message.role == .user ? "Your message" : "Agent reply")
        .accessibilityIdentifier("chat-message:\(message.id)")
        .contextMenu {
            Button("Copy message", systemImage: "doc.on.doc") { UIPasteboard.general.string = message.text }
            ShareLink(item: message.text)
        }
    }
}

private struct ChatHistoryPosition: PreferenceKey {
    static var defaultValue: CGFloat = -CGFloat.greatestFiniteMagnitude
    static func reduce(value: inout CGFloat, nextValue: () -> CGFloat) { value = nextValue() }
}

private struct ChatHistoryScrollObserver: ViewModifier {
    let changed: (Bool) -> Void
    func body(content: Content) -> some View {
        if #available(iOS 18.0, *) {
            content.onScrollGeometryChange(for: Bool.self) { geometry in
                geometry.contentOffset.y + geometry.contentInsets.top < 140
            } action: { _, near in changed(near) }
        } else {
            content.onPreferenceChange(ChatHistoryPosition.self) { position in changed(position >= -140) }
        }
    }
}
