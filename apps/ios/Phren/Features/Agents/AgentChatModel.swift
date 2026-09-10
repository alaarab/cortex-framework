import PhrenKit
import PhrenLive
import SwiftUI

struct ChatAttachmentDraft: Identifiable, Equatable {
    let attachment: AgentAttachment
    var path: String?
    var id: UUID { attachment.id }
}

@MainActor private enum AgentChatDrafts {
    static var text: [String: String] = [:]
    static var attachments: [String: [ChatAttachmentDraft]] = [:]
    static let store: AgentDraftStore? = {
        #if DEBUG && targetEnvironment(simulator)
        if AppModel.isUITesting && !ProcessInfo.processInfo.arguments.contains("--chat-persistent-draft") { return nil }
        #endif
        let root = FileManager.default.urls(for: .applicationSupportDirectory, in: .userDomainMask)[0]
            .appendingPathComponent(AppModel.isUITesting ? "AgentDraftTests" : "AgentDrafts", isDirectory: true)
        #if DEBUG && targetEnvironment(simulator)
        if AppModel.isUITesting && ProcessInfo.processInfo.arguments.contains("--chat-clear-drafts") { try? FileManager.default.removeItem(at: root) }
        #endif
        return AgentDraftStore(root: root)
    }()
}

@Observable @MainActor
final class AgentChatModel {
    var panes: [AgentChatPanes.Pane] = []
    var target: AgentChatTarget?
    var history = AgentChatHistory()
    var progress = AgentChatProgress()
    let reveal = ChatTextReveal()
    var animateReplies = true
    private var hasTranscript = false
    private(set) var awaitingReply = false
    private(set) var sentAt: Date?
    private var submittedAfterLine = -1
    var liveActivity: String?
    var modelName: String?
    var questionsSupported = true
    var progressUnavailable = false
    var messages: [AgentChatMessage] { history.messages }
    var hasMore: Bool { history.hasMore }
    var error: String?
    var deliveryError: String?
    var loading = true
    var connected = false
    var sending = false
    var loadingHistory = false
    var stopping = false
    var needsAnswer = false
    var approval: AgentApproval?
    var question: AgentQuestionPrompt?
    var interactionConnected = false
    var answering = false
    private var answeredQuestions: Set<String> = []
    private var statusTask: Task<Void, Never>?
    private var progressTask: Task<Void, Never>?
    private var progressConnected = false
    private var statusGeneration = UUID()
    var receivedAt: Date?
    var deliveryStatus: String?
    var draftStorageError: String?
    private var restoringDraft = false
    var draft = "" { didSet { if let target { AgentChatDrafts.text[target.id] = draft; persistDraft() } } }
    var attachments: [ChatAttachmentDraft] = [] { didSet { if let target { AgentChatDrafts.attachments[target.id] = attachments; persistDraft() } } }
    var sentImages: [ChatAttachmentDraft] = []
    private var generation = UUID()
    private var streamTask: Task<Void, Never>?
    private var streamTarget: AgentChatTarget?

    func choose(_ pane: AgentChatPanes.Pane, session: LiveAgentSession) {
        do {
            let chosen = try pane.target(hostID: session.host.id, workspaceID: session.workspaceID, tabID: session.tab.id, muxID: session.host.muxID)
            target = chosen
            restoringDraft = true
            defer { restoringDraft = false }
            var saved = AgentDraftStore.Draft()
            draftStorageError = nil
            do { saved = try AgentChatDrafts.store?.load(target: chosen.id) ?? saved }
            catch { draftStorageError = error.localizedDescription }
            draft = AgentChatDrafts.text[chosen.id] ?? saved.text
            attachments = AgentChatDrafts.attachments[chosen.id] ?? saved.attachments.map { .init(attachment: $0) }
            history = .init(); progress = .init(); reveal.finish(); hasTranscript = false
            awaitingReply = false; sentAt = nil; liveActivity = nil; modelName = nil
            connected = false; error = nil; deliveryError = nil
            sentImages = []; needsAnswer = false; approval = nil; question = nil; answeredQuestions = []
        } catch { self.error = error.localizedDescription }
    }
    private func persistDraft() {
        guard !restoringDraft, let target else { return }
        do {
            try AgentChatDrafts.store?.save(.init(text: draft, attachments: attachments.map(\.attachment)), target: target.id)
            draftStorageError = nil
        } catch { draftStorageError = error.localizedDescription }
    }
    func chooseAnother() {
        progressTask?.cancel(); progressTask = nil
        streamTask?.cancel(); streamTask = nil; streamTarget = nil
        statusTask?.cancel(); statusTask = nil; interactionConnected = false; approval = nil
        target = nil; history = .init(); connected = false
        progress = .init(); reveal.finish(); hasTranscript = false; awaitingReply = false; sentAt = nil; liveActivity = nil; modelName = nil
        draft = ""; attachments = []; sentImages = []; deliveryError = nil; needsAnswer = false
    }
    func add(_ attachment: AgentAttachment) {
        guard attachments.count < 4 else { deliveryError = "Attach up to four files in one message."; return }
        attachments.append(.init(attachment: attachment)); deliveryError = nil
    }

    func run(_ session: LiveAgentSession) async {
        let run = UUID(); generation = run; loading = true
        defer {
            if generation == run {
                progressTask?.cancel(); progressTask = nil
                streamTask?.cancel(); streamTask = nil; streamTarget = nil
                statusTask?.cancel(); statusTask = nil; interactionConnected = false; approval = nil
                connected = false; loading = false
                reveal.finish()
            }
        }
        while !Task.isCancelled {
            do {
                let list = try await Self.fetchPanes(session)
                try Task.checkCancellation()
                guard generation == run else { return }
                panes = list.panes
                if target == nil {
                    let supported = panes.filter { (try? $0.target(hostID: session.host.id, workspaceID: session.workspaceID, tabID: session.tab.id, muxID: session.host.muxID)) != nil }
                    if supported.count == 1 { choose(supported[0], session: session) }
                }
                if let target {
                    needsAnswer = try list.validate(target).needsAnswer || approval != nil
                    if target.source == "copilot" { liveActivity = try list.validate(target).agentStatus }
                    if needsAnswer { awaitingReply = false }
                    if streamTarget != target { beginStream(session, target: target, run: run) }
                }
                loading = false
            } catch {
                guard !Task.isCancelled, generation == run else { return }
                progressTask?.cancel(); progressTask = nil
                streamTask?.cancel(); streamTask = nil; streamTarget = nil
                statusTask?.cancel(); statusTask = nil; interactionConnected = false; approval = nil
                connected = false; loading = false; self.error = error.localizedDescription
            }
            do { try await Task.sleep(for: .seconds(3)) } catch { return }
        }
    }

    private func beginStream(_ session: LiveAgentSession, target: AgentChatTarget, run: UUID) {
        streamTask?.cancel(); streamTarget = target
        beginStatus(session, target: target, run: run)
        beginProgress(session, target: target, run: run)
        streamTask = Task {
            do {
                #if DEBUG && targetEnvironment(simulator)
                if AgentChatFixture.enabled {
                    while !Task.isCancelled {
                        accept(try AgentChatFixture.transcript(target))
                        try await Task.sleep(for: .milliseconds(500))
                    }
                    return
                }
                #endif
                let updates = PhrenConnection.chatUpdates(host: session.host, privateKey: try DeviceSSHKey.load(session.host.id), target: target)
                for try await frame in updates {
                    try Task.checkCancellation()
                    guard self.target == target, generation == run else { return }
                    accept(frame)
                }
                throw LiveConnectionError.disconnected
            } catch {
                guard !Task.isCancelled, generation == run, self.target == target else { return }
                streamTarget = nil; connected = false; self.error = "Reconnecting… \(error.localizedDescription)"
            }
        }
    }
    func accept(_ frame: AgentChatTranscript) {
        if frame.kind == .backlog { question = nil }
        for event in frame.questionEvents {
            switch event {
            case .question(let prompt): if !answeredQuestions.contains(prompt.id) { question = prompt }
            case .resolved(let id): if question?.id == id { question = nil }
            }
        }
        reveal.receive(frame, previous: messages, animated: animateReplies && hasTranscript)
        if frame.messages.contains(where: { $0.line > submittedAfterLine && $0.role != .user }) { awaitingReply = false }
        if !progressConnected, !frame.progressEvents.isEmpty { acceptProgress(frame) }
        history.receive(frame); hasTranscript = true; connected = true; receivedAt = .now; error = nil; loading = false
    }

    func acceptProgress(_ frame: AgentChatTranscript) {
        progress.receive(frame)
        if frame.progressEvents.contains(where: { event in
            guard event.line > submittedAfterLine else { return false }
            switch event.value { case .started, .finished, .stopped: return true; default: return false }
        }) { awaitingReply = false }
    }

    private func beginProgress(_ session: LiveAgentSession, target: AgentChatTarget, run: UUID) {
        // Phren Hook includes real lifecycle and usage events in the chat stream.
        progressTask?.cancel(); progressTask = nil
        progressConnected = false; progressUnavailable = false
    }

    private func beginStatus(_ session: LiveAgentSession, target: AgentChatTarget, run: UUID) {
        statusTask?.cancel(); interactionConnected = false; approval = nil
        let statusRun = UUID(); statusGeneration = statusRun
        statusTask = Task {
            while !Task.isCancelled {
                do {
                    #if DEBUG && targetEnvironment(simulator)
                    if AgentChatFixture.enabled {
                        approval = try AgentChatFixture.approval(target)
                        interactionConnected = true
                        return
                    }
                    #endif
                    for try await status in PhrenConnection.interactionUpdates(host: session.host, privateKey: try DeviceSSHKey.load(session.host.id), target: target) {
                        try Task.checkCancellation()
                        guard self.target == target, generation == run, statusGeneration == statusRun else { return }
                        if awaitingReply, liveActivity != "working", status.activity == "working" { awaitingReply = false }
                        approval = status.approval; questionsSupported = status.questionsSupported; liveActivity = status.activity; modelName = status.modelName; interactionConnected = true
                        if approval != nil || ["waiting", "blocked"].contains(status.activity ?? "") { awaitingReply = false }
                    }
                } catch {}
                guard !Task.isCancelled, self.target == target, generation == run, statusGeneration == statusRun else { return }
                approval = nil; interactionConnected = false
                do { try await Task.sleep(for: .seconds(3)) } catch { return }
            }
        }
    }

    func answer(_ session: LiveAgentSession, approval expected: AgentApproval? = nil, approve: Bool = false,
                question prompt: AgentQuestionPrompt? = nil, selections: [[Int]] = []) async {
        guard !answering, !sending, connected, let target else { return }
        guard (expected != nil && expected == approval && interactionConnected) || (prompt != nil && prompt == question && needsAnswer) else { return }
        answering = true; deliveryError = nil
        defer { answering = false }
        do {
            #if DEBUG && targetEnvironment(simulator)
            if AgentChatFixture.enabled { AgentChatFixture.answered = true }
            else { try await submitAnswer(session, target: target, approval: expected, approve: approve, question: prompt, selections: selections) }
            #else
            try await submitAnswer(session, target: target, approval: expected, approve: approve, question: prompt, selections: selections)
            #endif
            guard self.target == target else { return }
            approval = nil
            if let prompt { answeredQuestions.insert(prompt.id); question = nil }
            deliveryStatus = "Answer sent"
        } catch {
            approval = nil; question = nil
            deliveryError = "Answer wasn't confirmed. Refresh or open Herdr to check the current prompt. Your answer hasn't been retried."
        }
    }
    private func submitAnswer(_ session: LiveAgentSession, target: AgentChatTarget, approval: AgentApproval?, approve: Bool,
                              question: AgentQuestionPrompt?, selections: [[Int]]) async throws {
        let key = try DeviceSSHKey.load(session.host.id)
        if let approval { try await PhrenConnection.answerApproval(host: session.host, privateKey: key, target: target, actionID: approval.actionId, approve: approve) }
        else if let question { try await PhrenConnection.answerQuestions(host: session.host, privateKey: key, target: target, prompt: question, selections: selections) }
    }

    var historyError: String?

    func loadOlder(_ session: LiveAgentSession) async {
        guard !loadingHistory, let target, let before = history.startLine, before > 0 else { return }
        loadingHistory = true; historyError = nil
        defer { loadingHistory = false }
        do {
            let page: AgentChatTranscript
            #if DEBUG && targetEnvironment(simulator)
            if AgentChatFixture.enabled { page = try AgentChatFixture.older(target) }
            else { page = try await PhrenConnection.chatHistory(host: session.host, privateKey: DeviceSSHKey.load(session.host.id), target: target, beforeLine: before) }
            #else
            page = try await PhrenConnection.chatHistory(host: session.host, privateKey: DeviceSSHKey.load(session.host.id), target: target, beforeLine: before)
            #endif
            guard !Task.isCancelled, self.target == target else { return }
            history.receive(page)
        } catch is CancellationError {
        } catch { historyError = "Couldn't load earlier messages." }
    }

    func stop(_ session: LiveAgentSession) async {
        guard !stopping, !sending, connected, !needsAnswer, let target else { return }
        stopping = true; deliveryError = nil
        defer { stopping = false }
        do {
            #if DEBUG && targetEnvironment(simulator)
            if AgentChatFixture.enabled { AgentChatFixture.stopped = true }
            else { try await PhrenConnection.stopChatTurn(host: session.host, privateKey: DeviceSSHKey.load(session.host.id), target: target) }
            #else
            try await PhrenConnection.stopChatTurn(host: session.host, privateKey: DeviceSSHKey.load(session.host.id), target: target)
            #endif
            deliveryStatus = "Stop requested"
        } catch { deliveryError = "Stop wasn't confirmed. \(error.localizedDescription)" }
    }

    /// Uploads can be reused after failure; prompt delivery is never replayed.
    func send(_ session: LiveAgentSession) async {
        guard !sending, connected, !needsAnswer, let target,
              !draft.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty || !attachments.isEmpty else { return }
        guard !AgentSlashCommand.isCommand(draft) || attachments.isEmpty else {
            deliveryError = "Remove attachments before running a slash command."; return
        }
        let submitted = draft, submittedIDs = Set(attachments.map(\.id))
        var sent = attachments
        sending = true; deliveryError = nil
        defer { sending = false; deliveryStatus = nil }
        do {
            for index in sent.indices where sent[index].path == nil {
                deliveryStatus = "Uploading \(index + 1) of \(sent.count)…"
                let path: String
                #if DEBUG && targetEnvironment(simulator)
                if AgentChatFixture.enabled { path = try AgentChatFixture.upload(sent[index].attachment) }
                else { path = try await PhrenConnection.uploadChatAttachment(host: session.host, privateKey: DeviceSSHKey.load(session.host.id), target: target, attachment: sent[index].attachment) }
                #else
                path = try await PhrenConnection.uploadChatAttachment(host: session.host, privateKey: DeviceSSHKey.load(session.host.id), target: target, attachment: sent[index].attachment)
                #endif
                try Task.checkCancellation()
                sent[index].path = path
                if let original = attachments.firstIndex(where: { $0.id == sent[index].id }) { attachments[original].path = path }
            }
        } catch {
            deliveryError = "Attachment upload didn't finish. Your message hasn't been sent. \(error.localizedDescription)"
            return
        }
        let paths = sent.compactMap { $0.path }.joined(separator: "\n")
        let text = paths.isEmpty ? submitted : submitted + "\n\nAttached files on this computer:\n" + paths
        deliveryStatus = "Sending…"
        submittedAfterLine = max(0, history.totalLines) - 1
        sentAt = .now; awaitingReply = true
        do {
            #if DEBUG && targetEnvironment(simulator)
            if AgentChatFixture.enabled { try await AgentChatFixture.send(target, text: text) }
            else { try await PhrenConnection.sendChat(host: session.host, privateKey: DeviceSSHKey.load(session.host.id), target: target, text: text) }
            #else
            try await PhrenConnection.sendChat(host: session.host, privateKey: DeviceSSHKey.load(session.host.id), target: target, text: text)
            #endif
            if draft == submitted { draft = "" }
            if AgentSlashCommand.isCommand(submitted) { awaitingReply = false; sentAt = nil }
            // Keep small local previews, not full uploaded files, in the conversation.
            sentImages += sent.filter { $0.attachment.isImage }.compactMap { item in
                ChatAttachmentPreparation.preview(item.attachment).map { ChatAttachmentDraft(attachment: $0, path: item.path) }
            }
            if sentImages.count > 16 { sentImages.removeFirst(sentImages.count - 16) }
            attachments.removeAll { submittedIDs.contains($0.id) }
        } catch {
            awaitingReply = false
            deliveryError = "Delivery wasn't confirmed. Check the conversation before trying again. \(error.localizedDescription)"
        }
    }
    static func fetchPanes(_ session: LiveAgentSession) async throws -> AgentChatPanes {
        #if DEBUG && targetEnvironment(simulator)
        if AgentChatFixture.enabled { return try AgentChatFixture.panes(session) }
        #endif
        return try await PhrenConnection.chatPanes(host: session.host, privateKey: DeviceSSHKey.load(session.host.id), workspaceID: session.workspaceID, tabID: session.tab.id)
    }
}
