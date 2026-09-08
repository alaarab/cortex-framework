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
}

@Observable @MainActor
final class AgentChatModel {
    var panes: [AgentChatPanes.Pane] = []
    var target: AgentChatTarget?
    var history = AgentChatHistory()
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
    var receivedAt: Date?
    var deliveryStatus: String?
    var draft = "" { didSet { if let target { AgentChatDrafts.text[target.id] = draft } } }
    var attachments: [ChatAttachmentDraft] = [] { didSet { if let target { AgentChatDrafts.attachments[target.id] = attachments } } }
    var sentImages: [ChatAttachmentDraft] = []
    private var generation = UUID()
    private var streamTask: Task<Void, Never>?
    private var streamTarget: AgentChatTarget?

    func choose(_ pane: AgentChatPanes.Pane, session: DiscoveredMoshiSession) {
        do {
            let chosen = try pane.target(hostID: session.host.id, workspaceID: session.workspaceID, tabID: session.tab.id)
            target = chosen
            draft = AgentChatDrafts.text[chosen.id] ?? ""
            attachments = AgentChatDrafts.attachments[chosen.id] ?? []
            history = .init(); connected = false; error = nil; deliveryError = nil
            sentImages = []; needsAnswer = false
        } catch { self.error = error.localizedDescription }
    }
    func chooseAnother() {
        streamTask?.cancel(); streamTask = nil; streamTarget = nil
        target = nil; history = .init(); connected = false
        draft = ""; attachments = []; sentImages = []; deliveryError = nil; needsAnswer = false
    }
    func add(_ attachment: AgentAttachment) {
        guard attachments.count < 4 else { deliveryError = "Attach up to four files in one message."; return }
        attachments.append(.init(attachment: attachment)); deliveryError = nil
    }

    func run(_ session: DiscoveredMoshiSession) async {
        let run = UUID(); generation = run; loading = true
        defer {
            if generation == run {
                streamTask?.cancel(); streamTask = nil; streamTarget = nil
                connected = false; loading = false
            }
        }
        while !Task.isCancelled {
            do {
                let list = try await Self.fetchPanes(session)
                try Task.checkCancellation()
                guard generation == run else { return }
                panes = list.panes
                if target == nil {
                    let supported = panes.filter { (try? $0.target(hostID: session.host.id, workspaceID: session.workspaceID, tabID: session.tab.id)) != nil }
                    if supported.count == 1 { choose(supported[0], session: session) }
                }
                if let target {
                    needsAnswer = try list.validate(target).needsAnswer
                    if streamTarget != target { beginStream(session, target: target, run: run) }
                }
                loading = false
            } catch {
                guard !Task.isCancelled, generation == run else { return }
                streamTask?.cancel(); streamTask = nil; streamTarget = nil
                connected = false; loading = false; self.error = error.localizedDescription
            }
            do { try await Task.sleep(for: .seconds(3)) } catch { return }
        }
    }

    private func beginStream(_ session: DiscoveredMoshiSession, target: AgentChatTarget, run: UUID) {
        streamTask?.cancel(); streamTarget = target
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
                let updates = MoshiConnection.chatUpdates(host: session.host, privateKey: try DeviceSSHKey.load(session.host.id), target: target)
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
    private func accept(_ frame: AgentChatTranscript) {
        history.receive(frame); connected = true; receivedAt = .now; error = nil; loading = false
    }

    func loadOlder(_ session: DiscoveredMoshiSession) async {
        guard !loadingHistory, let target, let before = history.startLine, before > 0 else { return }
        loadingHistory = true
        defer { loadingHistory = false }
        do {
            let page: AgentChatTranscript
            #if DEBUG && targetEnvironment(simulator)
            if AgentChatFixture.enabled { page = try AgentChatFixture.older(target) }
            else { page = try await MoshiConnection.chatHistory(host: session.host, privateKey: DeviceSSHKey.load(session.host.id), target: target, beforeLine: before) }
            #else
            page = try await MoshiConnection.chatHistory(host: session.host, privateKey: DeviceSSHKey.load(session.host.id), target: target, beforeLine: before)
            #endif
            guard self.target == target else { return }
            history.receive(page)
        } catch { self.error = "Couldn't load earlier messages. \(error.localizedDescription)" }
    }

    func stop(_ session: DiscoveredMoshiSession) async {
        guard !stopping, !sending, connected, !needsAnswer, let target else { return }
        stopping = true; deliveryError = nil
        defer { stopping = false }
        do {
            #if DEBUG && targetEnvironment(simulator)
            if AgentChatFixture.enabled { AgentChatFixture.stopped = true }
            else { try await MoshiConnection.stopChatTurn(host: session.host, privateKey: DeviceSSHKey.load(session.host.id), target: target) }
            #else
            try await MoshiConnection.stopChatTurn(host: session.host, privateKey: DeviceSSHKey.load(session.host.id), target: target)
            #endif
            deliveryStatus = "Stop requested"
        } catch { deliveryError = "Stop wasn't confirmed. \(error.localizedDescription)" }
    }

    /// Uploads can be reused after failure; prompt delivery is never replayed.
    func send(_ session: DiscoveredMoshiSession) async {
        guard !sending, connected, !needsAnswer, let target,
              !draft.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty || !attachments.isEmpty else { return }
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
                else { path = try await MoshiConnection.uploadChatAttachment(host: session.host, privateKey: DeviceSSHKey.load(session.host.id), target: target, attachment: sent[index].attachment) }
                #else
                path = try await MoshiConnection.uploadChatAttachment(host: session.host, privateKey: DeviceSSHKey.load(session.host.id), target: target, attachment: sent[index].attachment)
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
        do {
            #if DEBUG && targetEnvironment(simulator)
            if AgentChatFixture.enabled { try await AgentChatFixture.send(target, text: text) }
            else { try await MoshiConnection.sendChat(host: session.host, privateKey: DeviceSSHKey.load(session.host.id), target: target, text: text) }
            #else
            try await MoshiConnection.sendChat(host: session.host, privateKey: DeviceSSHKey.load(session.host.id), target: target, text: text)
            #endif
            if draft == submitted { draft = "" }
            // Keep small local previews, not full uploaded files, in the conversation.
            sentImages += sent.filter { $0.attachment.isImage }.compactMap { item in
                ChatAttachmentPreparation.preview(item.attachment).map { ChatAttachmentDraft(attachment: $0, path: item.path) }
            }
            if sentImages.count > 16 { sentImages.removeFirst(sentImages.count - 16) }
            attachments.removeAll { submittedIDs.contains($0.id) }
        } catch {
            deliveryError = "Delivery wasn't confirmed. Check the conversation before trying again. \(error.localizedDescription)"
        }
    }
    private static func fetchPanes(_ session: DiscoveredMoshiSession) async throws -> AgentChatPanes {
        #if DEBUG && targetEnvironment(simulator)
        if AgentChatFixture.enabled { return try AgentChatFixture.panes(session) }
        #endif
        return try await MoshiConnection.chatPanes(host: session.host, privateKey: DeviceSSHKey.load(session.host.id), workspaceID: session.workspaceID, tabID: session.tab.id)
    }
}
