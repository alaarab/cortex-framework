import PhrenKit
import SwiftUI

struct ChatActivityIndicator: View {
    let waiting: Bool
    let revealing: Bool
    let needsAnswer: Bool
    let working: Bool
    let progress: AgentChatProgress
    let sentAt: Date?
    private var busy: Bool { !needsAnswer && (waiting || revealing || working || progress.phase == .working) }
    private var label: String {
        if needsAnswer { return "Waiting for your answer" }
        if waiting { return "Waiting for agent…" }
        if revealing { return "Receiving reply…" }
        if working || progress.phase == .working { return "Agent is working" }
        if progress.phase == .stopped { return "Stopped" }
        if progress.phase == .finished { return "Finished" }
        return "Ready"
    }
    var body: some View {
        HStack(spacing: 7) {
            if busy { ProgressView().controlSize(.mini).tint(PhrenTheme.cyan).accessibilityHidden(true) }
            else { Image(systemName: needsAnswer ? "pause.circle" : "checkmark.circle").accessibilityHidden(true) }
            Text(label).accessibilityIdentifier("chat-activity")
            if busy, let start = waiting ? sentAt : progress.startedAt, start <= .now {
                Text(start, style: .timer).monospacedDigit().fixedSize().accessibilityLabel("Elapsed time")
            }
        }.font(.caption).foregroundStyle(needsAnswer ? PhrenTheme.warning : PhrenTheme.cyan)
    }
}
