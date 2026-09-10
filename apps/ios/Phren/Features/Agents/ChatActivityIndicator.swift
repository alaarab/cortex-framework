import PhrenKit
import SwiftUI

struct ChatActivityIndicator: View {
    let connected: Bool
    let reconnecting: Bool
    let waiting: Bool
    let revealing: Bool
    let needsAnswer: Bool
    let working: Bool
    let progress: AgentChatProgress
    private var busy: Bool { connected && !needsAnswer && (waiting || revealing || working || progress.phase == .working) }
    private var label: String {
        if reconnecting { return "Reconnecting" }
        if !connected { return "Disconnected" }
        if needsAnswer { return "Waiting for your answer" }
        if waiting { return "Waiting for agent…" }
        if revealing { return "Receiving reply…" }
        if working || progress.phase == .working { return "Agent is working" }
        if progress.phase == .stopped { return "Stopped" }
        if progress.phase == .finished { return "Finished" }
        return "Ready"
    }
    var body: some View {
        Group {
            if busy { ProgressView().controlSize(.mini).tint(PhrenTheme.cyan) }
            else { Image(systemName: reconnecting ? "wifi.exclamationmark" : needsAnswer ? "pause.circle" : "circle.fill").font(.system(size: 9)) }
        }
        .frame(width: 12, height: 12)
        .foregroundStyle(reconnecting || needsAnswer ? PhrenTheme.warning : connected ? PhrenTheme.cyan : PhrenTheme.textDim)
        .accessibilityElement(children: .ignore)
        .accessibilityLabel(label).accessibilityIdentifier("chat-activity")
    }
}
