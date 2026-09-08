import PhrenKit
import PhrenLive
import SwiftTerm
import SwiftUI

@Observable @MainActor
private final class HerdrTerminalModel: NSObject, @preconcurrency TerminalViewDelegate {
    let terminal = TouchTerminalView(frame: .zero, font: .monospacedSystemFont(ofSize: 12, weight: .regular),
                                options: TerminalOptions(cols: 80, rows: 24, scrollback: 2_000))
    var connected = false
    var error: String?
    var control = false
    #if DEBUG && targetEnvironment(simulator)
    var fixtureReport = ""
    private var fixtureInput = ""
    #endif
    private var socket: HerdrTerminalSocket?
    private var writes: Task<Void, Never>?
    private var generation = UUID()
    override init() {
        super.init()
        terminal.terminalDelegate = self
        terminal.configureTouchInput()
        terminal.nativeBackgroundColor = UIColor(PhrenTheme.bgSunken)
        terminal.nativeForegroundColor = UIColor(PhrenTheme.text)
        terminal.caretColor = UIColor(PhrenTheme.cyan)
        terminal.selectedTextBackgroundColor = UIColor(PhrenTheme.lavender.opacity(0.30))
        terminal.selectedTextForegroundColor = UIColor(PhrenTheme.text)
        terminal.selectionHandleColor = UIColor(PhrenTheme.lavender)
        terminal.accessibilityIdentifier = "herdr-terminal"
    }
    func run(host: LiveHost, session: DiscoveredMoshiSession?, target: AgentChatTarget?, paneID: String?) async {
        let run = UUID(); generation = run
        let socket = HerdrTerminalSocket(); self.socket = socket
        connected = false; error = nil
        defer {
            if generation == run { connected = false; self.socket = nil; writes?.cancel(); terminal.resignFirstResponder() }
        }
        do {
            #if DEBUG && targetEnvironment(simulator)
            if AgentChatFixture.enabled {
                try await Task.sleep(for: .milliseconds(200))
                fixtureInput = ""
                let args = ProcessInfo.processInfo.arguments
                if args.contains("--terminal-mouse-fixture") {
                    terminal.feed(text: "\u{1B}[?1049h\u{1B}[?1002h\u{1B}[?1006h")
                    terminal.feed(text: (1...18).map { "Selectable terminal text · line \($0)" }.joined(separator: "\r\n"))
                } else if args.contains("--terminal-scrollback-fixture") {
                    terminal.feed(text: (1...100).map { "Scrollback history · line \($0)" }.joined(separator: "\r\n"))
                } else {
                    terminal.feed(text: "\u{1B}[2J\u{1B}[HPhren · Herdr\r\nFixture workspace · pane 1\r\n$ ")
                }
                terminal.feed(text: "\u{1B}[2 q") // Steady cursor keeps UI automation idle.
                connected = true
                while !Task.isCancelled {
                    let report: [String: Any] = ["input": fixtureInput,
                        "selected": terminal.selection.getSelectedText(),
                        "topRow": terminal.getTerminal().getTopVisibleRow(),
                        "copyActions": terminal.copyActions]
                    let updated = String(decoding: try JSONSerialization.data(withJSONObject: report, options: .sortedKeys), as: UTF8.self)
                    if fixtureReport != updated { fixtureReport = updated }
                    try await Task.sleep(for: .milliseconds(100))
                }
                return
            }
            #endif
            let key = try DeviceSSHKey.load(host.id)
            if let target {
                guard target.hostID == host.id, target.muxID == host.muxID else { throw PhrenKitError.validation("Reopen this terminal from the current computer.") }
                _ = try await MoshiConnection.chatPanes(host: host, privateKey: key, workspaceID: target.workspaceID, tabID: target.tabID).validate(target)
                try await MoshiConnection.herdrAction(host: host, privateKey: key, operation: .focus,
                                                     workspaceID: target.workspaceID, tabID: target.tabID, paneID: target.paneID)
            } else if let session {
                guard session.host == host else { throw PhrenKitError.validation("The Herdr server changed.") }
                let fresh = try await MoshiConnection.fetch(host: host, privateKey: key)
                guard fresh.sessions(on: host).contains(where: { $0.id == session.id }) else { throw PhrenKitError.validation("This Herdr tab has closed.") }
                if let paneID {
                    let list = try await MoshiConnection.chatPanes(host: host, privateKey: key, workspaceID: session.workspaceID, tabID: session.tab.id)
                    guard list.panes.contains(where: { $0.id == paneID }) else { throw PhrenKitError.validation("This pane has closed.") }
                }
                try await MoshiConnection.herdrAction(host: host, privateKey: key, operation: .focus, workspaceID: session.workspaceID, tabID: session.tab.id, paneID: paneID)
            }
            var resized = false
            for try await bytes in MoshiConnection.herdrTerminal(host: host, privateKey: key, socket: socket) {
                try Task.checkCancellation()
                guard generation == run else { return }
                terminal.feed(byteArray: ArraySlice(bytes)); connected = true
                try await socket.acknowledge(bytes.count)
                if !resized {
                    resized = true
                    try await socket.resize(columns: terminal.getTerminal().cols, rows: terminal.getTerminal().rows)
                }
            }
            throw LiveConnectionError.disconnected
        } catch {
            if !Task.isCancelled, generation == run { self.error = error.localizedDescription }
        }
    }
    func input(_ text: String) {
        #if DEBUG && targetEnvironment(simulator)
        if AgentChatFixture.enabled { if connected { fixtureInput += text }; return }
        #endif
        guard connected, let socket else { return }
        let previous = writes, run = generation
        writes = Task {
            await previous?.value
            guard !Task.isCancelled, connected, generation == run else { return }
            do { try await socket.input(text) }
            catch { connected = false; self.error = "Input wasn't confirmed. Reconnect before typing again." }
        }
    }
    func sizeChanged(source: TerminalView, newCols: Int, newRows: Int) {
        #if DEBUG && targetEnvironment(simulator)
        if AgentChatFixture.enabled { return }
        #endif
        guard connected, let socket else { return }
        Task { try? await socket.resize(columns: newCols, rows: newRows) }
    }
    func send(source: TerminalView, data: ArraySlice<UInt8>) {
        input(String(decoding: data, as: UTF8.self))
        Task { @MainActor [weak self] in self?.control = source.controlModifier }
    }
    func setTerminalTitle(source: TerminalView, title: String) {}
    func hostCurrentDirectoryUpdate(source: TerminalView, directory: String?) {}
    func scrolled(source: TerminalView, position: Double) {}
    func requestOpenLink(source: TerminalView, link: String, params: [String: String]) {
        if let url = URL(string: link), ["http", "https"].contains(url.scheme?.lowercased() ?? "") { UIApplication.shared.open(url) }
    }
    func clipboardCopy(source: TerminalView, content: Data) {}
    func clipboardRead(source: TerminalView) -> Data? { nil }
    func rangeChanged(source: TerminalView, startY: Int, endY: Int) {}
    func bell(source: TerminalView) {}
    func iTermContent(source: TerminalView, content: ArraySlice<UInt8>) {}
}

private struct HerdrTerminalSurface: UIViewRepresentable {
    let model: HerdrTerminalModel
    func makeUIView(context: Context) -> TerminalView { model.terminal }
    func updateUIView(_ view: TerminalView, context: Context) { view.isUserInteractionEnabled = model.connected }
}

struct HerdrTerminalView: View {
    let host: LiveHost
    var session: DiscoveredMoshiSession? = nil
    var target: AgentChatTarget? = nil
    var paneID: String? = nil
    @Environment(\.scenePhase) private var scenePhase
    @AppStorage("sessions.live.preferences.v1") private var hostData = Data()
    @State private var model = HerdrTerminalModel()
    @State private var visible = false
    @State private var directions = false
    @State private var reconnect = UUID()
    private var currentHost: LiveHost? { (try? LiveSessionPreferences.read(hostData))?.hosts.first { $0.id == host.id } }
    private var active: Bool { visible && scenePhase == .active && currentHost == host }
    var body: some View {
        VStack(spacing: 0) {
            HStack {
                Circle().fill(model.connected && active ? PhrenTheme.cyan : PhrenTheme.textDim).frame(width: 6, height: 6)
                Text("\(host.name) · \(host.herdrSession ?? "default")").lineLimit(1)
                Spacer()
                if !model.connected && model.error == nil && active { ProgressView().controlSize(.small) }
            }.font(.caption).foregroundStyle(PhrenTheme.textMuted).padding(12)
            if let error = model.error {
                Label(error, systemImage: "wifi.exclamationmark").font(.footnote).foregroundStyle(PhrenTheme.warning).padding(12)
            }
            if currentHost != host { Text("Connection settings changed. Reopen Herdr from the computer list.").font(.footnote).padding() }
            HerdrTerminalSurface(model: model).padding(.horizontal, 4)
            HStack(spacing: 0) {
                key("Esc", "\u{1B}"); key("Tab", "\t")
                Button {
                    model.terminal.controlModifier.toggle()
                    model.control = model.terminal.controlModifier
                } label: {
                    Text("Ctrl").foregroundStyle(model.control ? PhrenTheme.cyan : PhrenTheme.text)
                        .frame(maxWidth: .infinity, minHeight: 44).contentShape(Rectangle())
                        .background(model.control ? PhrenTheme.cyan.opacity(0.14) : .clear, in: RoundedRectangle(cornerRadius: 8))
                }.accessibilityValue(model.control ? "On" : "Off")
                Button { directions.toggle() } label: {
                    Image(systemName: "dpad").frame(maxWidth: .infinity, minHeight: 44).contentShape(Rectangle())
                }.accessibilityLabel("Arrow keys")
                    .popover(isPresented: $directions) {
                        VStack(spacing: 0) {
                            arrow("arrow.up", "Up", "\u{1B}[A")
                            HStack(spacing: 0) {
                                arrow("arrow.left", "Left", "\u{1B}[D")
                                arrow("arrow.down", "Down", "\u{1B}[B")
                                arrow("arrow.right", "Right", "\u{1B}[C")
                            }
                        }.padding(8).presentationCompactAdaptation(.popover)
                    }
                Button { model.terminal.paste(nil) } label: {
                    Image(systemName: "document.on.clipboard").frame(maxWidth: .infinity, minHeight: 44).contentShape(Rectangle())
                }.accessibilityLabel("Paste into terminal")
                Button {
                    if model.terminal.isFirstResponder { model.terminal.resignFirstResponder() }
                    else { model.terminal.becomeFirstResponder() }
                } label: {
                    Image(systemName: "keyboard").frame(maxWidth: .infinity, minHeight: 44).contentShape(Rectangle())
                }.accessibilityLabel("Toggle terminal keyboard")
            }
            .font(.system(size: 16, weight: .medium)).buttonStyle(.plain)
            .foregroundStyle(PhrenTheme.text).padding(.horizontal, 8).padding(.vertical, 2)
            .background(PhrenTheme.bgSunken)
            .overlay(alignment: .top) { Rectangle().fill(PhrenTheme.border).frame(height: 0.5) }
            .accessibilityIdentifier("terminal-toolbar")
            .disabled(!model.connected || !active)
        }
        #if DEBUG && targetEnvironment(simulator)
        .overlay(alignment: .topLeading) {
            if AgentChatFixture.enabled {
                Text(model.fixtureReport).font(.system(size: 1)).frame(width: 1, height: 1)
                    .accessibilityIdentifier("terminal-fixture-report")
            }
        }
        #endif
        .background(PhrenTheme.bgSunken).navigationTitle("Herdr terminal").navigationBarTitleDisplayMode(.inline)
        .toolbar(.hidden, for: .tabBar)
        .toolbar {
            ToolbarItem(placement: .primaryAction) {
                Button("Reconnect", systemImage: "arrow.clockwise") { reconnect = UUID() }.disabled(!active)
            }
        }
        .onAppear { visible = true }.onDisappear { visible = false }
        .task(id: Run(active: active, reconnect: reconnect)) {
            if active { await model.run(host: host, session: session, target: target, paneID: paneID) }
        }
    }
    private func key(_ title: String, _ sequence: String) -> some View {
        Button { model.input(sequence) } label: {
            Text(title).frame(maxWidth: .infinity, minHeight: 44).contentShape(Rectangle())
        }
    }
    private func arrow(_ symbol: String, _ title: String, _ sequence: String) -> some View {
        Button { model.input(sequence) } label: {
            Image(systemName: symbol).frame(width: 48, height: 44).contentShape(Rectangle())
        }.accessibilityLabel(title).disabled(!model.connected || !active)
    }
    private struct Run: Equatable { let active: Bool; let reconnect: UUID }
}
