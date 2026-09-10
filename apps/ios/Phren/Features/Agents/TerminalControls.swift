import PhrenKit
import SwiftUI
import UIKit

struct TerminalControls: View {
    let terminal: TouchTerminalView
    let hostID: UUID
    let source: String
    let enabled: Bool
    @Binding var control: Bool
    @Binding var shortcuts: Bool
    let send: (String) -> Void
    let attach: ([AgentAttachment]) -> Void
    @State private var attachmentSource: ChatAttachmentSource?
    @State private var pendingAttachments: [AgentAttachment] = []
    @State private var directions = false
    @State private var workspaces = false
    @State private var servers = false

    var body: some View {
        HStack(spacing: 2) {
            TerminalControlKey(selected: control, tap: {
                terminal.controlModifier.toggle(); control = terminal.controlModifier
            }, hold: { shortcuts = true })
                .frame(maxWidth: .infinity).frame(height: 44)
            key("Esc", "\u{1B}")
            key("Tab", "\t")
            icon("dpad", "Arrow keys") { directions.toggle() }
                .popover(isPresented: $directions) {
                    VStack(spacing: 5) {
                        HStack(spacing: 5) {
                            arrow("delete.left", "Backspace", "\u{7F}")
                            arrow("chevron.up", "Up", "\u{1B}[A")
                            arrow("eraser", "Clear line", "\u{05}\u{15}")
                        }
                        HStack(spacing: 5) {
                            arrow("chevron.left", "Left", "\u{1B}[D")
                            arrow("return", "Enter", "\r")
                            arrow("chevron.right", "Right", "\u{1B}[C")
                        }
                        arrow("chevron.down", "Down", "\u{1B}[B")
                    }.padding(10).buttonStyle(.plain)
                        .presentationBackground(PhrenTheme.chatPanel)
                        .presentationCompactAdaptation(.popover)
                }
            icon("command", "Terminal shortcuts") { shortcuts.toggle() }
            icon("document.on.clipboard", "Paste into terminal") { terminal.paste(nil) }
            icon("keyboard", "Toggle terminal keyboard") { terminal.toggleKeyboard() }
        }
        .font(.system(size: 15, weight: .medium, design: .monospaced))
        .buttonStyle(.plain).foregroundStyle(PhrenTheme.text)
        .padding(.horizontal, 5).padding(.vertical, 2)
        .background(PhrenTheme.chatPanel, in: Capsule())
        .overlay { Capsule().strokeBorder(PhrenTheme.borderStrong, lineWidth: 0.5) }
        .contentShape(Capsule())
        .dismissKeyboardOnDownwardDrag { _ = terminal.resignFirstResponder() }
        .padding(.horizontal, 6)
        .accessibilityElement(children: .contain)
        .accessibilityIdentifier("terminal-toolbar")
        .disabled(!enabled)
        .popover(isPresented: $shortcuts) {
            TerminalShortcutMenu(source: source, enabled: enabled, send: send, close: { shortcuts = false },
                                 openWorkspaces: { shortcuts = false; workspaces = true },
                                 openServers: { shortcuts = false; servers = true },
                                 upload: { source in shortcuts = false; attachmentSource = source })
                .presentationBackground(PhrenTheme.chatPanel)
                .presentationCompactAdaptation(.popover)
        }
        .sheet(item: $attachmentSource, onDismiss: {
            if !pendingAttachments.isEmpty { let items = pendingAttachments; pendingAttachments = []; attach(items) }
        }) { source in
            ChatAttachmentPicker(initialSource: source, canAdd: pendingAttachments.count < 4, add: {
                if pendingAttachments.count < 4 { pendingAttachments.append($0) }
            }, context: nil)
        }
        .navigationDestination(isPresented: $workspaces) { HerdrWorkspacesView(hostID: hostID) }
        .navigationDestination(isPresented: $servers) { WebServersView(hostID: hostID) }
    }

    private func key(_ title: String, _ sequence: String) -> some View {
        Button { send(sequence) } label: {
            Text(title).frame(maxWidth: .infinity, minHeight: 44).contentShape(Rectangle())
        }
    }
    private func icon(_ symbol: String, _ title: String, action: @escaping () -> Void) -> some View {
        Button(action: action) {
            Image(systemName: symbol).frame(maxWidth: .infinity, minHeight: 44).contentShape(Rectangle())
        }.accessibilityLabel(title)
    }
    private func arrow(_ symbol: String, _ title: String, _ sequence: String) -> some View {
        Button { send(sequence) } label: {
            Image(systemName: symbol).font(.system(size: 20, weight: .medium))
                .foregroundStyle(title == "Enter" ? PhrenTheme.cyan : PhrenTheme.text)
                .frame(width: 56, height: 48)
                .background(PhrenTheme.surface.opacity(0.45), in: RoundedRectangle(cornerRadius: 15))
                .contentShape(Rectangle())
        }.accessibilityLabel(title)
    }
}

/// A cancelled UIKit touch cannot also trigger the Ctrl tap after a hold.
private struct TerminalControlKey: UIViewRepresentable {
    let selected: Bool
    let tap: () -> Void
    let hold: () -> Void
    func makeCoordinator() -> Coordinator { Coordinator(self) }
    func makeUIView(context: Context) -> UIButton {
        let button = UIButton(type: .custom)
        button.setTitle("Ctrl", for: .normal)
        button.titleLabel?.font = .monospacedSystemFont(ofSize: 15, weight: .medium)
        button.layer.cornerRadius = 18
        button.addTarget(context.coordinator, action: #selector(Coordinator.tap), for: .touchUpInside)
        let hold = UILongPressGestureRecognizer(target: context.coordinator, action: #selector(Coordinator.hold(_:)))
        hold.minimumPressDuration = 0.4
        hold.cancelsTouchesInView = true
        button.addGestureRecognizer(hold)
        button.accessibilityLabel = "Ctrl"
        button.accessibilityHint = "Tap for Control. Hold for shortcuts."
        button.accessibilityCustomActions = [UIAccessibilityCustomAction(name: "Open shortcuts", target: context.coordinator, selector: #selector(Coordinator.accessibleHold))]
        return button
    }
    func updateUIView(_ button: UIButton, context: Context) {
        context.coordinator.parent = self
        button.isEnabled = context.environment.isEnabled
        button.isUserInteractionEnabled = context.environment.isEnabled
        button.setTitleColor(UIColor(selected ? PhrenTheme.cyan : PhrenTheme.text), for: .normal)
        button.backgroundColor = selected ? UIColor(PhrenTheme.cyan.opacity(0.14)) : .clear
        button.accessibilityValue = selected ? "On" : "Off"
    }
    final class Coordinator: NSObject {
        var parent: TerminalControlKey
        init(_ parent: TerminalControlKey) { self.parent = parent }
        @objc func tap() { parent.tap() }
        @objc func hold(_ gesture: UILongPressGestureRecognizer) {
            if gesture.state == .began { UISelectionFeedbackGenerator().selectionChanged(); parent.hold() }
        }
        @objc func accessibleHold() -> Bool { parent.hold(); return true }
    }
}

private struct TerminalShortcutMenu: View {
    let source: String
    let enabled: Bool
    let send: (String) -> Void
    let close: () -> Void
    let openWorkspaces: () -> Void
    let openServers: () -> Void
    let upload: (ChatAttachmentSource) -> Void
    @State private var tab = ""
    @State private var settings = false
    @ScaledMetric(relativeTo: .caption) private var tileWidth = 75.0
    @AppStorage("terminal.favorites.v1") private var favorites = "codex:/model,claude:/compact,copilot:/help"
    private let tabs = ["Favorites", "Uploads", "Codex", "Claude", "Copilot", "Herdr", "Keys"]
    private var selected: String { tab.isEmpty ? (tabs.first { $0.lowercased() == source } ?? "Keys") : tab }

    var body: some View {
        VStack(spacing: 10) {
            header
            if settings {
                TerminalGestureSettings()
            } else {
                ScrollView { shortcutContent }.frame(maxHeight: 220)
                if !["Keys", "Herdr", "Uploads"].contains(selected) {
                    Text("Insert a command, then use Enter when ready.").font(.caption2).foregroundStyle(PhrenTheme.textMuted)
                }
            }
        }.padding(10).frame(idealWidth: 370, maxWidth: 400)
            .buttonStyle(.plain).foregroundStyle(PhrenTheme.text)
            .accessibilityElement(children: .contain).accessibilityIdentifier("terminal-shortcut-menu")
    }

    private var header: some View {
        HStack(spacing: 0) {
            ScrollView(.horizontal) {
                HStack(spacing: 4) {
                    ForEach(tabs, id: \.self) { name in tabButton(name) }
                }
            }.scrollIndicators(.hidden)
            Button { settings.toggle() } label: { Image(systemName: "slider.horizontal.3").frame(width: 44, height: 44) }
                .accessibilityLabel("Terminal gestures")
            Button(action: close) { Image(systemName: "xmark").frame(width: 44, height: 44) }
                .accessibilityLabel("Close shortcuts")
        }
    }

    private func tabButton(_ name: String) -> some View {
        Button { tab = name; settings = false } label: {
            Group {
                if name == "Favorites" { Image(systemName: "star") }
                else if name == "Uploads" { Image(systemName: "square.and.arrow.up") }
                else { Text(name) }
            }
                .font(.caption.weight(.semibold)).padding(.horizontal, 11).frame(height: 44)
                .foregroundStyle(selected == name ? PhrenTheme.lavender : PhrenTheme.text)
                .background(selected == name ? PhrenTheme.lavender.opacity(0.14) : .clear, in: Capsule())
        }.accessibilityLabel(name + " shortcuts")
            .accessibilityAddTraits(selected == name ? .isSelected : [])
    }

    @ViewBuilder private var shortcutContent: some View {
        if selected == "Uploads" {
            HStack(spacing: 8) {
                uploadButton("Photos", "photo.on.rectangle", .photos)
                if UIImagePickerController.isSourceTypeAvailable(.camera) { uploadButton("Camera", "camera", .camera) }
                uploadButton("Files", "doc", .files)
            }
            Text("Attach to the agent in Phren chat.").font(.caption2).foregroundStyle(PhrenTheme.textMuted)
        } else if selected == "Herdr" {
            VStack(spacing: 8) {
                Button(action: openWorkspaces) {
                    shortcutLabel("Workspaces & panes", "Switch tabs, focus panes, and manage workspaces", "rectangle.split.3x1")
                }
                Button(action: openServers) {
                    shortcutLabel("Web servers", "Open a running app in the browser", "globe")
                }
            }
        } else if selected == "Keys" {
            editingKeys
        } else {
            commandGrid
        }
    }

    private func uploadButton(_ title: String, _ symbol: String, _ source: ChatAttachmentSource) -> some View {
        Button { upload(source) } label: {
            VStack(spacing: 7) {
                Image(systemName: symbol).font(.title3)
                Text(title).font(.caption)
            }
            .frame(maxWidth: .infinity, minHeight: 70)
            .background(PhrenTheme.surface.opacity(0.5), in: RoundedRectangle(cornerRadius: 14))
        }.accessibilityLabel("Attach from " + title)
    }

    private var editingKeys: some View {
        LazyVGrid(columns: [GridItem(.adaptive(minimum: 100))], spacing: 8) {
            keyTile("Clear line", "End · Ctrl U", "\u{05}\u{15}")
            keyTile("Backspace", "Delete character", "\u{7F}")
            keyTile("Enter", "Submit", "\r")
            keyTile("⇧ Tab", "Previous field", "\u{1B}[Z")
            keyTile("Home", "Start of line", "\u{01}")
            keyTile("End", "End of line", "\u{05}")
        }
    }

    private var commands: [(provider: String, command: AgentSlashCommand.Command)] {
        let providers = selected == "Favorites" ? ["codex", "claude", "copilot"] : [selected.lowercased()]
        let saved = Set(favorites.split(separator: ",").map(String.init))
        return providers.flatMap { provider in
            AgentSlashCommand.menu(source: provider).compactMap { command in
                guard selected != "Favorites" || saved.contains(provider + ":" + command.name) else { return nil }
                return (provider: provider, command: command)
            }
        }
    }

    @ViewBuilder private var commandGrid: some View {
        let entries = commands
        if entries.isEmpty { Text("Hold a command to add it to Favorites.").font(.caption).padding() }
        LazyVGrid(columns: [GridItem(.adaptive(minimum: tileWidth))], spacing: 8) {
            ForEach(Array(entries.enumerated()), id: \.offset) { entry in
                commandTile(provider: entry.element.provider, command: entry.element.command)
            }
        }
    }

    private func commandTile(provider: String, command: AgentSlashCommand.Command) -> some View {
        Button { send(command.name + " ") } label: {
            VStack(alignment: .leading, spacing: 5) {
                Text(command.name).font(.system(.caption, design: .monospaced)).lineLimit(1)
                Text(selected == "Favorites" ? provider.capitalized : hint(command.name))
                    .font(.caption2).foregroundStyle(PhrenTheme.textMuted).lineLimit(1)
            }.frame(maxWidth: .infinity, minHeight: 44, alignment: .leading).padding(8)
                .background(PhrenTheme.surface.opacity(0.45), in: RoundedRectangle(cornerRadius: 16))
        }.accessibilityIdentifier("terminal-command:\(provider):\(command.name)")
            .accessibilityLabel(command.name + ", " + command.detail + ", " + provider.capitalized)
            .disabled(!enabled)
            .contextMenu {
                let id = provider + ":" + command.name
                let saved = favorites.split(separator: ",").map(String.init)
                Button(saved.contains(id) ? "Remove from Favorites" : "Add to Favorites", systemImage: "star") {
                    favorites = (saved.contains(id) ? saved.filter { $0 != id } : saved + [id]).joined(separator: ",")
                }
            }
    }
    private func shortcutLabel(_ title: String, _ detail: String, _ icon: String) -> some View {
        HStack {
            Image(systemName: icon).foregroundStyle(PhrenTheme.cyan)
            VStack(alignment: .leading, spacing: 4) {
                Text(title).font(.subheadline.weight(.medium))
                Text(detail).font(.caption).foregroundStyle(PhrenTheme.textMuted)
            }
            Spacer(); Image(systemName: "chevron.right").font(.caption)
        }.padding(12).background(PhrenTheme.surface.opacity(0.45), in: RoundedRectangle(cornerRadius: 16))
    }
    private func keyTile(_ title: String, _ detail: String, _ sequence: String) -> some View {
        Button { send(sequence) } label: { shortcutLabel(title, detail, "keyboard") }
            .disabled(!enabled)
    }
    private func hint(_ command: String) -> String {
        ["/model": "switch", "/permissions": "access", "/diff": "changes", "/review": "inspect",
         "/status": "session", "/skills": "browse", "/compact": "context", "/resume": "continue",
         "/new": "fresh", "/clear": "fresh", "/mcp": "tools", "/help": "commands",
         "/agent": "switch", "/context": "inspect", "/usage": "stats"][command] ?? "open"
    }
}

private struct TerminalGestureSettings: View {
    @AppStorage("terminal.twoFingerGestures.v1") private var enabled = true
    var body: some View {
        VStack(alignment: .leading, spacing: 12) {
            Toggle("Two-finger gestures", isOn: $enabled).font(.subheadline).tint(PhrenTheme.cyan)
            Text("Swipe up with two fingers for shortcuts. Swipe down with two fingers to hide the keyboard.")
            Text("Swipe with one finger to scroll. Pinch to resize. Hold to select text. Tap controls and links to open them.")
                .foregroundStyle(PhrenTheme.textMuted)
        }.font(.caption).padding(8).fixedSize(horizontal: false, vertical: true)
    }
}
