import SwiftUI
import PhrenKit

@main
struct PhrenApp: App {
    @State private var model = AppModel()
    @State private var appearance = PhrenAppearance.shared
    @Environment(\.scenePhase) private var scenePhase

    init() {
        Self.applyPhrenChrome()
    }

    var body: some Scene {
        WindowGroup {
            RootView()
                .environment(model)
                .defaultAppStorage(AppModel.isUITesting ? UserDefaults(suiteName: "phren.ui-tests")! : .standard)
                .tint(PhrenTheme.navigation)
                .foregroundStyle(PhrenTheme.text)
                // All current palettes use dark system controls and keyboards.
                .preferredColorScheme(.dark)
                .onChange(of: appearance.style) { _, _ in Self.applyPhrenChrome() }
                .modifier(ExternalURLTestCapture())
                .task { await model.bootstrap() }
                .onChange(of: scenePhase) { _, phase in
                    // Live sync runs only while the app is visible; returning
                    // to the foreground triggers an immediate catch-up pull.
                    switch phase {
                    case .active: Task { await model.enterForeground() }
                    case .background, .inactive: Task { await model.enterBackground() }
                    @unknown default: break
                    }
                }
                // Widget taps (`widgetURL`/`Link` on `phren://…`) land here
                // directly — no CFBundleURLTypes registration needed, that's
                // only required for *other* apps to open the scheme via
                // `UIApplication.open`. Just select the matching tab.
                .onOpenURL { url in
                    guard url.scheme == "phren" else { return }
                    switch url.host {
                    case "review":
                        model.selectedTab = .projects
                        model.showingMemoryMaintenance = true
                    case "projects": model.selectedTab = .projects
                    case "agents": model.selectedTab = .agents
                    case "tasks": model.selectedTab = .tasks
                    default: break
                    }
                }
        }
    }

    /// Neutral chrome keeps the content and small status accents in focus.
    private static func applyPhrenChrome() {
        let background = UIColor(PhrenTheme.bg)
        let text = UIColor(PhrenTheme.text)

        let nav = UINavigationBarAppearance()
        nav.configureWithOpaqueBackground()
        nav.backgroundColor = background
        nav.shadowColor = .clear
        nav.titleTextAttributes = [.foregroundColor: text]
        nav.largeTitleTextAttributes = [.foregroundColor: text]
        UINavigationBar.appearance().standardAppearance = nav
        UINavigationBar.appearance().scrollEdgeAppearance = nav
        UINavigationBar.appearance().compactAppearance = nav

        let tab = UITabBarAppearance()
        tab.configureWithOpaqueBackground()
        tab.backgroundColor = background
        for item in [tab.stackedLayoutAppearance, tab.inlineLayoutAppearance, tab.compactInlineLayoutAppearance] {
            item.normal.iconColor = UIColor(PhrenTheme.textMuted)
            item.normal.titleTextAttributes = [.foregroundColor: UIColor(PhrenTheme.textMuted)]
            item.selected.iconColor = text
            item.selected.titleTextAttributes = [.foregroundColor: text]
        }
        UISwitch.appearance().onTintColor = UIColor(PhrenTheme.accentSolid)
        UITabBar.appearance().standardAppearance = tab
        UITabBar.appearance().scrollEdgeAppearance = tab

        // UIAppearance covers new screens; update existing bars as well so a
        // theme changes immediately without discarding navigation or drafts.
        func update(_ view: UIView) {
            if let bar = view as? UINavigationBar {
                bar.standardAppearance = nav; bar.scrollEdgeAppearance = nav; bar.compactAppearance = nav
            } else if let bar = view as? UITabBar {
                bar.standardAppearance = tab; bar.scrollEdgeAppearance = tab
            } else if let toggle = view as? UISwitch { toggle.onTintColor = UIColor(PhrenTheme.accentSolid) }
            for child in view.subviews { update(child) }
        }
        for scene in UIApplication.shared.connectedScenes.compactMap({ $0 as? UIWindowScene }) {
            for window in scene.windows { update(window) }
        }
    }
}

/// UI tests inspect the actual URL handed to iOS, rather than merely checking
/// that a button attempted to launch an unavailable app in the simulator.
private struct ExternalURLTestCapture: ViewModifier {
    #if DEBUG && targetEnvironment(simulator)
    @State private var captured = ""
    #endif
    func body(content: Content) -> some View {
        #if DEBUG && targetEnvironment(simulator)
        if AppModel.isUITesting && ProcessInfo.processInfo.arguments.contains("--capture-chat-links") {
            content
                .environment(\.openURL, OpenURLAction { url in
                    guard url.host == "example.org" else { return .systemAction }
                    captured = url.absoluteString
                    return .handled
                })
                .overlay(alignment: .top) {
                    Text(captured).font(.caption2)
                        .accessibilityIdentifier("chat-opened-url")
                        .allowsHitTesting(false)
                }
        } else { content }
        #else
        content
        #endif
    }
}

struct RootView: View {
    @Environment(AppModel.self) private var model

    var body: some View {
        switch model.phase {
        case .loading:
            ProgressView("Loading…")
                .frame(maxWidth: .infinity, maxHeight: .infinity)
                .background(PhrenTheme.bg)
        case .signedOut, .pickingRepo, .initialSync:
            OnboardingFlow()
        case .ready:
            MainTabView()
        }
    }
}

struct MainTabView: View {
    @Environment(AppModel.self) private var model

    var body: some View {
        @Bindable var model = model
        TabView(selection: $model.selectedTab) {
            ProjectsView()
                .tabItem { Label("Projects", systemImage: "square.grid.2x2") }
                .tag(AppTab.projects)
            NavigationStack { LiveSessionsView() }
                .tabItem { Label("Agents", systemImage: "waveform.path") }
                .tag(AppTab.agents)
            TasksView()
                .tabItem { Label("Tasks", systemImage: "checklist") }
                .tag(AppTab.tasks)
            SearchView()
                .tabItem { Label("Search", systemImage: "magnifyingglass") }
                .tag(AppTab.search)
            SettingsView()
                .tabItem { Label("Settings", systemImage: "gearshape") }
                .tag(AppTab.settings)
        }
        .sheet(isPresented: $model.showingMemoryMaintenance) { MemoryMaintenanceView() }
    }
}
