import PhrenKit
import PhrenLive
import SwiftUI
import WebKit

struct WebPreviewView: View {
    let selection: WebServerSelection
    @AppStorage("sessions.live.preferences.v1") private var data = Data()
    @Environment(\.dismiss) private var dismiss
    @Environment(\.scenePhase) private var phase
    @State private var browser = WebPreviewModel()
    @State private var retry = UUID()
    @State private var editing = false
    private var host: LiveHost? { (try? LiveSessionPreferences.read(data))?.hosts.first { $0.id == selection.hostID } }
    private struct ConnectionID: Equatable { let host: LiveHost?; let active: Bool; let retry: UUID }

    var body: some View {
        ZStack {
            PhrenTheme.bg.ignoresSafeArea()
            if let webView = browser.webView { PreviewWebView(webView: webView) }
            if let message = browser.message {
                VStack(spacing: 14) {
                    Image(systemName: "network").font(.largeTitle).foregroundStyle(PhrenTheme.textMuted)
                    Text(message).font(.subheadline).multilineTextAlignment(.center)
                    Button("Reconnect") { retry = UUID() }.buttonStyle(.bordered)
                    if host != nil { Button("Connection settings", systemImage: "gearshape") { editing = true } }
                }.padding(24).frame(maxWidth: .infinity, maxHeight: .infinity).background(PhrenTheme.bg)
            } else if browser.webView == nil {
                ProgressView("Opening app…")
            }
        }
        .navigationBarTitleDisplayMode(.inline)
        .toolbar {
            ToolbarItem(placement: .topBarLeading) { Button("Done") { dismiss() } }
            ToolbarItem(placement: .principal) {
                VStack(spacing: 2) {
                    Text(selection.server.displayName).font(.subheadline.weight(.semibold)).lineLimit(1)
                    Text("\(host?.name ?? "Computer removed") · \(String(selection.server.port))")
                        .font(.caption2).foregroundStyle(PhrenTheme.textMuted).lineLimit(1)
                }
            }
            ToolbarItem(placement: .topBarTrailing) {
                Button("Reload page", systemImage: "arrow.clockwise") {
                    if browser.message != nil { retry = UUID() } else { browser.webView?.reload() }
                }
            }
            ToolbarItemGroup(placement: .bottomBar) {
                Button("Previous page", systemImage: "chevron.left") { browser.webView?.goBack() }.disabled(!browser.canGoBack)
                Button("Next page", systemImage: "chevron.right") { browser.webView?.goForward() }.disabled(!browser.canGoForward)
                Spacer()
                if browser.loading { ProgressView().controlSize(.small) }
            }
        }
        .task(id: ConnectionID(host: host, active: phase != .background, retry: retry)) {
            guard phase != .background else { browser.pause(); return }
            guard let host else { browser.message = "This computer was removed."; browser.stop(); return }
            await browser.connect(host: host, server: selection.server)
        }
        .onDisappear { browser.stop() }
        .sheet(isPresented: $editing) {
            if let host { NavigationStack { LiveHostEditor(existing: host) } }
        }
    }
}

@Observable @MainActor
private final class WebPreviewModel: NSObject, WKNavigationDelegate, WKUIDelegate {
    var webView: WKWebView?
    var message: String?
    var loading = false
    var canGoBack = false
    var canGoForward = false
    private var tunnel: WebPreviewTunnel?
    private var generation = UUID()
    private var baseURL: URL?

    func connect(host: LiveHost, server: WebServer) async {
        pause()
        let run = UUID(); generation = run
        message = nil; loading = true
        do {
            let url: URL
            #if DEBUG && targetEnvironment(simulator)
            if AppModel.isUITesting && ProcessInfo.processInfo.arguments.contains("--web-servers-fixture") {
                url = URL(string: "http://127.0.0.1:\(server.port)/")!
            } else {
                url = try await open(host: host, server: server)
            }
            #else
            url = try await open(host: host, server: server)
            #endif
            try Task.checkCancellation()
            guard generation == run else { return }
            if let view = webView, let previous = view.url {
                if baseURL == url { view.reload() }
                else {
                    var resume = URLComponents(url: previous, resolvingAgainstBaseURL: false)!
                    if resume.host == baseURL?.host, resume.port == baseURL?.port { resume.port = url.port }
                    view.load(URLRequest(url: resume.url ?? url))
                }
            } else {
                let config = WKWebViewConfiguration()
                // Keep cookies and storage isolated to this preview, but retain
                // them and the current page across background/reconnect cycles.
                config.websiteDataStore = .nonPersistent()
                let view = WKWebView(frame: .zero, configuration: config)
                view.navigationDelegate = self; view.uiDelegate = self
                view.allowsBackForwardNavigationGestures = true
                view.accessibilityIdentifier = "web-app-preview"
                webView = view
                view.load(URLRequest(url: url))
            }
            baseURL = url
            if let tunnel {
                await withTaskCancellationHandler { await tunnel.waitUntilClosed() } onCancel: { tunnel.close() }
                guard !Task.isCancelled, generation == run else { return }
                message = "The connection closed. Reconnect to keep browsing."
            }
        } catch {
            guard !Task.isCancelled, generation == run else { return }
            message = error.localizedDescription; loading = false
        }
    }

    private func open(host: LiveHost, server: WebServer) async throws -> URL {
        let key = try DeviceSSHKey.load(host.id)
        let current = try await MoshiConnection.webServers(host: host, privateKey: key)
        guard let live = current.first(where: { $0.id == server.id }) else {
            throw PhrenKitError.validation("This web server is no longer running. Refresh the list to find its new port.")
        }
        let connection = try await WebPreviewTunnel.open(host: host, privateKey: key, server: live)
        try Task.checkCancellation()
        tunnel = connection
        return connection.url
    }

    func pause() {
        generation = UUID()
        tunnel?.close(); tunnel = nil
        webView?.stopLoading()
    }

    func stop() {
        pause()
        webView = nil; baseURL = nil
        canGoBack = false; canGoForward = false
    }

    func webView(_ webView: WKWebView, didStartProvisionalNavigation navigation: WKNavigation!) { loading = true; message = nil }
    func webView(_ webView: WKWebView, didFinish navigation: WKNavigation!) {
        loading = false; canGoBack = webView.canGoBack; canGoForward = webView.canGoForward
    }
    func webView(_ webView: WKWebView, didFailProvisionalNavigation navigation: WKNavigation!, withError error: Error) { failed(error) }
    func webView(_ webView: WKWebView, didFail navigation: WKNavigation!, withError error: Error) { failed(error) }
    private func failed(_ error: Error) {
        guard (error as NSError).code != NSURLErrorCancelled else { return }
        message = error.localizedDescription; loading = false
    }
    func webViewWebContentProcessDidTerminate(_ webView: WKWebView) { message = "The page closed. Reconnect to load it again." }
    func webView(_ webView: WKWebView, decidePolicyFor navigationAction: WKNavigationAction,
                 decisionHandler: @escaping (WKNavigationActionPolicy) -> Void) {
        let scheme = navigationAction.request.url?.scheme?.lowercased()
        decisionHandler(["http", "https", "about"].contains(scheme ?? "") ? .allow : .cancel)
    }
    func webView(_ webView: WKWebView, createWebViewWith configuration: WKWebViewConfiguration,
                 for navigationAction: WKNavigationAction, windowFeatures: WKWindowFeatures) -> WKWebView? {
        if navigationAction.targetFrame == nil, let scheme = navigationAction.request.url?.scheme,
           ["http", "https"].contains(scheme) { webView.load(navigationAction.request) }
        return nil
    }
}

private struct PreviewWebView: UIViewRepresentable {
    let webView: WKWebView
    func makeUIView(context: Context) -> WKWebView { webView }
    func updateUIView(_ uiView: WKWebView, context: Context) {}
    static func dismantleUIView(_ uiView: WKWebView, coordinator: ()) { uiView.stopLoading() }
}
