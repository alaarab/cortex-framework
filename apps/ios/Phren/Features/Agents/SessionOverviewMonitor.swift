import Foundation
import Observation
import PhrenKit

/// Present the first refresh together, then keep each host independently live.
@Observable @MainActor
final class SessionOverviewMonitor {
    struct Computer: Identifiable {
        let host: LiveHost
        let monitor: LiveHostMonitor
        var id: UUID { host.id }
    }
    struct Group: Identifiable {
        let id: String
        let title: String
        let sessions: [DiscoveredMoshiSession]
        let fresh: Bool
    }

    private(set) var computers: [Computer] = []
    private(set) var ready = false
    private var generation = UUID()
    private var pending: Set<UUID> = []
    @ObservationIgnored private let initialWait: Duration
    @ObservationIgnored private let makeMonitor: @MainActor () -> LiveHostMonitor

    init(initialWait: Duration = .seconds(8), makeMonitor: @escaping @MainActor () -> LiveHostMonitor = { LiveHostMonitor() }) {
        self.initialWait = initialWait; self.makeMonitor = makeMonitor
    }

    func run(hosts: [LiveHost]) async {
        guard !Task.isCancelled else { return }
        let run = UUID(); generation = run
        computers = hosts.map { host in
            computers.first(where: { $0.host == host }) ?? Computer(host: host, monitor: makeMonitor())
        }
        pending = Set(computers.filter { $0.monitor.snapshot == nil && $0.monitor.message == nil }.map(\.id))
        ready = pending.isEmpty
        // Bound the initial reveal even when a transport cannot respond. A
        // pending computer is shown as connecting, without hiding healthy ones.
        let deadline = Task {
            do { try await Task.sleep(for: initialWait) } catch { return }
            if generation == run { ready = true }
        }
        defer { deadline.cancel() }
        await withTaskGroup(of: Void.self) { group in
            for computer in computers {
                group.addTask {
                    await computer.monitor.run(host: computer.host) { [weak self] in
                        guard let self, self.generation == run else { return }
                        self.pending.remove(computer.id)
                        if self.pending.isEmpty { self.ready = true }
                    }
                }
            }
        }
    }

    func groups(at date: Date, query: String, preferences: LiveSessionPreferences?, projects: [SessionProject]) -> [Group] {
        guard ready else { return [] }
        var live: [DiscoveredMoshiSession] = [], previous: [DiscoveredMoshiSession] = []
        for computer in computers {
            let sessions = (computer.monitor.snapshot?.sessions(on: computer.host) ?? []).filter { session in
                let project = preferences?.projectMatch(hostID: computer.id, cwd: session.tab.cwd, projects: projects)
                return session.matches(query, projectName: project?.project.name)
            }
            if computer.monitor.isFresh(at: date) { live += sessions } else { previous += sessions }
        }
        let order: [(MoshiWorkspaces.Tab.Activity, String)] = [
            (.working, "Working"), (.waiting, "Needs input"), (.error, "Needs attention"),
            (.idle, "Idle"), (.done, "Done"), (.unknown, "Other sessions"),
        ]
        var groups = order.compactMap { activity, title -> Group? in
            let matches = live.filter { $0.tab.activity == activity }.sorted(by: Self.ordered)
            return matches.isEmpty ? nil : Group(id: activity.rawValue, title: title, sessions: matches, fresh: true)
        }
        if !previous.isEmpty {
            groups.append(Group(id: "previous", title: "Last seen", sessions: previous.sorted(by: Self.ordered), fresh: false))
        }
        return groups
    }

    func connectedCount(at date: Date) -> Int { ready ? computers.filter { $0.monitor.isFresh(at: date) }.count : 0 }

    private static func ordered(_ lhs: DiscoveredMoshiSession, _ rhs: DiscoveredMoshiSession) -> Bool {
        (lhs.host.name.lowercased(), lhs.host.id.uuidString, lhs.workspaceName.lowercased(), lhs.tab.id)
            < (rhs.host.name.lowercased(), rhs.host.id.uuidString, rhs.workspaceName.lowercased(), rhs.tab.id)
    }
}
