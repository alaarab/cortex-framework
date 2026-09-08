import Foundation
import Observation
import PhrenKit

/// Each host owns its refresh loop. A slow or offline computer cannot delay
/// another computer's rows, and freshness never crosses host boundaries.
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
    @ObservationIgnored private let makeMonitor: @MainActor () -> LiveHostMonitor

    init(makeMonitor: @escaping @MainActor () -> LiveHostMonitor = { LiveHostMonitor() }) { self.makeMonitor = makeMonitor }

    func run(hosts: [LiveHost]) async {
        guard !Task.isCancelled else { return }
        computers = hosts.map { host in
            computers.first(where: { $0.host == host }) ?? Computer(host: host, monitor: makeMonitor())
        }
        await withTaskGroup(of: Void.self) { group in
            for computer in computers {
                group.addTask { await computer.monitor.run(host: computer.host) }
            }
        }
    }

    func groups(at date: Date, query: String, preferences: LiveSessionPreferences?, projects: [SessionProject]) -> [Group] {
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

    func connectedCount(at date: Date) -> Int { computers.filter { $0.monitor.isFresh(at: date) }.count }

    private static func ordered(_ lhs: DiscoveredMoshiSession, _ rhs: DiscoveredMoshiSession) -> Bool {
        (lhs.host.name.lowercased(), lhs.host.id.uuidString, lhs.workspaceName.lowercased(), lhs.tab.id)
            < (rhs.host.name.lowercased(), rhs.host.id.uuidString, rhs.workspaceName.lowercased(), rhs.tab.id)
    }
}
