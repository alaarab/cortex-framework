import XCTest
import PhrenKit
import PhrenLive
@testable import Phren

@MainActor
final class SessionOverviewTests: XCTestCase {
    func testFastComputerAppearsWithoutWaitingForAnOfflineComputer() async throws {
        let fast = try host("Fast"), slow = try host("Slow")
        let snapshot = try snapshot("working")
        let model = SessionOverviewMonitor {
            LiveHostMonitor { host, _ in
                if host.id == slow.id { try await Task.sleep(for: .seconds(30)) }
                return snapshot
            }
        }
        let run = Task { await model.run(hosts: [slow, fast]) }
        await eventually { model.computers.first { $0.id == fast.id }?.monitor.snapshot != nil }
        XCTAssertEqual(model.connectedCount(at: .now), 1)
        XCTAssertEqual(groups(model).flatMap(\.sessions).map(\.host.id), [fast.id])
        run.cancel(); await run.value
        XCTAssertEqual(model.connectedCount(at: .now), 0)
        XCTAssertEqual(groups(model).map(\.title), ["Last seen"])
    }

    func testSameSessionIDsOnTwoComputersStayDistinctAndSearchFindsHostAndProject() async throws {
        let first = try host("Mac"), second = try host("Linux")
        let working = try snapshot("working"), waiting = try snapshot("waiting")
        let model = SessionOverviewMonitor { LiveHostMonitor { host, _ in host.id == first.id ? working : waiting } }
        let run = Task { await model.run(hosts: [second, first]) }
        await eventually { model.connectedCount(at: .now) == 2 }
        let result = groups(model)
        XCTAssertEqual(result.map(\.title), ["Working", "Needs input"])
        XCTAssertEqual(Set(result.flatMap(\.sessions).map(\.id)).count, 2)
        XCTAssertEqual(groups(model, query: "Linux Project").flatMap(\.sessions).map(\.host.id), [second.id])
        run.cancel(); await run.value
    }

    func testFailureRetainsOnlyThatComputersStaleRowsAndClosedSessionsDisappear() async throws {
        let first = try host("Mac"), second = try host("Linux")
        let working = try snapshot("working"), empty = try MoshiWorkspaces.read(Data(#"{"kind":"herdr","groups":[]}"#.utf8))
        var failFirst = false, closeSecond = false
        let model = SessionOverviewMonitor {
            LiveHostMonitor(pollInterval: .milliseconds(20)) { host, _ in
                if host.id == first.id && failFirst { throw LiveConnectionError.disconnected }
                return host.id == second.id && closeSecond ? empty : working
            }
        }
        let run = Task { await model.run(hosts: [first, second]) }
        await eventually { model.connectedCount(at: .now) == 2 }
        failFirst = true
        await eventually { model.computers[0].monitor.message != nil }
        XCTAssertEqual(groups(model).map(\.title), ["Working", "Last seen"])
        XCTAssertEqual(groups(model).last?.sessions.first?.host.id, first.id)
        XCTAssertEqual(model.connectedCount(at: .now), 1)
        closeSecond = true
        await eventually { self.groups(model).flatMap(\.sessions).count == 1 }
        XCTAssertEqual(groups(model).map(\.title), ["Last seen"])
        failFirst = false
        await eventually { model.computers[0].monitor.message == nil }
        XCTAssertEqual(groups(model).map(\.title), ["Working"])
        run.cancel(); await run.value
    }

    func testRemovingAndReconfiguringAComputerDropsItsPreviousDestination() async throws {
        let first = try host("Mac"), second = try host("Linux")
        let working = try snapshot("working")
        let model = SessionOverviewMonitor { LiveHostMonitor { _, _ in working } }
        let run = Task { await model.run(hosts: [first, second]) }
        await eventually { model.connectedCount(at: .now) == 2 }
        let previous = model.computers[0].monitor
        run.cancel(); await run.value
        var changed = first; changed.herdrSession = "another-server"
        let next = Task { await model.run(hosts: [changed]) }
        await eventually { model.computers.count == 1 && model.connectedCount(at: .now) == 1 }
        XCTAssertFalse(model.computers[0].monitor === previous)
        XCTAssertEqual(groups(model).flatMap(\.sessions).map(\.id.muxID), ["herdr:another-server"])
        next.cancel(); await next.value
    }

    private func groups(_ model: SessionOverviewMonitor, query: String = "") -> [SessionOverviewMonitor.Group] {
        model.groups(at: .now, query: query, preferences: nil, projects: [])
    }
    private func host(_ name: String) throws -> LiveHost { try LiveHost(name: name, address: name.lowercased() + ".invalid", username: "fixture") }
    private func snapshot(_ status: String) throws -> MoshiWorkspaces {
        try MoshiWorkspaces.read(Data("""
        {"kind":"herdr","groups":[{"id":"w1","label":"Project","children":[{"id":"w1:t1","label":"Build","agent":"codex","agentStatus":"\(status)"}]}]}
        """.utf8))
    }
    private func eventually(_ condition: () -> Bool, file: StaticString = #filePath, line: UInt = #line) async {
        let deadline = Date().addingTimeInterval(2)
        while !condition() && Date() < deadline { try? await Task.sleep(for: .milliseconds(10)) }
        XCTAssertTrue(condition(), file: file, line: line)
    }
}
