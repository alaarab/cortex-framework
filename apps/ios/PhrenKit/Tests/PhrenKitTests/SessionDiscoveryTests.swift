import XCTest
@testable import PhrenKit

final class SessionDiscoveryTests: XCTestCase {
    private let project = SessionProject(storeID: "personal/brain", name: "phren")

    func testDirectoryRecognitionHandlesSubfoldersAndWorktreesWithoutPrefixMatches() throws {
        let host = try LiveHost(name: "Mac", address: "mac.example", username: "dev")
        let preferences = try LiveSessionPreferences.read(LiveSessionPreferences.saving(host, in: Data()))
        for path in ["/work/phren", "/work/Phren/apps/ios", "/home/dev/.codex/worktrees/ab12/phren/src"] {
            let match = try XCTUnwrap(preferences.projectMatch(hostID: host.id, cwd: path, projects: [project]))
            XCTAssertEqual(match.project, project)
            XCTAssertTrue(match.automatic)
            XCTAssertFalse(match.directory.hasSuffix("/src"))
        }
        for path in ["/work/phren-other", "/work/phren/../other", "/work/./phren", "phren", "/work/other"] {
            XCTAssertNil(preferences.projectMatch(hostID: host.id, cwd: path, projects: [project]))
        }
        XCTAssertNil(preferences.projectMatch(hostID: UUID(), cwd: "/work/phren", projects: [project]))
    }

    func testDuplicateStoreNamesNeedAChoiceAndExplicitMappingWins() throws {
        let host = try LiveHost(name: "Mac", address: "mac.example", username: "dev")
        let team = SessionProject(storeID: "team/brain", name: "phren")
        var data = try LiveSessionPreferences.saving(host, in: Data())
        XCTAssertNil(try LiveSessionPreferences.read(data).projectMatch(hostID: host.id, cwd: "/work/phren/src", projects: [project, team]))
        data = try LiveSessionPreferences.assigning(hostID: host.id, directory: "/work/phren", storeID: team.storeID, project: team.name, in: data)
        let preferences = try LiveSessionPreferences.read(data)
        let match = try XCTUnwrap(preferences.projectMatch(hostID: host.id, cwd: "/work/phren/src", projects: [project, team]))
        XCTAssertEqual(match.project, team)
        XCTAssertFalse(match.automatic)
        // A removed store's explicit choice must not be reassigned to a different store.
        XCTAssertEqual(preferences.projectMatch(hostID: host.id, cwd: "/work/phren", projects: [project])?.project, team)
    }

    func testDeepestProjectDirectoryWinsButAmbiguityDoesNotFallBackToParent() throws {
        let host = try LiveHost(name: "Mac", address: "mac.example", username: "dev")
        let preferences = try LiveSessionPreferences.read(LiveSessionPreferences.saving(host, in: Data()))
        let nested = SessionProject(storeID: "personal/brain", name: "app")
        let duplicate = SessionProject(storeID: "team/brain", name: "app")
        XCTAssertEqual(preferences.projectMatch(hostID: host.id, cwd: "/work/phren/app/src", projects: [project, nested])?.project, nested)
        XCTAssertNil(preferences.projectMatch(hostID: host.id, cwd: "/work/phren/app/src", projects: [project, nested, duplicate]))
    }

    func testDestinationsRetainExactHostWorkspaceAndTab() throws {
        let a = try LiveHost(name: "A", address: "a.example", username: "dev")
        let b = try LiveHost(name: "B", address: "b.example", username: "dev")
        let snapshot = try LiveWorkspaces.read(Data(#"{"kind":"herdr","groups":[{"id":"w1","label":"Work","children":[{"id":"w1:t1","label":"one"},{"id":"w1:t2","label":"two"}]}]}"#.utf8))
        let local = snapshot.sessions(on: a), remote = snapshot.sessions(on: b)
        XCTAssertEqual(local.map(\.workspaceID), ["w1", "w1"])
        XCTAssertEqual(local.map { $0.tab.id }, ["w1:t1", "w1:t2"])
        XCTAssertNotEqual(local[0].id, remote[0].id)
        XCTAssertNotEqual(local[0].id, local[1].id)
    }
}
