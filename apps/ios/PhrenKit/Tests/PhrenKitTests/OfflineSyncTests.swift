import XCTest
@testable import PhrenKit

final class OfflineSyncTests: XCTestCase {
    func testFailedWriteWaitsForNextPollAndRetriesAnUnchangedHead() async throws {
        try await verifyRecovery(notModified: false)
    }

    func testFailedWriteRetriesAfterNotModifiedResponse() async throws {
        try await verifyRecovery(notModified: true)
    }

    private func verifyRecovery(notModified: Bool) async throws {
        let root = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: root) }
        let store = try LocalStore(rootDirectory: root, owner: "o", repo: "r", branch: "main")
        let client = FakeGitHubClient(remote: ["demo/tasks.md": "# Tasks\n\n## Queue\n"])
        let engine = SyncEngine(client: client, store: store, stateDirectory: root)
        await engine.pull()
        await client.setWritesOffline(true)
        try await engine.enqueue(.addTask(project: "demo", text: "Saved in the elevator"))
        // Leave automatic flushing enabled: an offline request must settle,
        // rather than continuously rescheduling itself between sync polls.
        try await Task.sleep(nanoseconds: 150_000_000)
        let attempts = await client.writes.count
        XCTAssertEqual(attempts, 1)
        let queued = await engine.pendingOps()
        XCTAssertEqual(queued.count, 1)

        await client.setWritesOffline(false)
        await client.setNotModified(notModified)
        await engine.pull()
        for _ in 0..<100 {
            if await engine.pendingOps().isEmpty { break }
            try await Task.sleep(nanoseconds: 10_000_000)
        }
        let remaining = await engine.pendingOps()
        let remote = await client.remoteContent("demo/tasks.md")
        XCTAssertTrue(remaining.isEmpty)
        XCTAssertTrue(remote?.contains("Saved in the elevator") == true)
        await engine.setAutoFlush(false)
        await engine.flushNow()
    }
}
