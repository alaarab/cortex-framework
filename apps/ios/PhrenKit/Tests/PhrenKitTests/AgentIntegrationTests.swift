import Foundation
import XCTest
@testable import PhrenKit

final class AgentIntegrationTests: XCTestCase {
    private func target(_ mux: String = "herdr:default", host: UUID = UUID()) throws -> AgentChatTarget {
        try .init(hostID: host, workspaceID: "w1", tabID: "w1:t1", paneID: "w1:p1", source: "codex", sessionID: "test-session", muxID: mux)
    }
    func testLegacyHostDefaultsToDefaultHerdrAndNamedServersIsolateDrafts() throws {
        let id = UUID()
        let old = Data("{\"id\":\"\(id)\",\"name\":\"Mac\",\"address\":\"mac.local\",\"port\":22,\"username\":\"me\"}".utf8)
        let host = try JSONDecoder().decode(LiveHost.self, from: old)
        XCTAssertEqual(host.muxID, "herdr:default")
        XCTAssertNotEqual(try target(host: id).id, try target("herdr:work", host: id).id)
        XCTAssertThrowsError(try LiveHost(name: "Mac", address: "mac", username: "me", herdrSession: "wrong:server"))
    }
    func testDraftSurvivesNewStoreWithImagesAndClearsOnlyItsConversation() throws {
        let root = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: root) }
        let attachment = try AgentAttachment(name: "photo.png", data: Data([1, 2, 3]), isImage: true)
        let store = AgentDraftStore(root: root)
        try store.save(.init(text: "Unsent words", attachments: [attachment]), target: "first")
        try store.save(.init(text: "Other computer"), target: "second")
        let reopened = AgentDraftStore(root: root)
        let draft = try reopened.load(target: "first")
        XCTAssertEqual(draft.text, "Unsent words"); XCTAssertEqual(draft.attachments, [attachment])
        try reopened.save(.init(), target: "first")
        XCTAssertEqual(try reopened.load(target: "second").text, "Other computer")
        XCTAssertEqual(try AgentDraftStore(root: root).load(target: "first").text, "")
    }
    func testCorruptOrFutureDraftCannotBeOverwrittenEvenWithoutLoadingFirst() throws {
        for future in [false, true] {
            let root = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
            defer { try? FileManager.default.removeItem(at: root) }
            try AgentDraftStore(root: root).save(.init(text: "preserve me"), target: "target")
            let folder = try XCTUnwrap(FileManager.default.contentsOfDirectory(at: root, includingPropertiesForKeys: nil).first)
            let manifest = folder.appendingPathComponent("draft.json")
            let original = Data((future ? #"{"schemaVersion":999,"target":"target","text":"future draft","files":[]}"# : "broken json").utf8)
            try original.write(to: manifest)
            XCTAssertThrowsError(try AgentDraftStore(root: root).save(.init(), target: "target"))
            XCTAssertThrowsError(try AgentDraftStore(root: root).save(.init(text: "replacement"), target: "target"))
            let preserved = try FileManager.default.contentsOfDirectory(at: folder, includingPropertiesForKeys: nil)
            XCTAssertTrue(preserved.contains { (try? Data(contentsOf: $0)) == original })
        }
    }
    func testAttachmentCorruptionPreservesManifestAndRejectsLoad() throws {
        let root = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: root) }
        let item = try AgentAttachment(name: "file.txt", data: Data("original".utf8), isImage: false)
        try AgentDraftStore(root: root).save(.init(attachments: [item]), target: "target")
        let folder = try XCTUnwrap(FileManager.default.contentsOfDirectory(at: root, includingPropertiesForKeys: nil).first)
        try Data("changed".utf8).write(to: folder.appendingPathComponent(item.id.uuidString + ".bin"))
        XCTAssertThrowsError(try AgentDraftStore(root: root).load(target: "target"))
        XCTAssertTrue(FileManager.default.fileExists(atPath: folder.appendingPathComponent("draft.json").path))
    }
    func testHandwrittenLegacyDraftDefaultsAndAttachmentQuota() throws {
        let root = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: root) }
        try AgentDraftStore(root: root).save(.init(text: "initial"), target: "target")
        let folder = try XCTUnwrap(FileManager.default.contentsOfDirectory(at: root, includingPropertiesForKeys: nil).first)
        try Data(#"{"target":"target","text":"Unsent legacy draft"}"#.utf8).write(to: folder.appendingPathComponent("draft.json"))
        let reopened = AgentDraftStore(root: root)
        let draft = try reopened.load(target: "target")
        XCTAssertEqual(draft.text, "Unsent legacy draft"); XCTAssertTrue(draft.attachments.isEmpty)
        let quota = root.appendingPathComponent("quota-fixture.bin")
        FileManager.default.createFile(atPath: quota.path, contents: Data())
        let handle = try FileHandle(forWritingTo: quota)
        try handle.truncate(atOffset: 256 * 1_024 * 1_024); try handle.close()
        let item = try AgentAttachment(name: "file.txt", data: Data("new bytes".utf8), isImage: false)
        XCTAssertThrowsError(try reopened.save(.init(text: draft.text, attachments: [item]), target: "target"))
        XCTAssertEqual(try AgentDraftStore(root: root).load(target: "target").text, draft.text)
    }
    func testOriginalImageIndexesAndQuestionResolutionArePreserved() throws {
        let raw = Data(#"{"type":"backlog","source":"claude","entries":[{"line":18,"raw":{"message":{"role":"user","content":[{"type":"thinking","thinking":"private"},{"type":"text","text":"photo"},{"type":"image","source":{"type":"base64","data":"fake"}}]}}}] }"#.utf8)
        XCTAssertEqual(try AgentChatTranscript.read(raw, source: "claude").messages.flatMap(\.imageBlocks), [2])
        let prompt = try JSONDecoder().decode(AgentQuestionPrompt.self, from: Data(#"{"toolUseId":"question-1","questions":[{"id":"color","question":"Which color?","options":[{"label":"Cyan"},{"label":"Purple"}]}]}"#.utf8))
        let t = try target()
        let body = try XCTUnwrap(JSONSerialization.jsonObject(with: prompt.answerBody(target: t, selections: [[1]])) as? [String: Any])
        XCTAssertEqual(body["toolUseId"] as? String, "question-1")
        XCTAssertEqual((body["answers"] as? [[String: Any]])?.first?["optionIndexes"] as? [Int], [1])
        XCTAssertThrowsError(try prompt.answerBody(target: t, selections: [[2]]))
        XCTAssertThrowsError(try prompt.answerBody(target: t, selections: [[0, 1]]))
        XCTAssertEqual(AgentQuestionEvent.read(["type": "response_item", "payload": ["type": "function_call_output", "call_id": "question-1"]], source: "codex"), [.resolved("question-1")])
    }
    func testApprovalMustMatchConversationAndDiffMustStayOnGateway() throws {
        let t = try target()
        let valid = Data(#"{"agentStatus":{"source":"codex","session":"test-session","pendingApproval":{"actionId":"action-1","title":"Run tests","message":"npm test"}}}"#.utf8)
        XCTAssertEqual(try AgentInteractionStatus.read(valid, target: t)?.approval?.id, "action-1")
        XCTAssertThrowsError(try AgentInteractionStatus.read(Data(String(decoding: valid, as: UTF8.self).replacingOccurrences(of: "test-session", with: "other").utf8), target: t))
        XCTAssertEqual(try AgentRepositoryDiff.statusPath(Data(#"{"git":true,"url":"/apps/diff/diff_ab12/"}"#.utf8)), "/apps/diff/diff_ab12/api/status")
        for url in ["https://example.com/", "/apps/diff/diff_ab12/../", "/apps/diff/diff_ab12/?token=x"] {
            XCTAssertThrowsError(try AgentRepositoryDiff.statusPath(JSONSerialization.data(withJSONObject: ["git": true, "url": url])))
        }
    }
}
