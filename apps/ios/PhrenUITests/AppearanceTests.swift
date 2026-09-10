import XCTest

final class AppearanceTests: XCTestCase {
    @MainActor
    func testThemesApplyAndPersistWithoutLosingChatDraft() {
        let app = XCUIApplication()
        app.launchArguments = ["--ui-testing", "--automatic-sessions-fixture", "--session-details-fixture", "--native-chat-fixture", "--chat-design"]
        app.launch()
        XCTAssertTrue(app.tabBars.buttons["Agents"].waitForExistence(timeout: 15))
        app.tabBars.buttons["Agents"].tap()
        let session = app.buttons["overview-chat:A1000000-0000-0000-0000-000000000001:herdr:default:w7:w7:t9"]
        XCTAssertTrue(session.waitForExistence(timeout: 8)); session.tap()
        let composer = app.descendants(matching: .any).matching(identifier: "chat-composer").firstMatch
        XCTAssertTrue(composer.waitForExistence(timeout: 8))
        composer.tap(); composer.typeText("Keep my draft across themes")
        app.buttons["chat-close"].tap()
        openThemes(app)
        for style in ["graphite", "slate", "amethyst", "midnight"] {
            let choice = app.buttons["theme-\(style)"]
            for _ in 0..<4 where !choice.isHittable { app.scrollViews.firstMatch.swipeUp() }
            if !choice.isHittable { app.scrollViews.firstMatch.swipeDown(velocity: .fast) }
            XCTAssertTrue(choice.isHittable)
            choice.tap()
            XCTAssertEqual(choice.value as? String, "Selected")
            let capture = XCTAttachment(screenshot: app.screenshot())
            capture.name = "Theme \(style)"; capture.lifetime = .keepAlways; add(capture)
        }
        app.tabBars.buttons["Agents"].tap()
        session.tap()
        XCTAssertTrue(composer.waitForExistence(timeout: 8))
        XCTAssertEqual(composer.value as? String, "Keep my draft across themes")
        let chat = XCTAttachment(screenshot: app.screenshot())
        chat.name = "Midnight chat"; chat.lifetime = .keepAlways; add(chat)
        app.terminate(); app.launch()
        XCTAssertTrue(app.tabBars.buttons["Settings"].waitForExistence(timeout: 15))
        openThemes(app)
        XCTAssertEqual(app.buttons["theme-midnight"].value as? String, "Selected")
    }

    @MainActor private func openThemes(_ app: XCUIApplication) {
        app.tabBars.buttons["Settings"].tap()
        let row = app.buttons["settings-theme"]
        XCTAssertTrue(row.waitForExistence(timeout: 5)); row.tap()
        XCTAssertTrue(app.buttons["theme-midnight"].waitForExistence(timeout: 5))
    }
}
