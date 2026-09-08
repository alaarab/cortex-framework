import XCTest

final class AgentChatTests: XCTestCase {
    @MainActor
    func testNativeChatReadsAndRepliesToTheSelectedConversation() {
        let app = launch()
        app.buttons["live-chat:w7:w7:t9"].tap()
        XCTAssertTrue(app.navigationBars["Agent chat"].waitForExistence(timeout: 5))
        XCTAssertTrue(app.staticTexts["The project screen is ready. What would you like to change?"].waitForExistence(timeout: 5))
        let composer = app.descendants(matching: .any).matching(identifier: "chat-composer").firstMatch
        composer.tap(); composer.typeText("Use the cyan accent")
        app.buttons["chat-send"].tap()
        XCTAssertTrue(app.staticTexts["Received in codex on w7:p1: Use the cyan accent"].waitForExistence(timeout: 8))
        XCTAssertFalse(app.buttons["Open workspace link"].exists)
        capture(app, "Native agent conversation in Phren")
        app.buttons["Chat options"].tap()
        app.buttons["Project memory"].tap()
        XCTAssertTrue(app.navigationBars["phone · brain"].waitForExistence(timeout: 5))
    }

    @MainActor
    func testMultipleAgentsRequireAPaneChoiceAndReplyToThatPane() {
        let app = launch(extra: ["--chat-multiple"])
        app.buttons["live-chat:w7:w7:t9"].tap()
        XCTAssertTrue(app.buttons["chat-pane:w7:p2"].waitForExistence(timeout: 5))
        XCTAssertFalse(app.buttons["chat-send"].isEnabled)
        app.buttons["chat-pane:w7:p2"].tap()
        XCTAssertTrue(app.staticTexts["I reviewed the changes. The project navigation looks consistent."].waitForExistence(timeout: 5))
        let composer = app.descendants(matching: .any).matching(identifier: "chat-composer").firstMatch
        composer.tap(); composer.typeText("Check the toolbar")
        app.buttons["chat-send"].tap()
        XCTAssertTrue(app.staticTexts["Received in claude on w7:p2: Check the toolbar"].waitForExistence(timeout: 8))
    }

    @MainActor
    func testFailedDeliveryKeepsDraftAndDoesNotRetryOnForeground() {
        let app = launch(extra: ["--chat-send-fails"])
        app.buttons["live-chat:w7:w7:t9"].tap()
        XCTAssertTrue(app.staticTexts["The project screen is ready. What would you like to change?"].waitForExistence(timeout: 5))
        let composer = app.descendants(matching: .any).matching(identifier: "chat-composer").firstMatch
        composer.tap(); composer.typeText("Keep this draft")
        app.buttons["chat-send"].tap()
        XCTAssertTrue(app.staticTexts["chat-delivery-error"].waitForExistence(timeout: 5))
        XCTAssertEqual(composer.value as? String, "Keep this draft")
        XCUIDevice.shared.press(.home); app.activate()
        XCTAssertEqual(composer.value as? String, "Keep this draft")
        XCTAssertFalse(app.staticTexts["Received in codex on w7:p1: Keep this draft"].exists)
    }

    @MainActor
    func testProjectContextStaysInDraftUntilSentAndSurvivesReopening() {
        let app = launch()
        app.buttons["live-chat:w7:w7:t9"].tap()
        XCTAssertTrue(app.staticTexts["The project screen is ready. What would you like to change?"].waitForExistence(timeout: 5))
        app.buttons["Add project context"].tap()
        XCTAssertTrue(app.navigationBars["Project context"].waitForExistence(timeout: 5))
        app.buttons["[decision] Keep phone sessions connected to project memory"].tap()
        let composer = app.descendants(matching: .any).matching(identifier: "chat-composer").firstMatch
        XCTAssertTrue((composer.value as? String)?.contains("Finding — phone (sample/brain)") == true)
        XCTAssertFalse(app.staticTexts.matching(NSPredicate(format: "label BEGINSWITH %@", "Received in codex")).firstMatch.exists)
        app.navigationBars["Agent chat"].buttons["Done"].tap()
        app.buttons["live-chat:w7:w7:t9"].tap()
        XCTAssertTrue(composer.waitForExistence(timeout: 5))
        XCTAssertTrue((composer.value as? String)?.contains("Finding — phone (sample/brain)") == true)
    }

    @MainActor
    func testBlockedAgentRequiresTerminalAndCannotSend() {
        let app = launch(extra: ["--chat-blocked"])
        app.buttons["live-chat:w7:w7:t9"].tap()
        XCTAssertTrue(app.staticTexts["The agent needs an approval or answer in the terminal."].waitForExistence(timeout: 5))
        let composer = app.descendants(matching: .any).matching(identifier: "chat-composer").firstMatch
        composer.tap(); composer.typeText("Keep this for later")
        XCTAssertFalse(app.buttons["chat-send"].isEnabled)
    }

    @MainActor
    func testMoshiPreferenceOpensTheSameWorkspaceDirectly() {
        let app = launch(extra: ["--prefer-moshi", "--capture-moshi-links"])
        app.buttons["live-chat:w7:w7:t9"].tap()
        app.assertMoshiOpened("moshi://herdr?workspace=w7")
        XCTAssertFalse(app.navigationBars["Agent chat"].exists)
    }

    @MainActor
    func testProjectMenuOpensNativeChat() {
        let app = launch()
        app.tabBars.buttons["Projects"].tap()
        app.buttons["project:sample/brain:phone"].tap()
        app.buttons["Project session"].tap()
        app.buttons["Chat with agent"].tap()
        let found = app.buttons["discovered-session:A1000000-0000-0000-0000-000000000001:w7:w7:t9"]
        XCTAssertTrue(found.waitForExistence(timeout: 10))
        found.tap()
        XCTAssertTrue(app.navigationBars["Agent chat"].waitForExistence(timeout: 5))
        XCTAssertTrue(app.staticTexts["The project screen is ready. What would you like to change?"].waitForExistence(timeout: 5))
    }

    @MainActor
    private func launch(extra: [String] = []) -> XCUIApplication {
        let app = XCUIApplication()
        app.launchArguments = ["--ui-testing", "--automatic-sessions-fixture", "--session-details-fixture", "--native-chat-fixture"] + extra
        app.launch()
        XCTAssertTrue(app.tabBars.buttons["Agents"].waitForExistence(timeout: 15))
        app.tabBars.buttons["Agents"].tap()
        app.buttons.matching(NSPredicate(format: "label BEGINSWITH %@", "Test Mac,")).firstMatch.tap()
        XCTAssertTrue(app.buttons["live-chat:w7:w7:t9"].waitForExistence(timeout: 10))
        return app
    }
    @MainActor private func capture(_ app: XCUIApplication, _ name: String) {
        let shot = XCTAttachment(screenshot: app.screenshot()); shot.name = name; shot.lifetime = .keepAlways; add(shot)
    }
}
