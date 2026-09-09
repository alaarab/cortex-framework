import XCTest

final class AgentChatTests: XCTestCase {
    @MainActor
    func testHerdrWorkspaceBrowserAndNamedServerSelection() {
        let app = launch()
        app.buttons["Herdr workspaces & terminal"].tap()
        XCTAssertTrue(app.navigationBars["Herdr"].waitForExistence(timeout: 8))
        XCTAssertTrue(app.staticTexts["Phone work"].waitForExistence(timeout: 8))
        capture(app, "Herdr workspace browser")
        app.buttons.matching(NSPredicate(format: "label CONTAINS %@", "Herdr server")).firstMatch.tap()
        app.buttons["work"].tap()
        XCTAssertTrue(app.buttons.matching(NSPredicate(format: "label CONTAINS %@ AND label CONTAINS %@", "Herdr server", "work")).firstMatch.waitForExistence(timeout: 8))
        app.buttons["Open Herdr terminal"].tap()
        XCTAssertTrue(app.navigationBars["Herdr terminal"].waitForExistence(timeout: 8))
        XCTAssertTrue(app.staticTexts["Test Mac · work"].waitForExistence(timeout: 8))
    }
    @MainActor
    func testInlineApprovalAndQuestionAnswers() {
        var app = launch(extra: ["--chat-approval"])
        app.buttons["live-chat:w7:w7:t9"].tap()
        XCTAssertTrue(app.buttons["Approve"].waitForExistence(timeout: 8))
        capture(app, "Inline approval in Phren")
        app.buttons["Approve"].tap()
        XCTAssertTrue(app.staticTexts["Answer received in this conversation."].waitForExistence(timeout: 8))
        XCTAssertFalse(app.buttons["Approve"].exists)
        app.terminate()
        app = launch(extra: ["--chat-question"])
        app.buttons["live-chat:w7:w7:t9"].tap()
        XCTAssertTrue(app.buttons["Send answer"].waitForExistence(timeout: 8))
        XCTAssertFalse(app.buttons["Send answer"].isEnabled)
        app.buttons.matching(NSPredicate(format: "label CONTAINS %@", "Keep the Phren accent")).firstMatch.tap()
        capture(app, "Inline question in Phren")
        app.buttons["Send answer"].tap()
        XCTAssertTrue(app.staticTexts["Answer received in this conversation."].waitForExistence(timeout: 8))
    }

    @MainActor
    func testHistoricalImageDiffAndNativeHerdrNavigation() {
        let app = launch(extra: ["--chat-historical-image"])
        app.buttons["live-chat:w7:w7:t9"].tap()
        XCTAssertTrue(app.buttons["View conversation image"].waitForExistence(timeout: 8))
        app.buttons["View conversation image"].tap()
        XCTAssertTrue(app.navigationBars["Conversation image.jpg"].waitForExistence(timeout: 5))
        app.navigationBars["Conversation image.jpg"].buttons["Done"].tap()
        app.buttons["Chat options"].tap()
        app.buttons["Repository changes"].tap()
        XCTAssertTrue(app.staticTexts["Theme.swift"].waitForExistence(timeout: 5))
        app.staticTexts["Theme.swift"].tap()
        capture(app, "Native repository diff")
        XCTAssertTrue(app.staticTexts.matching(NSPredicate(format: "label CONTAINS %@", "+let accent = cyan")).firstMatch.exists)
        app.navigationBars.buttons.element(boundBy: 0).tap()
        app.navigationBars.buttons.element(boundBy: 0).tap()
        app.buttons["Chat options"].tap()
        app.buttons["Herdr terminal"].tap()
        XCTAssertTrue(app.navigationBars["Herdr terminal"].waitForExistence(timeout: 8))
        XCTAssertTrue(app.buttons["Toggle terminal keyboard"].waitForExistence(timeout: 8))
        capture(app, "Native Herdr terminal")
    }

    @MainActor
    func testDraftAndAttachmentSurviveProcessRelaunch() {
        var app = launch(extra: ["--chat-persistent-draft", "--chat-clear-drafts"])
        app.buttons["live-chat:w7:w7:t9"].tap()
        let composer = app.descendants(matching: .any).matching(identifier: "chat-composer").firstMatch
        XCTAssertTrue(composer.waitForExistence(timeout: 8))
        attachImage(app)
        composer.tap(); composer.typeText("Keep this across relaunch")
        app.terminate()
        app = launch(extra: ["--chat-persistent-draft"])
        app.buttons["live-chat:w7:w7:t9"].tap()
        let restored = app.descendants(matching: .any).matching(identifier: "chat-composer").firstMatch
        XCTAssertTrue(restored.waitForExistence(timeout: 8))
        XCTAssertEqual(restored.value as? String, "Keep this across relaunch")
        XCTAssertTrue(app.buttons["Preview Screenshot.png"].waitForExistence(timeout: 8))
        capture(app, "Draft restored after process relaunch")
    }
    /// Seed this simulator with `xcrun simctl addmedia <device> <test-image>`.
    @MainActor
    func testSystemPhotoPickerPreparesAnAttachment() throws {
        let app = launch()
        app.buttons["live-chat:w7:w7:t9"].tap()
        XCTAssertTrue(app.staticTexts["The project screen is ready. What would you like to change?"].waitForExistence(timeout: 5))
        app.buttons["Add attachment"].tap()
        app.buttons["Photos"].tap()
        let picker = app.scrollViews["photosView_content_scroll_view"]
        XCTAssertTrue(picker.waitForExistence(timeout: 8))
        let introduction = picker.buttons["Close"].firstMatch
        if introduction.exists { introduction.tap() }
        let photo = picker.images.firstMatch
        guard photo.waitForExistence(timeout: 8) else {
            throw XCTSkip("Seed the UI test simulator with a photo to exercise the system picker")
        }
        // Photos' remote grid exposes its image frame but not AX hit testing.
        photo.coordinate(withNormalizedOffset: CGVector(dx: 0.5, dy: 0.5)).tap()
        let done = app.navigationBars["Photos"].buttons["Done"]
        if done.waitForExistence(timeout: 3) { done.tap() }
        else { app.buttons["Add"].firstMatch.tap() }
        let preview = app.buttons.matching(NSPredicate(format: "label BEGINSWITH %@", "Preview Image.")).firstMatch
        XCTAssertTrue(preview.waitForExistence(timeout: 10))
        capture(app, "System photo picker attachment")
    }

    @MainActor
    func testImageAttachmentCanBeRemovedPreviewedAndSent() {
        let app = launch()
        app.buttons["live-chat:w7:w7:t9"].tap()
        XCTAssertTrue(app.staticTexts["The project screen is ready. What would you like to change?"].waitForExistence(timeout: 5))
        attachImage(app)
        XCTAssertTrue(app.buttons["Preview Screenshot.png"].waitForExistence(timeout: 5))
        app.buttons["Preview Screenshot.png"].tap()
        XCTAssertTrue(app.navigationBars["Screenshot.png"].waitForExistence(timeout: 5))
        app.navigationBars["Screenshot.png"].buttons["Done"].tap()
        app.buttons["Remove Screenshot.png"].tap()
        XCTAssertFalse(app.buttons["Preview Screenshot.png"].exists)
        attachImage(app)
        let composer = app.descendants(matching: .any).matching(identifier: "chat-composer").firstMatch
        composer.tap(); composer.typeText("Review this screenshot")
        capture(app, "Image and prompt ready to send")
        app.buttons["chat-send"].tap()
        XCTAssertTrue(app.buttons["View attached Screenshot.png"].waitForExistence(timeout: 8))
        capture(app, "Sent image in conversation")
        if app.buttons["Latest messages"].isHittable { app.buttons["Latest messages"].tap() }
        XCTAssertTrue(app.staticTexts.matching(NSPredicate(format: "label BEGINSWITH %@ AND label CONTAINS %@", "Received in codex", "/tmp/phren-fixture/")).firstMatch.waitForExistence(timeout: 8))
        XCTAssertFalse(app.buttons["Remove Screenshot.png"].exists)
    }

    @MainActor
    func testFailedUploadRetainsImageAndTextWithoutSending() {
        let app = launch(extra: ["--chat-upload-fails"])
        app.buttons["live-chat:w7:w7:t9"].tap()
        XCTAssertTrue(app.staticTexts["The project screen is ready. What would you like to change?"].waitForExistence(timeout: 5))
        attachImage(app)
        let composer = app.descendants(matching: .any).matching(identifier: "chat-composer").firstMatch
        composer.tap(); composer.typeText("Keep my screenshot")
        app.buttons["chat-send"].tap()
        XCTAssertTrue(app.staticTexts["chat-delivery-error"].waitForExistence(timeout: 5))
        XCTAssertTrue(app.staticTexts["chat-delivery-error"].label.contains("message hasn't been sent"))
        XCTAssertEqual(composer.value as? String, "Keep my screenshot")
        XCTAssertTrue(app.buttons["Remove Screenshot.png"].exists)
        XCTAssertFalse(app.staticTexts.matching(NSPredicate(format: "label BEGINSWITH %@", "Received in codex")).firstMatch.exists)
    }

    @MainActor
    func testEarlierHistorySurvivesLiveRefresh() {
        let app = launch(extra: ["--chat-history"])
        app.buttons["live-chat:w7:w7:t9"].tap()
        XCTAssertTrue(app.buttons["chat-history"].waitForExistence(timeout: 5))
        app.buttons["chat-history"].tap()
        XCTAssertTrue(app.staticTexts["Earlier project discussion"].waitForExistence(timeout: 5))
        XCUIDevice.shared.press(.home); app.activate()
        XCTAssertTrue(app.staticTexts["Earlier project discussion"].waitForExistence(timeout: 5))
        XCTAssertFalse(app.buttons["chat-history"].exists)
    }

    @MainActor
    func testStopAndCodeCardsWorkInsideChat() {
        let app = launch(extra: ["--chat-working", "--chat-markdown"])
        app.buttons["live-chat:w7:w7:t9"].tap()
        XCTAssertTrue(app.buttons["chat-stop"].waitForExistence(timeout: 5))
        app.buttons["Copy code"].tap()
        app.buttons["chat-stop"].tap()
        XCTAssertTrue(app.staticTexts["Turn stopped in the selected pane."].waitForExistence(timeout: 8))
        capture(app, "Native code card and stopped turn")
    }

    @MainActor private func attachImage(_ app: XCUIApplication) {
        app.buttons["Add attachment"].tap()
        XCTAssertTrue(app.buttons["Add test image"].waitForExistence(timeout: 5))
        app.buttons["Add test image"].tap()
    }

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
    func testRejectedSendKeepsDraftAndAllowsExplicitRetry() {
        let app = launch(extra: ["--chat-send-rejected", "--chat-working"])
        app.buttons["live-chat:w7:w7:t9"].tap()
        XCTAssertTrue(app.staticTexts["The project screen is ready. What would you like to change?"].waitForExistence(timeout: 5))
        let composer = app.descendants(matching: .any).matching(identifier: "chat-composer").firstMatch
        composer.tap(); composer.typeText("Keep it up")
        app.buttons["chat-send"].tap()
        let error = app.staticTexts["chat-delivery-error"]
        XCTAssertTrue(error.waitForExistence(timeout: 5))
        XCTAssertTrue(error.label.contains("The selected terminal is unavailable."))
        XCTAssertFalse(error.label.contains("update moshi-hook"))
        XCTAssertEqual(composer.value as? String, "Keep it up")
        XCTAssertTrue(app.buttons["chat-send"].isEnabled)
        XCUIDevice.shared.press(.home); app.activate()
        XCTAssertFalse(app.staticTexts["Received in codex on w7:p1: Keep it up"].exists)
        XCTAssertTrue(app.buttons["chat-send"].waitForExistence(timeout: 5))
        let ready = XCTNSPredicateExpectation(predicate: NSPredicate(format: "enabled == true"), object: app.buttons["chat-send"])
        XCTAssertEqual(XCTWaiter.wait(for: [ready], timeout: 8), .completed)
        capture(app, "Rejected message keeps an editable draft")
        app.buttons["chat-send"].tap()
        XCTAssertTrue(app.staticTexts["Received in codex on w7:p1: Keep it up"].waitForExistence(timeout: 8))
        XCTAssertFalse(error.exists)
    }

    @MainActor
    func testOfflineComposerShowsReconnectWithoutLosingDraft() {
        let app = launch(extra: ["--chat-offline"])
        app.buttons["live-chat:w7:w7:t9"].tap()
        XCTAssertTrue(app.staticTexts["The project screen is ready. What would you like to change?"].waitForExistence(timeout: 5))
        let composer = app.descendants(matching: .any).matching(identifier: "chat-composer").firstMatch
        composer.tap(); composer.typeText("Keep this while offline")
        let reconnect = app.buttons["chat-reconnect"]
        XCTAssertTrue(reconnect.waitForExistence(timeout: 8))
        XCTAssertFalse(app.buttons["chat-send"].isEnabled)
        reconnect.tap()
        XCTAssertEqual(composer.value as? String, "Keep this while offline")
        XCTAssertFalse(app.staticTexts["Received in codex on w7:p1: Keep this while offline"].exists)
        capture(app, "Reconnect is visible beside the draft")
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
        app.buttons["Chat options"].tap()
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
        XCTAssertTrue(app.buttons["chat-answer-terminal"].waitForExistence(timeout: 5))
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
