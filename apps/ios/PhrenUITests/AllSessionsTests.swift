import XCTest

final class AllSessionsTests: XCTestCase {
    private let mac = "A1000000-0000-0000-0000-000000000001"
    private let linux = "A1000000-0000-0000-0000-000000000002"

    @MainActor
    func testInitialOverviewRevealsTogetherAfterTheSlowerComputerResponds() {
        let app = launch(extra: ["--all-sessions-delayed"])
        let loading = app.descendants(matching: .any).matching(identifier: "agents-loading").firstMatch
        XCTAssertTrue(loading.exists)
        XCTAssertFalse(row(app, host: mac).exists, "The fast host must not appear as a partial page")
        XCTAssertTrue(row(app, host: mac).waitForExistence(timeout: 8))
        XCTAssertTrue(row(app, host: linux).exists)
        XCTAssertFalse(loading.exists)
        capture(app, "Complete overview after coordinated loading")
    }

    @MainActor
    func testOverviewOpensTheRightComputerWithCollidingWorkspaceAndTabIDs() {
        let app = launch(extra: ["--all-sessions-change"])
        let first = row(app, host: mac), second = row(app, host: linux)
        XCTAssertTrue(first.waitForExistence(timeout: 10))
        XCTAssertTrue(second.waitForExistence(timeout: 10))
        XCTAssertTrue(first.isHittable); XCTAssertTrue(second.isHittable)
        XCTAssertLessThan(first.frame.minY, second.frame.minY)
        XCTAssertLessThanOrEqual(first.frame.height, 100)
        capture(app, "Sessions across two computers")
        first.tap()
        XCTAssertTrue(app.staticTexts["chat-location"].waitForExistence(timeout: 8))
        XCTAssertTrue(app.staticTexts["chat-location"].label.contains("Test Mac · Shared project"))
        let composer = app.descendants(matching: .any).matching(identifier: "chat-composer").firstMatch
        XCTAssertTrue(composer.waitForExistence(timeout: 8))
        XCTAssertEqual(app.keyboards.count, 0)
        composer.tap(); composer.typeText("Overview Mac only")
        app.buttons["chat-send"].tap()
        XCTAssertTrue(app.staticTexts["Received in codex on w1:p1: Overview Mac only"].waitForExistence(timeout: 8))
        // The next overview poll moves this row from Working to Done. The
        // conversation must remain presented even though its row moved groups.
        Thread.sleep(forTimeInterval: 11)
        XCTAssertTrue(app.staticTexts["chat-location"].label.contains("Test Mac · Shared project"))
        app.buttons["chat-close"].tap()
        XCTAssertTrue(second.waitForExistence(timeout: 5))
        second.tap()
        XCTAssertTrue(app.staticTexts["chat-location"].waitForExistence(timeout: 8))
        XCTAssertTrue(app.staticTexts["chat-location"].label.contains("Test Linux · Shared project"))
        XCTAssertTrue(app.staticTexts["The project screen is ready. What would you like to change?"].waitForExistence(timeout: 8))
        XCTAssertFalse(app.staticTexts["Overview Mac only"].exists)
        app.buttons["chat-close"].tap()
        app.buttons["overview-detail:\(linux):herdr:default:w1:w1:t1"].tap()
        XCTAssertTrue(app.navigationBars["Session details"].waitForExistence(timeout: 5))
        XCTAssertTrue(app.staticTexts["Review Linux deployment"].exists)
        app.navigationBars.buttons["Done"].tap()
        XCUIDevice.shared.press(.home); app.activate()
        XCTAssertTrue(first.waitForExistence(timeout: 10)); XCTAssertTrue(second.waitForExistence(timeout: 10))
        XCTAssertTrue(app.buttons["all-web-servers"].exists)
    }

    @MainActor
    func testSearchAcrossComputersAndOfflineRowsDoNotDisableTheOtherComputer() {
        let app = launch(extra: ["--all-sessions-offline"])
        let first = row(app, host: mac), second = row(app, host: linux)
        XCTAssertTrue(first.waitForExistence(timeout: 10)); XCTAssertTrue(second.waitForExistence(timeout: 10))
        let search = app.searchFields.firstMatch
        if !search.isHittable { app.collectionViews.firstMatch.swipeDown() }
        search.tap(); search.typeText("Test Linux")
        XCTAssertTrue(second.waitForExistence(timeout: 5)); XCTAssertFalse(first.exists)
        let clear = search.buttons.firstMatch
        if clear.exists { clear.tap() } else { search.typeText(String(repeating: XCUIKeyboardKey.delete.rawValue, count: 10)) }
        if app.buttons["Close"].exists { app.buttons["Close"].tap() }
        else { app.buttons["Cancel"].tap() }
        app.buttons["Refresh all sessions"].tap()
        let lastSeen = app.staticTexts.matching(NSPredicate(format: "label CONTAINS[c] %@", "Last seen")).firstMatch
        XCTAssertTrue(lastSeen.waitForExistence(timeout: 15))
        XCTAssertTrue(first.isEnabled); XCTAssertFalse(second.isEnabled)
        capture(app, "One offline computer leaves the other sessions live")
        first.tap()
        XCTAssertTrue(app.staticTexts["chat-location"].waitForExistence(timeout: 8))
        XCTAssertTrue(app.staticTexts["chat-location"].label.contains("Test Mac · Shared project"))
    }

    @MainActor
    func testLargeTextKeepsComputerLabelsAndDetailsReadable() {
        let app = launch(extra: ["-UIPreferredContentSizeCategoryName", "UICTContentSizeCategoryAccessibilityXXXL"])
        let first = row(app, host: mac)
        XCTAssertTrue(first.waitForExistence(timeout: 10))
        let title = app.staticTexts["Build the iPhone overview"]
        let metadata = app.staticTexts.matching(NSPredicate(format: "label CONTAINS %@ AND label CONTAINS %@", "Test Mac ·", "Working")).firstMatch
        XCTAssertTrue(metadata.exists)
        XCTAssertLessThanOrEqual(title.frame.maxY, metadata.frame.minY)
        XCTAssertGreaterThan(first.frame.height, 90)
        XCTAssertTrue(first.frame.contains(metadata.frame))
        let details = app.buttons["overview-detail:\(mac):herdr:default:w1:w1:t1"]
        XCTAssertGreaterThanOrEqual(details.frame.height, 44)
        XCTAssertTrue(details.isHittable)
        capture(app, "All sessions with accessibility text")
        details.tap()
        XCTAssertTrue(app.navigationBars["Session details"].waitForExistence(timeout: 5))
    }

    @MainActor
    func testEmptyOverviewKeepsComputerAndWebControlsAvailable() {
        let app = launch(extra: ["--all-sessions-empty"])
        XCTAssertTrue(app.staticTexts["No sessions running on the connected computers"].waitForExistence(timeout: 10))
        XCTAssertTrue(app.buttons["all-web-servers"].isHittable)
        XCTAssertTrue(app.buttons["live-host:\(mac)"].exists)
        XCTAssertTrue(app.buttons["Add computer"].exists)
    }

    @MainActor
    private func launch(extra: [String] = []) -> XCUIApplication {
        let app = XCUIApplication()
        app.launchArguments = ["--ui-testing", "--automatic-sessions-fixture", "--all-sessions-fixture", "--native-chat-fixture"] + extra
        app.launch()
        XCTAssertTrue(app.tabBars.buttons["Agents"].waitForExistence(timeout: 15))
        app.tabBars.buttons["Agents"].tap()
        return app
    }
    @MainActor
    private func row(_ app: XCUIApplication, host: String) -> XCUIElement {
        app.buttons["overview-chat:\(host):herdr:default:w1:w1:t1"]
    }
    @MainActor
    private func capture(_ app: XCUIApplication, _ name: String) {
        let screenshot = XCTAttachment(screenshot: app.screenshot())
        screenshot.name = name; screenshot.lifetime = .keepAlways; add(screenshot)
    }
}
