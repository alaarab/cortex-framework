import XCTest

final class AutomaticSessionTests: XCTestCase {
    @MainActor
    func testDiscoveryAndRefreshWaitForATapThenOpenDirectly() {
        // Background discovery never launches another app; selecting a result does.
        let app = launch(extra: ["--capture-moshi-links"])
        openProjectSession(app)
        let session = app.buttons["discovered-session:A1000000-0000-0000-0000-000000000001:w7:w7:t9"]
        XCTAssertTrue(session.waitForExistence(timeout: 15))
        XCTAssertEqual(app.staticTexts["moshi-opened-url"].label, "")
        app.buttons["Refresh sessions"].tap()
        XCUIDevice.shared.press(.home)
        app.activate()
        XCTAssertTrue(session.waitForExistence(timeout: 10))
        XCTAssertEqual(app.staticTexts["moshi-opened-url"].label, "")
        session.tap()
        app.assertMoshiOpened("moshi://herdr?workspace=w7")
    }

    @MainActor
    func testLiveLinksKeepTheirDestinationsAfterRefreshAndReturningToTheApp() {
        let app = launch(extra: ["--capture-moshi-links", "--observed-live-session-ids", "--prefer-moshi"])
        app.tabBars.buttons["Agents"].tap()
        app.buttons.matching(NSPredicate(format: "label BEGINSWITH %@", "Test Mac,")).firstMatch.tap()
        let first = app.buttons["live-chat:w7:w7:t1"]
        XCTAssertTrue(first.waitForExistence(timeout: 10))
        first.tap()
        app.assertMoshiOpened("moshi://herdr?workspace=w7")
        XCUIDevice.shared.press(.home)
        app.activate()
        // Foreground refresh reverses the workspace order in this fixture.
        for workspace in ["wC", "w2", "w7"] {
            let link = app.buttons["live-chat:\(workspace):\(workspace):t1"]
            XCTAssertTrue(link.waitForExistence(timeout: 10))
            if !link.isHittable { app.swipeUp() }
            link.tap()
            app.assertMoshiOpened("moshi://herdr?workspace=\(workspace)")
        }
    }

    @MainActor
    func testSwitchingLiveRowsSendsTheSelectedWorkspaceAndTab() {
        let app = launch(extra: ["--capture-moshi-links", "--prefer-moshi"])
        app.tabBars.buttons["Agents"].tap()
        app.buttons.matching(NSPredicate(format: "label BEGINSWITH %@", "Test Mac,")).firstMatch.tap()
        let first = app.buttons["live-chat:w7:w7:t9"]
        XCTAssertTrue(first.waitForExistence(timeout: 10))
        first.tap()
        app.assertMoshiOpened("moshi://herdr?workspace=w7")
        let other = app.buttons["live-chat:w8:w8:t1"]
        if !other.isHittable { app.swipeUp() }
        other.tap()
        app.assertMoshiOpened("moshi://herdr?workspace=w8")
    }

    @MainActor
    func testChosenProjectSessionSendsItsOwnTab() {
        let app = launch(extra: ["--multiple-project-sessions", "--capture-moshi-links"])
        openProjectSession(app)
        let chosen = app.buttons["discovered-session:A1000000-0000-0000-0000-000000000001:w7:w7:t10"]
        XCTAssertTrue(chosen.waitForExistence(timeout: 10))
        chosen.tap()
        app.assertMoshiOpened("moshi://herdr?workspace=w7&tab=w7%3At10")
    }

    @MainActor
    func testProjectFindsOneSessionAndOpensOnTap() {
        let app = launch()
        openProjectSession(app)
        let session = app.buttons["discovered-session:A1000000-0000-0000-0000-000000000001:w7:w7:t9"]
        XCTAssertTrue(session.waitForExistence(timeout: 15))
        XCTAssertFalse(app.alerts["Couldn't open Moshi"].exists)
        session.tap()
        XCTAssertTrue(app.alerts["Couldn't open Moshi"].waitForExistence(timeout: 5))
        app.alerts.buttons["OK"].tap()
        XCTAssertTrue(app.staticTexts["Build phone app"].exists)
        XCTAssertFalse(app.staticTexts["Unrelated session"].exists)
        XCTAssertFalse(app.textFields["moshi.session"].exists)
        let screenshot = XCTAttachment(screenshot: app.screenshot())
        screenshot.name = "Project session discovered without a manual Moshi link"
        screenshot.lifetime = .keepAlways
        add(screenshot)
    }

    @MainActor
    func testMultipleMatchesOfferRealSessionsInsteadOfGuessing() {
        let app = launch(extra: ["--multiple-project-sessions"])
        openProjectSession(app)
        XCTAssertTrue(app.staticTexts["Review phone changes"].waitForExistence(timeout: 15))
        XCTAssertTrue(app.staticTexts["Build phone app"].exists)
        XCTAssertFalse(app.alerts["Couldn't open Moshi"].exists)
        XCTAssertFalse(app.staticTexts["Unrelated session"].exists)
        let screenshot = XCTAttachment(screenshot: app.screenshot())
        screenshot.name = "Choose between discovered sessions in the same project"
        screenshot.lifetime = .keepAlways
        add(screenshot)
        app.buttons["discovered-session:A1000000-0000-0000-0000-000000000001:w7:w7:t10"].tap()
        XCTAssertTrue(app.alerts["Couldn't open Moshi"].waitForExistence(timeout: 5))
        XCTAssertFalse(app.textFields["moshi.session"].exists)
    }

    @MainActor
    func testLiveRowRecognizesProjectAndGraphCanResumeItsSession() {
        let app = launch()
        app.tabBars.buttons["Agents"].tap()
        app.buttons.matching(NSPredicate(format: "label BEGINSWITH %@", "Test Mac,")).firstMatch.tap()
        XCTAssertTrue(app.buttons["live-detail:w7:w7:t9"].waitForExistence(timeout: 10))
        app.buttons["live-detail:w7:w7:t9"].tap()
        let graph = app.buttons["Explore graph"]
        XCTAssertTrue(graph.waitForExistence(timeout: 5))
        // Secondary actions remain in details without a manual session link.
        app.buttons["session-detail-open"].tap()
        XCTAssertTrue(app.alerts["Couldn't open Moshi"].waitForExistence(timeout: 5))
        app.alerts.buttons["OK"].tap()
        graph.tap()
        XCTAssertTrue(app.webViews.staticTexts["PHONE"].firstMatch.waitForExistence(timeout: 20))
        app.buttons["Search graph"].tap()
        let search = app.textFields["Search findings, tasks, projects"]
        search.tap(); search.typeText("phone sessions")
        app.buttons.matching(NSPredicate(format: "label CONTAINS %@", "Keep phone sessions connected")).firstMatch.tap()
        let open = app.buttons["Open in Moshi"]
        if !open.isHittable { app.collectionViews.firstMatch.swipeUp() }
        open.tap()
        let found = app.buttons["discovered-session:A1000000-0000-0000-0000-000000000001:w7:w7:t9"]
        XCTAssertTrue(found.waitForExistence(timeout: 15))
        found.tap()
        XCTAssertTrue(app.alerts["Couldn't open Moshi"].waitForExistence(timeout: 15))
        XCTAssertFalse(app.textFields["moshi.session"].exists)
    }

    @MainActor
    func testOfflineComputerDoesNotInventOrOpenASession() {
        let app = launch(extra: ["--session-discovery-offline"])
        openProjectSession(app)
        XCTAssertTrue(app.staticTexts.matching(NSPredicate(format: "label CONTAINS %@", "connection closed")).firstMatch.waitForExistence(timeout: 15))
        XCTAssertFalse(app.alerts["Couldn't open Moshi"].exists)
        XCTAssertFalse(app.staticTexts["Build phone app"].exists)
    }

    @MainActor
    private func launch(extra: [String] = []) -> XCUIApplication {
        let app = XCUIApplication()
        app.launchArguments = ["--ui-testing", "--automatic-sessions-fixture"] + extra
        app.launch()
        XCTAssertTrue(app.tabBars.buttons["Projects"].waitForExistence(timeout: 15))
        return app
    }

    @MainActor
    private func openProjectSession(_ app: XCUIApplication) {
        app.buttons["project:sample/brain:phone"].tap()
        app.buttons["Project session"].tap()
        app.buttons["Open in Moshi"].tap()
    }
}


extension XCUIApplication {
    @MainActor
    func assertMoshiOpened(_ destination: String, file: StaticString = #filePath, line: UInt = #line) {
        let opened = XCTNSPredicateExpectation(predicate: NSPredicate(format: "label == %@", destination),
                                               object: staticTexts["moshi-opened-url"])
        XCTAssertEqual(XCTWaiter.wait(for: [opened], timeout: 5), .completed, file: file, line: line)
        XCTAssertFalse(buttons["Open workspace link"].exists, "Opening a session must not need another tap", file: file, line: line)
        XCTAssertFalse(staticTexts["Select the computer in Moshi first"].exists, file: file, line: line)
    }
}
