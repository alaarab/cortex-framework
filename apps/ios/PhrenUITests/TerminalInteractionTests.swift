import XCTest

final class TerminalInteractionTests: XCTestCase {
    @MainActor
    func testOneCompactToolbarWithKeyboardAndArrowPad() throws {
        let app = launch("--terminal-mouse-fixture")
        app.buttons["Toggle terminal keyboard"].tap()
        XCTAssertTrue(app.keyboards.firstMatch.waitForExistence(timeout: 5))
        let toolbar = app.descendants(matching: .any).matching(identifier: "terminal-toolbar").firstMatch
        XCTAssertLessThanOrEqual(toolbar.frame.height, 48)
        for title in ["Esc", "Tab", "Ctrl"] {
            let keys = app.buttons.matching(NSPredicate(format: "label ==[c] %@", title))
            XCTAssertEqual(keys.count, 1,
                           "The terminal must not install a second key row")
            XCTAssertGreaterThanOrEqual(keys.firstMatch.frame.height, 44)
        }
        // iOS exposes keycaps below the rounded keyboard's own top inset.
        let inset = app.keyboards.firstMatch.frame.minY - toolbar.frame.maxY
        XCTAssertGreaterThanOrEqual(inset, 0)
        XCTAssertLessThanOrEqual(inset, 36)
        app.buttons["Arrow keys"].tap()
        app.buttons["Up"].tap(); app.buttons["Right"].tap()
        app.coordinate(withNormalizedOffset: CGVector(dx: 0.9, dy: 0.3)).tap()
        XCTAssertTrue(try state(app).input.contains("\u{1B}[A\u{1B}[C"))
        capture(app, "One compact terminal toolbar with keyboard")
        app.navigationBars["Herdr terminal"].buttons.element(boundBy: 0).tap()
        XCTAssertTrue(app.tabBars.buttons["Agents"].waitForExistence(timeout: 5))
        XCTAssertTrue(app.tabBars.buttons["Agents"].isHittable, "Leaving terminal restores app navigation")
    }

    @MainActor
    func testHerdrSwipeSendsOnlyWheelEventsAndHoldSelectsLocally() throws {
        let app = launch("--terminal-mouse-fixture")
        let terminal = app.descendants(matching: .any).matching(identifier: "herdr-terminal").firstMatch
        XCTAssertTrue(terminal.waitForExistence(timeout: 5))
        let upper = terminal.coordinate(withNormalizedOffset: CGVector(dx: 0.5, dy: 0.25))
        let lower = terminal.coordinate(withNormalizedOffset: CGVector(dx: 0.5, dy: 0.65))
        lower.press(forDuration: 0.05, thenDragTo: upper)
        upper.press(forDuration: 0.05, thenDragTo: lower)
        let scrolled = try state(app)
        XCTAssertTrue(scrolled.input.contains("\u{1B}[<64;"))
        XCTAssertTrue(scrolled.input.contains("\u{1B}[<65;"))
        let nonWheel = scrolled.input.replacingOccurrences(of: "\u{1B}\\[<6[45];[0-9]+;[0-9]+M",
                                                          with: "", options: .regularExpression)
        XCTAssertEqual(nonWheel, "", "A swipe must not send mouse presses, releases, drags, or arrow keys")
        XCTAssertEqual(scrolled.selected, "")
        XCTAssertEqual(scrolled.copyActions, 0)

        let origin = terminal.coordinate(withNormalizedOffset: .zero)
        let start = origin.withOffset(CGVector(dx: 45, dy: 8))
        let end = origin.withOffset(CGVector(dx: 240, dy: 40))
        start.press(forDuration: 0.7, thenDragTo: end)
        XCTAssertTrue(app.descendants(matching: .any).matching(NSPredicate(format: "label == %@", "Copy")).firstMatch.waitForExistence(timeout: 5))
        let selected = try state(app)
        XCTAssertTrue(selected.selected.hasPrefix("Selectable terminal text · line 1"),
                      "Selection must track the word under the finger: \(selected.selected)")
        XCTAssertTrue(selected.selected.contains("line 2"))
        XCTAssertFalse(app.keyboards.firstMatch.exists, "Selecting text must not summon the keyboard")
        XCTAssertEqual(selected.input, scrolled.input, "Selection stays on the phone")
        XCTAssertEqual(selected.copyActions, 0, "Selection alone must not copy")
        capture(app, "Hold to select with explicit copy and paste")
        app.descendants(matching: .any).matching(NSPredicate(format: "label == %@", "Copy")).firstMatch.tap()
        XCTAssertEqual(try state(app).copyActions, 1)
        app.buttons["Paste into terminal"].tap()
        XCTAssertEqual(try state(app).input, scrolled.input + selected.selected,
                       "Paste sends the copied text without adding Enter")
        app.buttons["Toggle terminal keyboard"].tap()
        terminal.coordinate(withNormalizedOffset: CGVector(dx: 0.8, dy: 0.25)).tap()
        let clicked = try state(app).input
        XCTAssertTrue(clicked.contains("\u{1B}[<0;"), "Taps must still reach Herdr controls")
        XCTAssertTrue(clicked.hasSuffix("m"), "A tap must release its mouse button")
        app.buttons["Toggle terminal keyboard"].tap()
        terminal.swipeUp()
        let resumed = try state(app).input
        XCTAssertTrue(resumed.hasPrefix(clicked))
        XCTAssertTrue(String(resumed.dropFirst(clicked.count)).contains("\u{1B}[<65;"),
                      "Scrolling must resume after selection and paste")
    }

    @MainActor
    func testSelectAllKeepsCopyAvailableWithoutOpeningKeyboard() throws {
        let app = launch("--terminal-mouse-fixture")
        let terminal = app.descendants(matching: .any).matching(identifier: "herdr-terminal").firstMatch
        terminal.coordinate(withNormalizedOffset: .zero).withOffset(CGVector(dx: 45, dy: 8)).press(forDuration: 0.7)
        let selectAll = app.descendants(matching: .any).matching(NSPredicate(format: "label == %@", "Select All")).firstMatch
        // The native edit menu paginates at accessibility text sizes.
        if !selectAll.waitForExistence(timeout: 2), app.buttons["Forward"].exists { app.buttons["Forward"].tap() }
        XCTAssertTrue(selectAll.waitForExistence(timeout: 5))
        XCTAssertEqual(try state(app).selected, "Selectable")
        selectAll.tap()
        let copy = app.descendants(matching: .any).matching(NSPredicate(format: "label == %@", "Copy")).firstMatch
        if !copy.exists, app.buttons["Back"].exists { app.buttons["Back"].tap() }
        XCTAssertTrue(copy.isHittable)
        let selected = try state(app)
        XCTAssertTrue(selected.selected.contains("line 1"))
        XCTAssertTrue(selected.selected.contains("line 18"))
        XCTAssertEqual(selected.copyActions, 0)
        XCTAssertEqual(selected.input, "")
        XCTAssertFalse(app.keyboards.firstMatch.exists)
        copy.tap()
        XCTAssertEqual(try state(app).copyActions, 1)
    }

    @MainActor
    func testShellScrollbackStaysLocal() throws {
        let app = launch("--terminal-scrollback-fixture")
        let terminal = app.descendants(matching: .any).matching(identifier: "herdr-terminal").firstMatch
        XCTAssertTrue(terminal.waitForExistence(timeout: 5))
        let before = try state(app)
        XCTAssertGreaterThan(before.topRow, 0)
        terminal.swipeDown()
        let after = try state(app)
        XCTAssertLessThan(after.topRow, before.topRow)
        XCTAssertEqual(after.input, "", "Local history scrolling must not send keys to the shell")
        XCTAssertEqual(after.copyActions, 0)
        capture(app, "Local terminal scrollback")
    }

    @MainActor
    private func launch(_ fixture: String) -> XCUIApplication {
        let app = XCUIApplication()
        app.launchArguments = ["--ui-testing", "--automatic-sessions-fixture", "--session-details-fixture", "--native-chat-fixture", fixture]
        app.launch()
        XCTAssertTrue(app.tabBars.buttons["Agents"].waitForExistence(timeout: 15))
        app.tabBars.buttons["Agents"].tap()
        app.buttons.matching(NSPredicate(format: "label BEGINSWITH %@", "Test Mac,")).firstMatch.tap()
        XCTAssertTrue(app.buttons["Herdr workspaces & terminal"].waitForExistence(timeout: 5))
        app.buttons["Herdr workspaces & terminal"].tap()
        app.buttons["Open Herdr terminal"].tap()
        XCTAssertTrue(app.staticTexts["terminal-fixture-report"].waitForExistence(timeout: 8))
        return app
    }

    private struct State: Decodable {
        let input: String
        let selected: String
        let topRow: Int
        let copyActions: Int
    }

    @MainActor
    private func state(_ app: XCUIApplication) throws -> State {
        // Reading AX waits for the gesture/animation to settle; the fixture reports at 10 Hz.
        try JSONDecoder().decode(State.self, from: Data(app.staticTexts["terminal-fixture-report"].label.utf8))
    }

    @MainActor
    private func capture(_ app: XCUIApplication, _ name: String) {
        let attachment = XCTAttachment(screenshot: app.screenshot())
        attachment.name = name; attachment.lifetime = .keepAlways
        add(attachment)
    }
}
