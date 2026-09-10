import XCTest
@testable import Phren

final class AppearanceTests: XCTestCase {
    func testNamedCustomThemesRoundTripEditsAndDeletion() {
        let suite = "phren.appearance-test.\(UUID())"
        let defaults = UserDefaults(suiteName: suite)!
        defer { defaults.removePersistentDomain(forName: suite) }
        defaults.set("midnight", forKey: PhrenAppearance.storageKey)
        let appearance = PhrenAppearance(defaults: defaults)
        XCTAssertEqual(appearance.name, "Charcoal")
        XCTAssertEqual(appearance.palette.text, 0xFFFFFF)
        var first = PhrenCustomTheme(name: "Ocean", palette: appearance.palette)
        ThemeColorField.background.apply(0x24303B, to: &first.palette)
        ThemeColorField.accent.apply(0x56B7DE, to: &first.palette)
        appearance.save(first)
        let second = PhrenCustomTheme(name: "Evening", palette: PhrenAppearanceStyle.amethyst.palette)
        appearance.save(second)
        first.name = "Ocean blue"
        appearance.save(first)
        let reloaded = PhrenAppearance(defaults: defaults)
        XCTAssertEqual(reloaded.customThemes.count, 2)
        XCTAssertEqual(reloaded.name, "Ocean blue")
        XCTAssertEqual(reloaded.palette, first.palette)
        XCTAssertEqual(reloaded.palette.chatCanvas, 0x24303B)
        XCTAssertEqual(reloaded.palette.action, 0x56B7DE)
        reloaded.remove(first)
        XCTAssertEqual(reloaded.name, "Charcoal")
        XCTAssertEqual(PhrenAppearance(defaults: defaults).customThemes, [second])
    }
}
