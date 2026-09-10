import SwiftUI

enum PhrenAppearanceStyle: String, CaseIterable, Identifiable {
    case midnight, amethyst, graphite, slate
    var id: String { rawValue }
    var name: String { rawValue.capitalized }
    var detail: String {
        switch self {
        case .midnight: return "Black canvas. Bright Phren purple."
        case .amethyst: return "Deep violet. Soft lavender."
        case .graphite: return "Warm charcoal. Paper-white text."
        case .slate: return "Cool slate. Cyan and lavender."
        }
    }
    var palette: PhrenPalette {
        switch self {
        case .midnight:
            return .init(background: 0x050407, sunken: 0x030205, surface: 0x131018, raised: 0x251C32,
                         chatCanvas: 0x050407, chatPanel: 0x121016, text: 0xC99BFF, secondary: 0xCDB5EA,
                         muted: 0xAD95C8, dim: 0x9B89AD, accent: 0xB981FF, hover: 0xDDC1FF, solid: 0x7650B0,
                         action: 0xBE8AFF, navigation: 0xC99BFF)
        case .amethyst:
            return .init(background: 0x17121F, sunken: 0x100C17, surface: 0x251E32, raised: 0x3A2E4D,
                         chatCanvas: 0x17121F, chatPanel: 0x100D17, text: 0xEEE3FF, secondary: 0xD8CAE9,
                         muted: 0xB8A6CE, dim: 0xA491BA, accent: 0xB994F4, hover: 0xDCC5FF, solid: 0x7450A7,
                         action: 0xC39AF9, navigation: 0xDCC5FF)
        case .graphite:
            return .init(background: 0x202020, sunken: 0x181818, surface: 0x2C2B2E, raised: 0x424045,
                         chatCanvas: 0x202020, chatPanel: 0x151416, text: 0xF2EEF6, secondary: 0xDDD8E3,
                         muted: 0xBEB7C6, dim: 0xA8A1B1, accent: 0xC0AAF2, hover: 0xE0CCFF, solid: 0x756096,
                         action: 0xC0AAF2, navigation: 0xE4D9F3)
        case .slate:
            return .init(background: 0x292D3C, sunken: 0x242837, surface: 0x373D50, raised: 0x48516A,
                         chatCanvas: 0x272832, chatPanel: 0x1C1E27, text: 0xF3F2F8, secondary: 0xDCDFEF,
                         muted: 0xBCC3D8, dim: 0xABB3CA, accent: 0xB8AAF2, hover: 0xD2C5FF, solid: 0x71609E,
                         action: 0x70DBE8, navigation: 0xE4E3F5)
        }
    }
}

struct PhrenPalette {
    let background, sunken, surface, raised: UInt32
    let chatCanvas, chatPanel, text, secondary, muted, dim: UInt32
    let accent, hover, solid, action, navigation: UInt32
}

/// Read by the shared semantic colors, so existing views update in place.
/// Changing appearance never recreates navigation, chat models, or SSH sessions.
@Observable final class PhrenAppearance {
    static let storageKey = "appearance.theme.v1"
    static let shared = PhrenAppearance(defaults: ProcessInfo.processInfo.arguments.contains("--ui-testing")
                                        ? UserDefaults(suiteName: "phren.ui-tests")! : .standard)
    private let defaults: UserDefaults
    var style: PhrenAppearanceStyle {
        didSet { defaults.set(style.rawValue, forKey: Self.storageKey) }
    }

    init(defaults: UserDefaults) {
        self.defaults = defaults
        style = defaults.string(forKey: Self.storageKey).flatMap(PhrenAppearanceStyle.init(rawValue:)) ?? .midnight
    }
}
