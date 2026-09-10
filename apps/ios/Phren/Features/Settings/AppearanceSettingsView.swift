import SwiftUI

struct AppearanceSettingsView: View {
    @State private var appearance = PhrenAppearance.shared

    var body: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: 18) {
                Text("Make it yours.")
                    .font(.title2.weight(.semibold)).foregroundStyle(PhrenTheme.text)
                Text("Choose a palette for your projects, chats, and terminals.")
                    .font(.subheadline).foregroundStyle(PhrenTheme.textMuted)
                ForEach(PhrenAppearanceStyle.allCases) { style in
                    Button { appearance.style = style } label: {
                        themeCard(style)
                    }
                    .buttonStyle(.plain)
                    .accessibilityLabel(style.name + ". " + style.detail)
                    .accessibilityValue(appearance.style == style ? "Selected" : "Not selected")
                    .accessibilityAddTraits(appearance.style == style ? .isSelected : [])
                    .accessibilityIdentifier("theme-\(style.rawValue)")
                }
            }.padding(18)
        }
        .background(PhrenTheme.bg)
        .navigationTitle("Theme").navigationBarTitleDisplayMode(.inline)
    }

    private func themeCard(_ style: PhrenAppearanceStyle) -> some View {
        let palette = style.palette
        let selected = appearance.style == style
        return VStack(alignment: .leading, spacing: 14) {
            HStack(spacing: 10) {
                VStack(alignment: .leading, spacing: 4) {
                    Text(style.name).font(.headline)
                    Text(style.detail).font(.caption).foregroundStyle(Color(hex: palette.muted))
                }
                Spacer(minLength: 4)
                Image(systemName: selected ? "checkmark.circle.fill" : "circle")
                    .font(.system(size: 22)).foregroundStyle(Color(hex: selected ? palette.action : palette.dim))
            }
            VStack(alignment: .leading, spacing: 10) {
                HStack(spacing: 6) {
                    Circle().fill(Color(hex: palette.action)).frame(width: 5, height: 5)
                    Text("phren").font(.system(.caption, design: .monospaced).weight(.semibold))
                    Spacer()
                    Image(systemName: "ellipsis")
                }
                Text("A little memory. A clearer thought.")
                    .font(.system(.subheadline, design: .monospaced))
                HStack {
                    Text("Message your agent…").font(.system(.caption, design: .monospaced))
                        .foregroundStyle(Color(hex: palette.muted))
                    Spacer()
                    Image(systemName: "arrow.up").font(.caption.weight(.semibold))
                        .foregroundStyle(Color(hex: palette.chatPanel))
                        .frame(width: 26, height: 26).background(Color(hex: palette.action), in: Circle())
                }.padding(10).background(Color(hex: palette.chatPanel), in: RoundedRectangle(cornerRadius: 14))
            }.accessibilityHidden(true)
        }
        .padding(16)
        .foregroundStyle(Color(hex: palette.text))
        .background(Color(hex: palette.background), in: RoundedRectangle(cornerRadius: 22))
        .overlay {
            RoundedRectangle(cornerRadius: 22)
                .strokeBorder(Color(hex: selected ? palette.action : palette.muted).opacity(selected ? 0.65 : 0.22), lineWidth: selected ? 1.5 : 0.5)
        }
    }
}
