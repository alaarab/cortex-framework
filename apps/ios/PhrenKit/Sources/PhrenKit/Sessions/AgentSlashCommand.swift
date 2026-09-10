import Foundation

/// Suggestions are shortcuts, never a command allowlist. The live terminal
/// supplies the complete menu, including installed skills and plugin commands.
public enum AgentSlashCommand {
    public static func isCommand(_ text: String) -> Bool {
        text.hasPrefix("/")
    }
    public static func suggestions(source: String, draft: String) -> [String] {
        guard isCommand(draft), !draft.contains(where: { $0.isWhitespace }) else { return [] }
        let names: [String]
        switch source {
        case "codex": names = ["/model", "/permissions", "/review", "/status", "/skills", "/compact", "/resume", "/new", "/mcp"]
        case "claude": names = ["/help", "/model", "/permissions", "/context", "/usage", "/skills", "/compact", "/resume", "/clear", "/mcp"]
        case "copilot": names = ["/help", "/model", "/agent", "/context", "/usage", "/skills", "/compact", "/resume", "/clear", "/mcp"]
        default: names = []
        }
        return names.filter { $0.hasPrefix(draft.lowercased()) }
    }
}
