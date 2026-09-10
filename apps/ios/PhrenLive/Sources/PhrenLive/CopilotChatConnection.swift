import Foundation
import PhrenKit

extension GatewayRequest {
    static func copilot(_ target: AgentChatTarget, action: String, text: String? = nil, before: Int? = nil) throws -> Self {
        guard target.source == "copilot", UUID(uuidString: target.sessionID) != nil else {
            throw PhrenKitError.validation("Copilot needs a session ID from Herdr's Copilot integration.")
        }
        var value: [String: Any] = ["action": action, "session": target.sessionID.lowercased(),
            "server": String(target.muxID.dropFirst("herdr:".count)), "workspace": target.workspaceID,
            "tab": target.tabID, "pane": target.paneID]
        value["text"] = text; value["before"] = before
        return try copilotRequest(value, streaming: action == "watch")
    }
    static func copilotRequest(_ value: [String: Any], streaming: Bool = false) throws -> Self {
        let data = try JSONSerialization.data(withJSONObject: value, options: [.sortedKeys])
        guard data.count <= 49_152 else { throw PhrenKitError.validation("The Copilot message is too large.") }
        return Self(path: "", streaming: streaming, progressCommand: "phren-copilot-chat " + data.base64EncodedString())
    }
}

extension MoshiConnection {
    static func checkCopilotResponse(_ data: Data) throws {
        if let value = try JSONSerialization.jsonObject(with: data) as? [String: Any], value["error"] != nil {
            let message = value["error"] as? String ?? "Copilot couldn't connect. Open its terminal, or update Phren's chat bridge on this computer."
            throw PhrenKitError.validation(String(message.prefix(512)))
        }
    }

    static func copilotDiscovery(host: LiveHost, key: Data) async throws -> Data {
        var request = try GatewayRequest.copilotRequest(["action": "discover", "server": host.herdrSession ?? "default"])
        request.timeoutSeconds = 3
        let data = try await fetchData(host: host, key: .init(rawRepresentation: key), request: request)
        try checkCopilotResponse(data)
        return data
    }

    /// Some helpers omit Copilot entirely, including its provider and status.
    /// Read the pane identity from Herdr; never infer it from a transcript file.
    static func copilotPaneIdentities(_ data: Data, host: LiveHost, key: Data, workspace: String, tab: String) async throws -> Data {
        var request = try GatewayRequest.copilotRequest(["action": "panes", "server": host.herdrSession ?? "default", "workspace": workspace, "tab": tab])
        request.timeoutSeconds = 3
        let identities = try await fetchData(host: host, key: .init(rawRepresentation: key), request: request)
        try checkCopilotResponse(identities)
        return try CopilotDiscovery.mergePanes(data, copilot: identities, workspace: workspace, tab: tab)
    }
}

/// Merge only explicit Copilot pane records into the helper's existing topology.
/// Missing bridges leave other providers and terminal access intact.
enum CopilotDiscovery {
    static func panes(_ data: Data?) throws -> [[String: Any]] {
        guard let data else { return [] }
        guard data.count <= 1_048_576,
              let value = try JSONSerialization.jsonObject(with: data) as? [String: Any],
              let panes = value["panes"] as? [[String: Any]], panes.count <= 2_000,
              Set(panes.compactMap { $0["id"] as? String }).count == panes.count,
              panes.allSatisfy({ pane in
                  pane["agent"] as? String == "copilot" && ["id", "workspaceID", "tabID"].allSatisfy { AgentChatTarget.validID(pane[$0] as? String ?? "") }
              }) else { throw PhrenKitError.validation("Unsupported Copilot session list.") }
        return panes
    }
    static func mergePanes(_ data: Data, copilot: Data, workspace: String, tab: String) throws -> Data {
        let additions = try panes(copilot).filter { $0["workspaceID"] as? String == workspace && $0["tabID"] as? String == tab }
        guard !additions.isEmpty, var value = try JSONSerialization.jsonObject(with: data) as? [String: Any],
              var existing = value["panes"] as? [[String: Any]] else { return data }
        for pane in additions {
            if let index = existing.firstIndex(where: { $0["id"] as? String == pane["id"] as? String }) { existing[index] = pane }
            else { existing.append(pane) }
        }
        value["panes"] = existing
        return try JSONSerialization.data(withJSONObject: value)
    }
    static func mergeWorkspaces(_ data: Data, copilot: Data?) throws -> Data {
        // Supplemental discovery must never take existing sessions offline.
        let additions = (try? panes(copilot)) ?? []
        guard !additions.isEmpty, var value = try JSONSerialization.jsonObject(with: data) as? [String: Any],
              var groups = value["groups"] as? [[String: Any]] else { return data }
        for group in groups.indices {
            guard var tabs = groups[group]["children"] as? [[String: Any]] else { continue }
            for tab in tabs.indices {
                let matches = additions.filter { $0["workspaceID"] as? String == groups[group]["id"] as? String && $0["tabID"] as? String == tabs[tab]["id"] as? String }
                guard !matches.isEmpty else { continue }
                let previous = tabs[tab]["agent"] as? String
                tabs[tab]["agent"] = previous == nil || previous == "copilot" ? "copilot" : previous! + ", copilot"
                tabs[tab]["agentPaneCount"] = previous == nil || previous == "copilot" ? matches.count : nil
                let statuses = matches.compactMap { $0["agentStatus"] as? String } + [tabs[tab]["agentStatus"] as? String].compactMap { $0 }
                tabs[tab]["agentStatus"] = ["error", "blocked", "waiting", "working", "done", "idle", "unknown"].first { statuses.contains($0) }
                if previous == nil, matches.count == 1 {
                    tabs[tab]["title"] = matches[0]["title"]
                    tabs[tab]["cwd"] = matches[0]["cwd"]
                }
            }
            groups[group]["children"] = tabs
        }
        value["groups"] = groups
        return try JSONSerialization.data(withJSONObject: value)
    }
}
