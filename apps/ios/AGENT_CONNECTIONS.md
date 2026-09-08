# Agent connections: Moshi and Herdr

Phren manages project memory, skills, findings, tasks, and the graph. The iPhone
app reads live Herdr status and supports native Codex and Claude Code chat
through an existing Moshi hook. The Moshi iPhone app is optional; its installed
computer helper is still required for this adapter.

## Implemented connection

Agents → Add computer adds a computer with its own device SSH key and a
verified host fingerprint. Tailscale provides network reachability; Phren does
not borrow the Moshi app's credentials or tunnel. No Phren gateway is required.
The SSH channel only opens the remote loopback address `127.0.0.1:24543` and
exposes workspace and pane discovery, recent transcripts, and exact-session
prompt delivery. There is no shell or arbitrary route API.

The endpoint was observed on installed `moshi-hook 0.3.19`, which returns
`kind`, `capabilities`, and workspace `groups` with tab `children`. Tab metadata
includes `agentStatus`, `agent`, `cwd`, and optionally `agentPaneCount`. The
adapter supports the default Herdr server, not tmux or named server discovery.
Status cards summarize tabs. Native chat separately discovers the actual panes
and asks which agent to open when several supported conversations are present.
An agent conversation's `sessionId` is **not** a Herdr server/session name and
must not be used as one in a Moshi URL. Unknown states remain unknown.

The phone polls only visible computer or project-session screens, with cancellation, a total
request deadline, response size limits, and explicit stale/disconnected labels.
Live metadata stays in memory. An explicit directory → full store ID/project
mapping connects a tab to a graph. Without an explicit mapping, the deepest
directory component matching one unique attached project recognizes it automatically.
Path boundaries, longest-root matching, and full store identity prevent similarly
named directories or projects from being conflated. Missing
stores or projects are shown as unavailable. Preferences reject corrupt or
future schemas without replacing the original data.

The exported SSH authorization line restricts forwarding to the gateway and
disables shell commands. The hook itself offers more capabilities than status;
this authorization is not a server-side read-only credential scope. The chat
client uses `/v1/prompt` for explicit text replies but exposes no arbitrary
terminal keys, approval responses, or agent launch/stop operations.

See [phone setup and tests](README.md#live-herdr-sessions-over-tailscale--ssh),
[Moshi gateway roles](https://getmoshi.app/docs/install-desktop),
[workspace discovery](https://getmoshi.app/docs/debug-multiplexer-chooser), and
[Tailscale setup](https://getmoshi.app/docs/tailscale).

## Native conversation

Tapping a live card opens Phren chat by default. Projects and graph node details
also offer **Chat with agent**. **Settings → Agent conversations → Open agents
in** can make normal agent taps open Moshi; explicit chat actions still use Phren.
Cards offer an explicit **Chat** action, an optional Moshi shortcut, and an info
button for full session details.

`GET /v1/workspaces/panes?groupId=…&childId=…` supplies the pane, provider, and
conversation ID. Phren pins that tuple together with the configured computer,
workspace, and tab. It never selects the newest transcript or guesses by title.
The pane is checked again immediately before each prompt. If its provider or
conversation changes, sending stops until the user reopens chat. Native routing
uses the chosen computer's SSH connection, independent of Moshi's current card.

The observed `moshi-hook 0.3.19` contract is:

- `/v1/transcripts?source=codex|claude&session=…&limit=200` upgrades to a WebSocket
  and returns a `backlog` frame containing numbered raw JSONL entries.
- `POST /v1/prompt` accepts JSON `{source, sessionId, pane, tab, text}` and returns
  `{ok: true}` when accepted. This acknowledges delivery, not agent completion.

The current reader fetches one recent snapshot every three seconds while chat
is visible and active. Each request has a 20-second deadline; transcripts are
bounded to 8 MB, other responses to 1 MB, and prompts to 32 KB. Backgrounding,
leaving chat, or changing the computer configuration cancels requests. This is
recent conversation polling, not token streaming or full-history pagination.
Codex response items and Claude message blocks become native conversation rows;
tool calls/results are collapsed. Reasoning and system records are excluded.

Replies require a deliberate send. There is no retry on reconnect. If delivery
is uncertain, the draft remains and the user is told to check the conversation
before trying again. Drafts survive reopening within the app process and are
keyed by the full conversation identity. Transcripts and drafts are not written
to Git or persisted to disk. The context picker inserts selected project summary,
finding, or skill text into the draft for review. Context uses the selected
pane's directory and full store identity. Project memory, skills, and graph are
also reachable from chat options.

Attachments, in-app approvals, starting/stopping agents, and other providers
are not implemented. Recognized blocked/waiting states disable reply and direct
the user to the terminal. **Open terminal in Moshi** remains available.

Core tests cover transcript normalization and identity guards. Transport tests
cover fragmented frames and limits. An opt-in `PHREN_CHAT_E2E_FIXTURE` test uses
an inert echo process in a disposable Herdr pane and a pinned SSH relay to the
installed helper; it never prompts a real user's agent. UI tests cover native
read/reply, pane choice, failed drafts, project context, and the Moshi preference.

## Optional iPhone handoff

Live rows construct a Herdr destination from the hook's observed IDs. When the
latest snapshot contains just one tab in a workspace, the handoff uses only
`workspace`, avoiding a second tab-focus transition after Moshi resumes its card.
Multiple tabs (or an unknown tab count) include the selected tab's ID. The default
server is implicit. No manual link or project mapping is needed to open the tab.
The Agents action is labelled with the destination workspace,
with its view identity tied to the URL across row refreshes. Long-pressing it
can copy that same link for a direct comparison in Safari.
Project → Project session and graph node details → Session discover current
sessions using directory recognition and explicit mappings. A single match stays
visible for selection, as do multiple matches. Discovery and subsequent refreshes
never launch Moshi automatically. Tapping a session's open action hands off
directly, without a computer confirmation popup. Opening an unmatched session
remembers its directory for this project.

Manual tmux/Herdr shortcuts remain available for unsupported discovery targets.
The app encodes each value independently and preserves shortcuts on launch failure.

Moshi's links resume active/minimized session cards; they do not create a
connection and have no public host selector. Matching workspaces across hosts
can therefore be ambiguous. Known workspace collisions show a warning; the user
must have the intended computer connected in Moshi. A unique result among Phren's
configured computers does not establish uniqueness among Moshi's active cards.
Moshi chooses the matching card, which can be on a different computer. The URL
can also resolve to an unintended computer. Full host selection requires a supported
Moshi API. Do not invent `hostId` parameters or use its internal terminal route.
Phren cannot inspect Moshi's iPhone session cards or share its credentials.
An agent conversation ID is never used as a Herdr server, workspace, tab, or pane.
[Link grammar](https://getmoshi.app/docs/notifications#open-active-sessions-with-deep-links).

### Investigating a wrong-session handoff

Moshi 3.13.0 or newer is required for the `tab` and `pane` parameters. Verify
the installed phone version when workspace navigation resumes the old tab.
The hook version alone does not establish the phone app's capabilities.

`AutomaticSessionTests` captures the actual URL passed to iOS and checks both
switching between live workspace rows and choosing a different tab in a project.
The capture is enabled only in debug simulator UI tests with
`--capture-moshi-links`; it does not intercept links in device builds.

If the outgoing workspace/tab IDs are correct, compare a direct Safari link
using the current hook IDs, first with `workspace` alone and then with `tab`.
This separates Phren's project matching from Moshi's card selection and focus.
An accepted iOS URL-open callback confirms only that the app handled the URL;
it cannot confirm which terminal or Chat View Moshi displayed. A passing
simulator launch test is therefore not physical-device handoff verification.
Regression coverage checks that initial discovery and foreground refresh do not
send a URL, a single tap opens the selected destination without a confirmation,
and that destination stays correct after row refreshes and returning to the app.

## Next integration steps

- Verify setup and the app handoff on a physical iPhone over its tailnet.
- Add named Herdr servers and tmux only after observing their discovery contract.
- Retain provider session/workspace/tab/pane metadata if the desktop registry
  becomes a source. Never execute its `focus` argv from a phone payload.
- Add full-history loading, streaming updates, attachments, and structured
  approvals after verifying the helper contracts and their lifecycle behavior.

The CLI's existing `AgentRecord`/`JoinedAgent` and Herdr provider remain in
`packages/cli/src/agents/`. No process supervisor, remote task execution,
background transcript collection, new daemon, or webhook notifications are added here.
