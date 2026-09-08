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
exposes workspace and pane discovery, live transcripts, earlier history,
attachments, exact-session prompt delivery, and interrupting the current turn.
There is no shell or arbitrary route API.

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
client uses `/v1/prompt` for deliberate replies and `/v1/keys` only for Escape
to interrupt a working turn. It exposes no arbitrary terminal keys, approval
responses, agent launch, or process termination API.

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
  and returns a `backlog` frame containing numbered raw JSONL entries, followed
  by `append` frames as the transcript changes. The helper emits transcript
  records, so this is not a guarantee of token-by-token generation.
- A WebSocket message `{type: "older", beforeLine, limit: 200}` returns an
  `older` frame. Phren uses a short separate connection for each earlier page,
  merges by absolute line/block identity, and retains pages on reconnect.
- `POST /v1/upload` accepts JSON `{name, data}` with a generated filename and
  base64 bytes. `{ok: true, path}` identifies the file on the chosen computer.
  This installed helper writes a temporary `moshi-upload-*` directory.
- `POST /v1/keys` accepts `{source, sessionId, keys: ["Escape"]}`. Phren checks
  the exact pane identity and working/nonblocked state immediately beforehand.
- `POST /v1/prompt` accepts JSON `{source, sessionId, pane, tab, text}` and returns
  `{ok: true}` when accepted. This acknowledges delivery, not agent completion.

The visible chat maintains a live WebSocket and checks pane identity/status every
three seconds. The initial connection deadline is 20 seconds; explicit POSTs
have 60 seconds, and the live connection uses a ping/idle watchdog. Transcript
frames are bounded to 8 MB, other responses to 1 MB, and prompts to 32 KB.
Loaded history is capped at 4,000 visible blocks or 12 MB of text. Backgrounding,
leaving chat, or changing the computer configuration cancels requests.
Codex response items and Claude message blocks become native conversation rows;
tool calls/results are collapsed. Reasoning and system records are excluded.
Native paragraphs, headings, and fenced code support text selection, copy, and
message sharing. Remote HTML is not rendered.

Replies require a deliberate send. There is no retry on reconnect. If delivery
is uncertain, the draft remains and the user is told to check the conversation
before trying again. Drafts survive reopening within the app process and are
keyed by the full conversation identity. Image/file drafts also survive
reopening in the same process. Successfully uploaded paths are reused after a
failed send; reconnect never uploads or delivers a draft automatically.
Transcripts and drafts are not written to Git or persisted to disk. The context picker inserts selected project summary,
finding, or skill text into the draft for review. Context uses the selected
pane's directory and full store identity. Project memory, skills, and graph are
also reachable from chat options.

The composer accepts Photos, camera, Files, and an explicit native Paste image
action. Up to four files (8 MB each) can be submitted. Photos are downsampled to
2,048 pixels and re-encoded without source metadata. Attachments upload only
when Send is tapped; returned local paths are appended to the agent prompt.
This uses the selected computer's SSH tunnel and helper, not a public upload
service or the Moshi phone app. Uploaded files follow the helper's temporary-file
lifetime; Phren does not promise permanent storage or delete host files remotely.
Recent sent image previews are kept in bounded memory. Older transcript image
blobs are not yet fetched, and drafts/previews do not survive app termination.

The microphone opens an editable dictation sheet using Phren's existing Apple
Speech integration. Recording starts only on explicit action, stops when leaving
or backgrounding, and inserts text into the draft without sending it.

In-app approvals, starting agents, process termination, and other providers
are not implemented. Stop interrupts the current working turn with Escape.
Recognized blocked/waiting states disable reply and direct the user to the terminal. **Open terminal in Moshi** remains available.

Core tests cover transcript normalization, attachment bounds, history merging,
and identity guards. Transport tests cover fragmented frames, limits, and
rejection of a different computer before key loading or network access.
An opt-in `PHREN_CHAT_E2E_FIXTURE` test uses
an inert echo process in a disposable Herdr pane and a pinned SSH relay to the
installed helper; it never prompts a real user's agent. UI tests cover native
read/reply, pane choice, failed drafts/uploads, image preview/removal/send, earlier
history after foregrounding, stop, code cards, project context, and the Moshi
preference. `PHREN_CHAT_ITERATION_FIXTURE` additionally verifies a real image
upload (including host file byte equality), live reply, earlier history, and
Escape receipt through pinned SSH to an inert Codex-shaped process. These tests
do not claim a physical iPhone camera/microphone check or model image analysis.

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
- Add structured approvals, questions, transcript image retrieval, and
  repository diff/browser previews after verifying their exact-session contracts.
- Track the ongoing [chat feature comparison](CHAT_FEATURES.md).

The CLI's existing `AgentRecord`/`JoinedAgent` and Herdr provider remain in
`packages/cli/src/agents/`. No process supervisor, remote task execution,
background transcript collection, new daemon, or webhook notifications are added here.
