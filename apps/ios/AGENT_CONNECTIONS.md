# Agent connections: Moshi and Herdr

Phren manages project memory, skills, findings, tasks, and the graph. The iPhone
app reads live Herdr status and supports native Codex and Claude Code chat
through an existing Moshi hook. The Moshi iPhone app is optional; its installed
computer helper is still required for this adapter.

## Implemented connection

Agents → Add computer adds a computer with its own device SSH key and a
verified host fingerprint. Tailscale provides network reachability; Phren does
not borrow the Moshi app's credentials or tunnel. No Phren gateway is required.
Agent SSH channels open the remote loopback address `127.0.0.1:24543` and
exposes workspace and pane discovery, live transcripts, earlier history,
attachments, exact-session prompt delivery, and interrupting the current turn.
Native terminal input is carried by the helper's Herdr PTY route. Web previews
use separate pinned SSH connections forwarding only to a discovered HTTP(S)
server's loopback address and port. A separate restricted SSH exec channel reads
only token counters and lifecycle events for the selected conversation.

The endpoint was observed on installed `moshi-hook 0.3.19`, which returns
`kind`, `capabilities`, and workspace `groups` with tab `children`. Tab metadata
includes `agentStatus`, `agent`, `cwd`, and optionally `agentPaneCount`. The
adapter supports default and named Herdr servers, selected from `/v1/muxes`. Workspace requests carry `mux=herdr:<name>`; terminal requests use `mux=herdr&muxSession=<name>`. tmux discovery is not implemented.
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

The Agents landing page also polls all saved computers, using one independent
`LiveHostMonitor` per configured host. It publishes each result as it arrives;
there is no all-host response barrier. Working and waiting sessions lead the
combined list. Every row identifies its computer, and row/detail identity includes
the host ID and Herdr server as well as workspace/tab IDs. Search spans machine,
project, title, agent, and folder. Offline snapshots are separated under Last seen
and cannot open chat, while other computers remain live. Leaving/backgrounding
cancels the loops. Host removal or configuration changes replace the corresponding
monitor, so its previous destination cannot leak into the new connection.
App tests exercise independent updates, cancellation, failed hosts, closed tabs,
and host changes. Chat presentation belongs to the overview, so moving a row to
another status group does not dismiss an open conversation. UI tests open and reply to sessions with identical workspace/tab
IDs on two computers, verify the host label and reply isolation, and cover search,
offline rows, empty state, and returning from the background.

The exported SSH authorization line restricts forwarding to IPv4/IPv6 localhost and
forces a counters-only reader, which rejects shell commands. The hook itself offers more capabilities than status;
this authorization is not a server-side read-only credential scope. The chat
client uses `/v1/prompt` for deliberate replies and `/v1/keys` only for Escape
to interrupt a working turn. The native terminal carries deliberate keyboard input. Approval/question endpoints revalidate the pane tuple and use the exact helper-verified action/prompt identity. Workspace/tab closure requires an explicit confirmation because it stops processes.

See [phone setup and tests](README.md#live-herdr-sessions-over-tailscale--ssh),
[Moshi gateway roles](https://getmoshi.app/docs/install-desktop),
[workspace discovery](https://getmoshi.app/docs/debug-multiplexer-chooser), and
[Tailscale setup](https://getmoshi.app/docs/tailscale).

## Web servers

The globe opens a compact list grouped by saved SSH computer. A bounded `/events`
WebSocket snapshot supplies `servers` (name, origin, port, process, cwd). Entries
use computer ID + normalized loopback origin/port rather than temporary scan IDs.
Only valid HTTP(S) loopback origins are accepted. Discovery failures retain a
clearly previous list; a fresh scan must still contain the endpoint before opening.

The browser creates a loopback-only listener on the phone and relays raw TCP
through direct-tcpip children of one authenticated, pinned SSH connection. This
preserves HTTP bodies, assets, redirects, WebSocket upgrades, and TLS. It tries
the app's original port first, with an ephemeral fallback when occupied. Relay
reads apply backpressure; at most 64 browser connections are accepted per preview.
Closing/backgrounding tears down listeners and channels, without stopping the
remote server. Reconnect keeps the current WKWebView and its temporary storage;
dismissing discards them so a different computer cannot inherit localhost cookies.
WKWebView keeps certificate validation and has no bridge to Phren's native APIs.

Build 18's exported key line allows `127.0.0.1:*` and `[::1]:*`, retaining
`restrict`, `port-forwarding`, and `command="/usr/bin/false"`. The migration script
only changes old Phren-labelled restricted lines and backs up authorized_keys.
Transport tests cover pinned hosts, blocked app ports, two computers sharing a
port, redirects, 2 MB uploads, concurrent assets, live reload, and listener closure.
`PHREN_TEST_WEB_SERVERS=1 swift test --filter WebPreviewTests` additionally checks
the installed helper's discovery and a real local app through an isolated SSH relay.

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
The prompt addresses that live pane on the configured Herdr server. Supplying
`sessionId` to the helper selects its remembered terminal location even when
`pane` is also present; an absent or unusable record can reject a live Herdr
conversation with HTTP 404/422. Phren does not depend on that remembered location
for message delivery. Its last identity check is client-side; the helper also
checks the live pane's provider, but does not atomically compare the conversation
ID in this pane-addressed request.

The observed `moshi-hook 0.3.19` contract is:

- `/v1/transcripts?source=codex|claude&session=…&limit=200` upgrades to a WebSocket
  and returns a `backlog` frame containing numbered raw JSONL entries, followed
  by `append` frames as the transcript changes. The helper emits transcript
  records, so this is not a guarantee of token-by-token generation.
  New assistant entries and prefix extensions reveal progressively in the UI;
  the canonical transcript retains the entire received text for copy/share.
  A bounded queue advances at 30 Hz only while visible, catching long bursts up
  within about 2.5 seconds. It never manufactures text before a transcript
  entry arrives. Initial history, older pages, reconnects, Reduce Motion, and
  VoiceOver show received content immediately. Auto-follow updates during the
  reveal only while the reader is at the bottom.
- A WebSocket message `{type: "older", beforeLine, limit: 200}` returns an
  `older` frame. Phren uses a short separate connection for each earlier page,
  merges by absolute line/block identity, and retains pages on reconnect.
- `POST /v1/upload` accepts JSON `{name, data}` with a generated filename and
  base64 bytes. `{ok: true, path}` identifies the file on the chosen computer.
  This installed helper writes a temporary `moshi-upload-*` directory.
- `POST /v1/keys` accepts `{source, sessionId, keys: ["Escape"]}`. Phren checks
  the exact pane identity and working/nonblocked state immediately beforehand.
- `POST /v1/prompt?mux=herdr:<server>` accepts JSON `{source, pane, text}` over
  the loopback SSH forward and returns `{ok: true}` when accepted. Phren sends
  one explicit pane, without `sessionId` or a tab/focus fallback. This
  acknowledges delivery, not agent completion.

Codex `task_started`, `task_complete`, and abort events drive turn progress and
reported start times. The selected pane/status stream supplies live activity
for both agents, including approvals and questions. Sending starts a local
waiting indicator; it is cleared by new work/output or a delivery failure,
without replaying the prompt. Codex `token_count.info.last_token_usage` and
Claude assistant `message.usage` supply the latest response's token counts.
Unknown counters are omitted. Absolute line cursors ignore older pages and
duplicate status/usage records, and transcript replacement resets progress.
Counts are never synthesized from text or animated into a fake token rate.

The installed helper omits empty `entries` arrays and filters Codex lifecycle and
usage events from `/v1/transcripts`. Empty snapshots are accepted. A separate,
foreground-only SSH exec channel runs the installed fixed `chat-progress.py`
reader to obtain the missing records. `enable-chat-progress.py` installs it and
migrates only old restricted Phren-labelled keys with a backup. The forced command
accepts only `phren-chat-progress <provider> <UUID>`; arbitrary commands remain
disabled. It selects one exact session file, rejects ambiguous paths, validates
Codex session metadata, excludes Claude sidechains, and emits only allowed
counters/timestamps/state fields. It never emits transcript messages or reasoning.
Reads use 1 MB line and 512 MB file bounds, retain at most 32 progress records,
poll appended complete rows once a second, reset after truncation/replacement,
and exit on SSH stdin EOF. No agent input, installation daemon, or file writes
occur during a read. Missing Python/reader/permission/transcript leaves chat usable
and displays token setup access. Transcript-carried usage remains a fallback.

Validation includes parser/reveal tests, the complete waiting-to-finished simulator
flow, real helper text plus counters over pinned SSH, and restricted macOS SSH
authorization. `python3 apps/ios/scripts/test-chat-progress.py` covers content
filtering, split writes, truncation, session isolation, rejected commands, key
migration, Claude usage, and reader exit after disconnect.

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
before trying again. Failed HTTP requests retain a bounded plain JSON error
reason (32 KB response, 320 displayed characters), rather than recommending a
helper upgrade for every rejection. If the transcript connection drops, the
composer shows Reconnect; reconnecting never submits the draft.
Drafts survive process relaunch and are keyed by the full conversation identity, including the Herdr server. Text and attachment bytes are stored atomically in protected, backup-excluded Application Support files (four attachments per draft, 256 MB total attachment storage). Corrupt/future manifests are preserved and cannot be overwritten. Successfully uploaded paths are reused after a
failed send; reconnect never uploads or delivers a draft automatically.
Transcripts stay in bounded memory. Drafts are never written to Git or synchronized. Uploaded paths are not persisted across launches; attachments upload again when the user sends. The context picker inserts selected project summary,
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
Recent sent image previews are kept in bounded memory. Historical images use `/v1/transcripts/blob` with source/session/absolute line/original block index. Downloads are limited to 8 MB, cached within 16 MB, and downsampled for display.

The microphone opens an editable dictation sheet using Phren's existing Apple
Speech integration. Recording starts only on explicit action, stops when leaving
or backgrounding, and inserts text into the draft without sending it.

Approvals stream from `/events` and render inline. Question cards recognize Codex `request_user_input` and Claude `AskUserQuestion` transcript calls, clearing on their tool results. Answers are single-attempt POSTs; changed or already-answered prompts reject without replay. Unsupported/free-text/plan prompts remain available in the native Herdr terminal. Stop interrupts the current working turn with Escape. **Open terminal in Moshi** remains optional.

**Herdr** on a computer opens its workspace browser, named server picker and native terminal. Workspaces/tabs can be created, renamed and closed; panes can be listed, created and focused. New workspaces require an explicit folder path. Closing workspaces/tabs prompts before stopping processes. Terminal output uses bounded binary WebSocket frames with resize and acknowledgement flow control; leaving/backgrounding detaches the client and never kills its panes. SwiftTerm 1.20.0 is MIT licensed; notices are included in Settings. Its reviewed build plugin only generates version metadata, so command-line builds use `-skipPackagePluginValidation`.

**Repository changes** validates the selected pane and starts `/v1/diff/start` with that pane's cwd. Phren accepts only the returned local `/apps/diff/diff_<hex>/` path and renders `api/status`. Separate diff IDs prevent other repository views from changing the selected diff. Binary or omitted/oversized patches offer the native terminal; this viewer does not stage, discard, or commit files.

Core tests cover transcript normalization, attachment bounds, history merging,
and identity guards. Transport tests cover fragmented frames, limits, named
Herdr prompt routing, rejected recorded terminal locations, bounded HTTP error
reasons, single-attempt delivery, and
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
- Add tmux after observing its discovery and terminal contracts.
- Retain provider session/workspace/tab/pane metadata if the desktop registry
  becomes a source. Never execute its `focus` argv from a phone payload.
- Add browser previews, additional agent providers and dedicated agent launch controls after verifying their contracts.
- Track the ongoing [chat feature comparison](CHAT_FEATURES.md).

The CLI's existing `AgentRecord`/`JoinedAgent` and Herdr provider remain in
`packages/cli/src/agents/`. No process supervisor, remote task execution,
background transcript collection, new daemon, or webhook notifications are added here.
