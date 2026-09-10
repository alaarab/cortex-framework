# Phren Hook connections

Phren Hook is the computer-side service shipped with `@phren/cli`. The iPhone
connects over SSH with a separate device key and a pinned host key. There is no
third-party gateway dependency or public listener.

## Transport and identity

`phren bridge install` copies a self-contained Node bundle into
`~/.local/share/phren/bridge/versions/<version>` and activates it through a stable
`current` link. A LaunchAgent or systemd user service owns its lifetime.

The restricted SSH dispatcher accepts only `phren-hook v1 pipe` and
`phren-hook v1 terminal <server>`. The first relays HTTP/WebSocket bytes to a
mode-0600 Unix socket in a mode-0700 directory. The second attaches an existing
Herdr server through an SSH PTY. It does not execute arbitrary supplied shell
commands. Local web previews use a separate loopback-only SSH forward.

The health/workspace response identifies `product: phren-hook`, protocol 1, the
helper version, a durable computer ID, and capabilities. Mutating agent requests
carry the complete server/workspace/tab/pane/provider/conversation tuple. The
helper checks a fresh Herdr snapshot and the pane's session identity before
input. It never falls back to the focused pane or newest transcript. The iPhone
also checks its saved host UUID and pinned host key before connecting.

Herdr's public newline JSON socket API provides snapshots and targeted controls.
Conversation identity comes from a reported Herdr session ID, a transcript file
descriptor held by the foreground process, or a Phren lifecycle callback bound to
that terminal and process. Directory names are for project association only.

## Conversation protocol

- `GET /v1/health`, `/v1/muxes`, `/v1/workspaces`, `/v1/workspaces/panes`
- `WS /v1/transcripts`: backlog, append, and older frames with provider JSON rows
  and stable line numbers. History requests include `beforeLine`.
- `WS /v1/status`: exact-conversation activity, pending approval, and capabilities.
- `POST /v1/prompt`, `/v1/keys`, `/v1/upload`, `/v1/diff`
- `POST /v1/approvals/answer`: one exact pending callback, with approve or deny.
- `GET /v1/transcripts/blob`: bounded images from an exact transcript row/block.
- `POST /v1/workspaces/{create,rename,focus,close}` with an explicit server.
- `GET /v1/web-servers`, `/v1/activity`

Transcript readers retain bounded pages, wait for complete JSONL rows, detect
truncation/rotation, skip individual legacy rows over 64 MiB, exclude private reasoning and sidechain messages, and close
when a live conversation identity changes. Reconnection sends a fresh backlog;
the app merges by stable line identity. Input is attempted once. A missing reply
never triggers an automatic resend. Usage comes from actual provider events.

SSH terminal receive credit follows rendered bytes. Terminal output has a
bounded buffer and supports cancellation without closing remote shells. The
app's existing terminal gestures, keyboard dock, and reconnect policy apply.

## Local agent callbacks

Codex and Claude callback configuration preserves other hooks and includes
SessionStart, UserPromptSubmit, Stop, and PermissionRequest. Copilot callbacks
register SessionStart and UserPromptSubmit. They use a separate `agent.sock`
that the phone's dispatcher cannot reach. Callbacks never create agents.

Phren waits for an approval only while the exact conversation is watched. With
no watcher or no service, the agent continues its normal permission workflow.
An unanswered callback returns no decision after 55 seconds. The user can grant
or decline the current action; it never installs an always-allow rule. Codex
requires review of new hook definitions in `/hooks`. Existing sessions may need
to resume to load their provider's hook configuration.

Question dialogs and unsupported interactions stay in Phren's native terminal.
The protocol reports these capabilities explicitly. No blind terminal keystrokes
are used to answer a provider's structured approval.

Primary provider contracts:
[Codex hooks](https://developers.openai.com/codex/hooks),
[Claude hooks](https://code.claude.com/docs/en/hooks), and
[Copilot hooks](https://docs.github.com/en/copilot/reference/hooks-reference).
Herdr's installed `herdr api schema --json` is the control contract (protocol 20
was used for validation).

## Storage and migration

Recognized `phren-iphone` restricted keys gain the new dispatcher and PTY access,
with a timestamped backup and a concurrent-change check. Other keys are untouched.
The iPhone keeps its host/keychain identities, store mappings, and drafts. Old
external-app preferences are ignored. Existing third-party helpers remain intact.

Uploads are private, validate common image headers, cap individual images at
8 MiB and total retained storage at 256 MiB, and expire after 14 days when another
image is uploaded. The local activity journal retains two files of roughly 2 MiB
and contains status/provenance, not prompts or transcript text. Uninstall leaves
local data and SSH backups available for manual recovery.

## Verification

Build the CLI before running `packages/cli/src/bridge/bridge.test.ts`. It starts
the real bundled service with disposable Unix sockets and synthetic provider
logs, checks strict target validation, stream append/rotation, image boundaries,
private callback isolation, and stale approval rejection. No Moshi helper is used.

`PhrenLive` tests cover pinned SSH, protocol handling, cancellation, web previews,
and byte bounds. `PhrenHookEndToEndTests` can target a disposable local sshd and
named `phren-hook-standalone` Herdr server via `PHREN_HOOK_SSH_FIXTURE`. Its input
is restricted to a workspace the test creates and closes.

Run `phren bridge doctor` on each deployment computer. A passing Mac check does
not establish that a second computer has installed the service.
