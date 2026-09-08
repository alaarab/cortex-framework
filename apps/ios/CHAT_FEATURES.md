# Native chat feature review

Reviewed September 8, 2026 against Moshi's public documentation and the installed
`moshi-hook 0.3.19` protocol. This tracks Phren's implementation; it does not claim
complete Moshi parity. Native SwiftUI code uses the existing helper through the
configured computer's pinned SSH connection. Moshi on the iPhone is optional.

## Available in this iteration

| Capability | Phren behavior | Reference |
| --- | --- | --- |
| Existing agent conversation | Exact computer/workspace/tab/pane/provider/session selection; Codex and Claude Code | [Chat View](https://getmoshi.app/docs/chat-view) |
| Live replies | Foreground WebSocket backlog and append frames; reconnect without resending drafts | [Gateway](https://getmoshi.app/docs/debug-gateway) |
| Earlier history | Load earlier numbered pages; keep loaded pages on reconnect, with memory bounds | [Chat debugging](https://getmoshi.app/docs/debug-chat-view) |
| Image and file attachments | Photos, camera, Files, and explicit clipboard paste; preview/remove, upload on Send, preserve failed drafts | [Image paste](https://getmoshi.app/docs/image-paste) |
| Dictation | Existing Apple Speech integration; edit transcription and add it to the draft | [Voice workflows](https://getmoshi.app/docs/voice) |
| Stop | Escape to the validated, working conversation; does not terminate the process | [Chat controls](https://getmoshi.app/docs/chat-view) |
| Readable messages | Native inline Markdown, headings, fenced code cards with Copy, collapsed tools, copy/share messages | [Chat View](https://getmoshi.app/docs/chat-view) |
| Keyboard send | Command-Return; ordinary Return remains available for multiline text | [Chat controls](https://getmoshi.app/docs/chat-view) |
| Project context | Insert selected Phren summaries, findings, or skills into a draft; open project memory/skills/graph | Phren feature |

Image uploads use the helper's observed `/v1/upload` API. It returns a path in a
temporary host directory; Phren appends that path to the explicit agent prompt.
This differs from the SCP location described in Moshi's image-paste guide.
There is no public image hosting or borrowing of Moshi's iPhone credentials.
Photos are re-encoded without source metadata and limited to 2,048 pixels;
attachments are limited to four files, 8 MB each. Local sent previews are bounded.

## Next iterations

| Gap | Work required before shipping |
| --- | --- |
| Structured approvals and questions | Observe request IDs, answer payloads, stale-request handling, and exact-session validation. The helper advertises `approvals.answer`; Phren still sends the user to the terminal. |
| Older transcript images | Implement and verify the session/line/block-scoped blob API. New local sent-image previews work; historical image blocks remain placeholders. |
| Repository diffs | Verify a read-only route scoped to the selected pane's repository; add native navigation for changed files and hunks. See [Diff Viewer](https://getmoshi.app/docs/diff-viewer). |
| Browser previews | Verify URL, tunnel ownership, lifecycle, and explicit user navigation. See [Browser Preview](https://getmoshi.app/docs/browser-preview). |
| Durable drafts | Store text/attachments locally with bounded storage, cleanup, and full conversation identity; current drafts survive only within the app process. |
| Additional providers and multiplexers | Add providers and tmux/named Herdr servers after verifying their transcript, discovery, and send contracts. |
| Usage, background notifications, agent creation | Separate integrations; no inferred account metrics or background agent supervision. See [Agents and Usages](https://getmoshi.app/docs/agents-usages). |

## Validation

- Core coverage: attachment bounds/filenames, transcript normalization, pane and
  conversation guards, history merge/reconnect/truncation, and retained caps.
- Real helper integration: fresh pinned SSH relay to an inert process in its own
  disposable Herdr pane; upload and compare image bytes on the host, deliver its
  path, receive a live reply, retrieve earlier history, and observe Escape.
- Simulator UI coverage: image preview/removal/send, upload failure retaining
  image and text, earlier history after foregrounding, stop/code cards, and
  existing conversation, project-context, pane-choice, and Moshi-preference flows.
- Physical camera/microphone capture and actual model interpretation of an image
  still require device verification. The integration fixture deliberately does
  not prompt a user's running agent.

See [connection details and limits](AGENT_CONNECTIONS.md#native-conversation).
