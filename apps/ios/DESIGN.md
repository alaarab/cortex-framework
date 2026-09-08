# iPhone interaction and design notes

The user's September 8, 2026 screenshots compare Phren with Moshi. The useful
reference is the density and clarity of the interaction, while keeping Phren's
own colors and identity.

- Give the work most of the screen. A session is a compact title and metadata
  row, with one primary tap to chat. Put terminal, Moshi, graph, and metadata in
  session details instead of repeating a second action row on every card.
- Connection freshness and counts belong in a quiet inline status area. Herdr
  belongs in the computer toolbar. Neither needs a large promotional tile.
- Keep controls at least 44 points to touch. Reduce padding, duplicate controls,
  and extra rows before reducing text size. Allow rows to grow with Dynamic Type.
- Use exactly one terminal key bar, 48 points high. Esc, Tab, Ctrl, a directional
  pad, Paste, and keyboard visibility cover common input without four permanent
  arrow buttons or another accessory row above the system keyboard.
- A finger swipe scrolls. In a mouse-aware TUI such as Herdr, send wheel events;
  in a normal shell, scroll local history. Never reinterpret an ordinary drag as
  remote text selection or cursor-key input.
- Hold to select a word, then drag to extend the local selection. Copy and Paste
  are explicit actions in the standard context menu. Selection itself does not
  change either clipboard or send text to the computer. Paste adds no Enter and
  respects the terminal's bracketed-paste mode.
- Keep tappable terminal controls working, including Herdr's workspace switcher.
  The graph similarly owns its pan gesture and uses its visible back button.
- Carry the screen background through project controls, including the Skills
  entry and section picker. Avoid unintentional black gutters between them.

Review the actual keyboard-open screen, not just the empty terminal. Test both
scroll directions, hold-and-drag, copy/paste, keyboard dismissal, and returning
from another app. Inspect standard and accessibility text sizes. A compact layout
must preserve session identity, freshness checks, and the optional Moshi default.

Moshi references: [keyboard controls](https://getmoshi.app/docs/keyboard) and
[gestures](https://getmoshi.app/docs/gestures). These are interaction references;
Phren uses its own UI and SwiftTerm's public APIs.
