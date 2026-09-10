"""Read only progress counters for one exact agent conversation over SSH.

No installation, transcript content output, agent input, or filesystem writes.
stdin EOF ends the reader when the SSH channel closes.
"""
import glob
import json
import os
from pathlib import Path
import select
import shlex
import sys
import uuid

# Preserve the counters-only command and explicitly dispatch the Copilot
# adapter. SSH_ORIGINAL_COMMAND is parsed as data, never executed as a shell.
original = shlex.split(os.environ.get("SSH_ORIGINAL_COMMAND", "")) if not sys.argv[1:] else []
if len(original) == 2 and original[0] == "phren-copilot-chat":
    import runpy
    bridge = runpy.run_path(str(Path(__file__).with_name("copilot-chat.py")))
    bridge["main"](original[1])
    raise SystemExit(0)

args = sys.argv[1:]
if not args:
    # authorized_keys invokes this fixed reader, never the requested shell text.
    args = shlex.split(os.environ.get("SSH_ORIGINAL_COMMAND", ""))
    if len(args) != 3 or args[0] != "phren-chat-progress":
        raise SystemExit(1)
    args = args[1:]
if len(args) != 2:
    raise SystemExit(1)
source, session = args
try:
    valid = source in ("codex", "claude") and str(uuid.UUID(session)) == session.lower()
except ValueError:
    valid = False
if not valid:
    raise SystemExit(1)
root = Path(os.environ.get("CODEX_HOME", str(Path.home() / ".codex")) if source == "codex"
            else os.environ.get("CLAUDE_CONFIG_DIR", str(Path.home() / ".claude"))).resolve()
pattern = "sessions/*/*/*/rollout-*-" + session + ".jsonl" if source == "codex" else "projects/*/" + session + ".jsonl"
paths = [Path(p).resolve() for p in glob.glob(glob.escape(str(root)) + "/" + pattern)]
paths = [p for p in paths if root in p.parents and p.is_file()]
if len(paths) != 1:
    raise SystemExit(1)  # Never pick a newest or neighboring conversation.
path = paths[0]


def progress(raw):
    if source == "codex":
        if raw.get("type") != "event_msg":
            return None
        p = raw.get("payload", {})
        if not isinstance(p, dict):
            return None
        kind = p.get("type")
        if kind == "token_count":
            info = p.get("info") or {}
            if not isinstance(info, dict):
                return None
            value = info.get("last_token_usage")
            if not isinstance(value, dict):
                return None
            p = {"type": kind, "info": {"last_token_usage": {
                k: value[k] for k in ("input_tokens", "output_tokens", "cached_input_tokens") if k in value}}}
        elif kind in ("task_started", "task_complete", "turn_aborted", "task_aborted"):
            p = {k: p[k] for k in ("type", "started_at", "completed_at") if k in p}
        else:
            return None
        return {"type": "event_msg", "timestamp": raw.get("timestamp"), "payload": p}
    message = raw.get("message")
    if raw.get("isMeta") or raw.get("isSidechain") or not isinstance(message, dict) or message.get("role") != "assistant":
        return None
    value = message.get("usage")
    if not isinstance(value, dict):
        return None
    return {"type": "assistant", "message": {"role": "assistant", "usage": {
        k: value[k] for k in ("input_tokens", "output_tokens", "cache_read_input_tokens") if k in value}}}


file = None
identity = None
line = 0
try:
    while True:
        if path.resolve() != path:
            raise SystemExit(1)
        stat = path.stat()
        if stat.st_size > 536870912:  # Bound initial scan and growing logs to 512 MB.
            raise SystemExit(1)
        reset = file is None or identity != (stat.st_dev, stat.st_ino) or stat.st_size < file.tell()
        if reset:
            if file:
                file.close()
            file = path.open("rb")
            identity = (stat.st_dev, stat.st_ino)
            line = 0
        entries = []
        # Read complete lines only; the agent may still be writing its last row.
        while True:
            offset = file.tell()
            row = file.readline(1048577)
            if not row:
                break
            oversized = len(row) > 1048576
            while row and not row.endswith(b"\n") and oversized:
                row = file.readline(1048577)
            if not row.endswith(b"\n"):
                file.seek(offset)
                break
            if oversized and source == "codex" and line == 0:
                raise SystemExit(1)
            if not oversized:
                try:
                    raw = json.loads(row)
                    if source == "codex" and line == 0:
                        if raw.get("type") != "session_meta" or raw.get("payload", {}).get("id") != session:
                            raise SystemExit(1)
                    if source == "claude" and raw.get("sessionId", session) != session:
                        raise SystemExit(1)
                    value = progress(raw) if isinstance(raw, dict) else None
                    if value:
                        entries.append({"line": line, "raw": value})
                        entries = entries[-32:]
                except (ValueError, TypeError, AttributeError):
                    if source == "codex" and line == 0:
                        raise SystemExit(1)
            line += 1
        if reset or entries:
            print(json.dumps({"type": "backlog" if reset else "append", "source": source,
                              "entries": entries, "totalLines": line}), flush=True)
        readable, _, _ = select.select([sys.stdin], [], [], 1)
        if readable and not os.read(sys.stdin.fileno(), 1):
            break
finally:
    if file:
        file.close()
