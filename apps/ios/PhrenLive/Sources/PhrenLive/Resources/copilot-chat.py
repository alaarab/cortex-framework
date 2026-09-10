"""Bounded Copilot CLI transcript and exact Herdr pane input, over device SSH.

Invoked only by chat-progress.py's forced-command dispatcher. No shell commands,
arbitrary file paths, session resume, or agent startup are accepted from the phone.
Event schema: github/copilot-sdk nodejs/src/generated/session-events.ts.
"""
import base64
from collections import deque
import json
import os
from pathlib import Path
import re
import select
import shutil
import subprocess
import sys
import uuid


class BridgeError(ValueError):
    pass


def request(encoded):
    if len(encoded) > 65536:
        raise BridgeError("Request too large.")
    value = json.loads(base64.b64decode(encoded, validate=True))
    if not isinstance(value, dict) or set(value) - {"action", "session", "server", "workspace", "tab", "pane", "text", "before"}:
        raise BridgeError("Unsupported Copilot request.")
    if value.get("action") not in ("discover", "panes", "watch", "history", "send", "stop"):
        raise BridgeError("Unsupported Copilot action.")
    for key in (("server",) if value["action"] == "discover" else ("server", "workspace", "tab") if value["action"] == "panes" else ("server", "workspace", "tab", "pane")):
        if not isinstance(value.get(key), str) or not re.fullmatch(r"[A-Za-z0-9_%:.-]{1,200}", value[key]):
            raise BridgeError("Invalid Herdr destination.")
    if value["action"] not in ("discover", "panes") and str(uuid.UUID(value["session"])) != value["session"]:
        raise BridgeError("Invalid Copilot conversation.")
    if value["action"] == "history" and (type(value.get("before")) is not int or not 0 < value["before"] < 100000000):
        raise BridgeError("Invalid history range.")
    if value["action"] == "send":
        text = value.get("text")
        if not isinstance(text, str) or not text.strip() or len(text.encode()) > 32768 or any(ord(c) < 32 and c not in "\n\t" or 127 <= ord(c) < 160 for c in text):
            raise BridgeError("Enter a message up to 32 KB without terminal control characters.")
    return value


def herdr(value, *args):
    binary = shutil.which("herdr", path=os.environ.get("PATH", "") + os.pathsep + os.pathsep.join([
        str(Path.home() / ".local/bin"), "/opt/homebrew/bin", "/usr/local/bin"]))
    if not binary:
        raise BridgeError("Install Herdr on this computer to connect Copilot.")
    # Never inherit the SSH process's pane or API socket from a different server.
    env = {k: v for k, v in os.environ.items() if not k.startswith("HERDR_")}
    result = subprocess.run([binary, "--session", value["server"], *args], env=env,
                            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, timeout=10)
    if result.returncode or len(result.stdout) > 1048576:
        raise BridgeError("Herdr couldn't confirm the action. Check the terminal before retrying.")
    response = json.loads(result.stdout)
    if "error" in response or not isinstance(response.get("result"), dict):
        raise BridgeError("Herdr couldn't confirm the action. Check the terminal before retrying.")
    return response["result"]


def validate(value, mutate=False):
    pane = herdr(value, "pane", "get", value["pane"]).get("pane", {})
    identity = pane.get("agent_session") or {}
    if (pane.get("pane_id"), pane.get("workspace_id"), pane.get("tab_id"), pane.get("agent")) != (
            value["pane"], value["workspace"], value["tab"], "copilot") or (
            identity.get("kind"), identity.get("agent"), identity.get("value")) != ("id", "copilot", value["session"]):
        raise BridgeError("Copilot's conversation changed. Reopen chat from the session list.")
    if mutate and pane.get("agent_status") in ("blocked", "waiting", "unknown"):
        raise BridgeError("Open the terminal to check Copilot's current prompt.")
    if value["action"] == "stop" and pane.get("agent_status") != "working":
        raise BridgeError("Copilot is no longer working.")


def visible(raw):
    if not isinstance(raw, dict) or raw.get("agentId") or raw.get("ephemeral"):
        return None
    data = raw.get("data")
    if not isinstance(data, dict):
        return None
    kind = raw.get("type")
    # Do not transmit system prompts, transformed user content, reasoning,
    # credentials, binary payloads, or background-agent conversations.
    fields = {
        "user.message": ("content",), "assistant.message": ("content",),
        "tool.execution_start": ("toolName", "arguments"),
        "tool.execution_complete": (), "assistant.turn_start": (),
        "session.idle": ("aborted",), "abort": (),
        "assistant.usage": ("inputTokens", "outputTokens", "cacheReadTokens"),
    }
    if kind not in fields or kind == "user.message" and data.get("source") not in (None, "user"):
        return None
    clean = {k: data[k] for k in fields[kind] if k in data}
    if kind == "tool.execution_complete":
        result = data.get("result") or {}
        error = data.get("error") or {}
        clean = {"result": {"content": result.get("content", "")}, "error": {"message": error.get("message", "")}}
    return {"type": kind, "timestamp": raw.get("timestamp"), "data": clean}


def log_path(value):
    root = Path(os.environ.get("COPILOT_HOME", str(Path.home() / ".copilot"))).resolve()
    path = root / "session-state" / value["session"] / "events.jsonl"
    # Session logs must be regular files within the configured Copilot root.
    if path.resolve() != path or not path.is_file():
        raise BridgeError("Copilot's transcript isn't available yet. Start it in Herdr with the Copilot integration installed.")
    return path


def emit(kind, entries, total, start=None):
    print(json.dumps({"type": kind, "source": "copilot", "entries": list(entries), "totalLines": total,
                      "startLine": start, "hasMore": start is not None and start > 0}), flush=True)


def transcript(value):
    path = log_path(value)
    file = None
    identity = None
    line = 0
    try:
        while True:
            stat = path.stat()
            if path.resolve() != path or stat.st_size > 536870912:
                raise BridgeError("Copilot's transcript is unavailable or too large.")
            reset = file is None or identity != (stat.st_dev, stat.st_ino) or stat.st_size < file.tell()
            if reset:
                if file:
                    file.close()
                file = path.open("rb")
                identity = (stat.st_dev, stat.st_ino)
                line = 0
            entries = deque()
            size = 0
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
                raw = None
                if not oversized:
                    try:
                        raw = json.loads(row)
                    except (ValueError, TypeError):
                        pass
                if line == 0 and (not isinstance(raw, dict) or raw.get("type") != "session.start" or raw.get("data", {}).get("sessionId") != value["session"]):
                    raise BridgeError("Copilot's transcript identity didn't match this session.")
                clean = visible(raw)
                if clean and (value["action"] != "history" or line < value["before"]):
                    entry = {"line": line, "raw": clean}
                    length = len(json.dumps(entry).encode())
                    if length <= 200000:
                        # Live appends are chunked, never dropped. Backlogs are
                        # bounded pages, recoverable with the history action.
                        if not reset and (size + length > 200000 or len(entries) >= 200):
                            emit("append", entries, line)
                            entries.clear(); size = 0
                        entries.append(entry); size += length
                        while size > 200000 or len(entries) > 200:
                            size -= len(json.dumps(entries.popleft()).encode())
                line += 1
            if reset or entries:
                kind = "older" if value["action"] == "history" else "backlog" if reset else "append"
                emit(kind, entries, line, entries[0]["line"] if entries else 0)
            if value["action"] == "history":
                return
            readable, _, _ = select.select([sys.stdin], [], [], .25)
            if readable and not os.read(sys.stdin.fileno(), 1):
                return
    finally:
        if file:
            file.close()


def main(encoded):
    try:
        value = request(encoded)
        if value["action"] in ("discover", "panes"):
            args = ["pane", "list"]
            if value["action"] == "panes":
                args += ["--workspace", value["workspace"]]
            panes = herdr(value, *args).get("panes", [])
            found = []
            for pane in panes:
                if pane.get("agent") != "copilot":
                    continue
                if value["action"] == "panes" and (pane.get("workspace_id"), pane.get("tab_id")) != (value["workspace"], value["tab"]):
                    continue
                identity = pane.get("agent_session") or {}
                found.append({"id": pane["pane_id"], "workspaceID": pane["workspace_id"], "tabID": pane["tab_id"],
                              "label": pane.get("label") or "Copilot", "title": pane.get("title"),
                              "agent": "copilot", "agentStatus": pane.get("agent_status"),
                              "cwd": pane.get("foreground_cwd") or pane.get("cwd"),
                              "sessionId": identity.get("value") if identity.get("kind") == "id" and identity.get("agent") == "copilot" else None})
            print(json.dumps({"panes": found}), flush=True)
            return
        validate(value, mutate=value["action"] in ("send", "stop"))
        if value["action"] in ("watch", "history"):
            transcript(value)
        else:
            # Exactly one mutation, to the explicit pane. No retry or fallback.
            if value["action"] == "send":
                herdr(value, "agent", "prompt", value["pane"], value["text"])
            else:
                herdr(value, "agent", "send-keys", value["pane"], "esc")
            print('{"ok":true}', flush=True)
    except BridgeError as error:
        print(json.dumps({"error": str(error)}), flush=True)
    except (ValueError, KeyError, TypeError, OSError, subprocess.SubprocessError):
        print(json.dumps({"error": "Copilot connection or delivery wasn't confirmed. Check the terminal and update Phren's chat bridge on this computer."}), flush=True)


if __name__ == "__main__":
    main(sys.argv[1])
