#!/usr/bin/env python3
"""Install the counters-only SSH reader and upgrade existing Phren device keys."""
import argparse
import os
from pathlib import Path
import re
import shutil
import tempfile
import time

COMMAND = 'command="python3 ~/.local/share/phren/chat-progress.py"'


def upgrade(text):
    lines, changed = [], 0
    for line in text.splitlines(keepends=True):
        # Deliberately touch only the old, restricted, Phren-labelled key format.
        fields = line.split()
        if (len(fields) == 4 and fields[-1] == "phren-iphone" and fields[1] == "ssh-ed25519"
                and fields[0].startswith("restrict,port-forwarding,")
                and 'command="/usr/bin/false"' in fields[0]
                and re.search(r'permitopen="127\.0\.0\.1:(?:24543|\*)"', fields[0])):
            line = line.replace('command="/usr/bin/false"', COMMAND, 1)
            changed += 1
        lines.append(line)
    return "".join(lines), changed


def atomic_write(path, data, mode):
    fd, temporary = tempfile.mkstemp(prefix=".phren-progress-", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as file:
            file.write(data)
            file.flush()
            os.fsync(file.fileno())
        os.chmod(temporary, mode)
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--reader", type=Path, default=Path(__file__).resolve().parents[1] / "PhrenLive/Sources/PhrenLive/Resources/chat-progress.py")
    args = parser.parse_args()
    reader = args.reader.read_bytes()
    compile(reader, "chat-progress.py", "exec")
    target = Path.home() / ".local/share/phren/chat-progress.py"
    target.parent.mkdir(parents=True, exist_ok=True)
    atomic_write(target, reader, 0o700)
    path = Path.home() / ".ssh/authorized_keys"
    count = 0
    if path.exists():
        before = path.read_text()
        after, count = upgrade(before)
        if count:
            shutil.copy2(path, path.with_name(f"authorized_keys.phren-progress-{time.time_ns()}.bak"))
            if path.read_text() != before:
                raise RuntimeError("authorized_keys changed; retry the update")
            atomic_write(path, after.encode(), path.stat().st_mode & 0o777)
    print(f"Installed the chat progress reader; updated {count} Phren iPhone key(s).")


if __name__ == "__main__":
    main()
