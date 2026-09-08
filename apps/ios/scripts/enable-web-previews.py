#!/usr/bin/env python3
"""Upgrade existing Phren iPhone keys to allow localhost web app forwards.

Run as the SSH user on the connected computer. All unrelated keys/options stay
unchanged. Keep the forced command and restrict flags; permit only loopback.
"""
import os
from pathlib import Path
import shutil
import tempfile
import time


def upgrade(text):
    old = 'permitopen="127.0.0.1:24543"'
    new = 'permitopen="127.0.0.1:*",permitopen="[::1]:*"'
    lines = []
    changed = 0
    for line in text.splitlines(keepends=True):
        fields = line.split()
        if (len(fields) == 4 and fields[-1] == "phren-iphone"
                and fields[1] == "ssh-ed25519"
                and fields[0].startswith("restrict,port-forwarding,")
                and 'command="/usr/bin/false"' in fields[0] and old in fields[0]):
            line = line.replace(old, new, 1)
            changed += 1
        lines.append(line)
    return "".join(lines), changed


def main():
    path = Path.home() / ".ssh" / "authorized_keys"
    before = path.read_text()
    after, count = upgrade(before)
    if count:
        backup = path.with_name(f"authorized_keys.phren-web-{time.time_ns()}.bak")
        shutil.copy2(path, backup)
        fd, temporary = tempfile.mkstemp(prefix=".phren-keys-", dir=path.parent)
        try:
            with os.fdopen(fd, "w") as file:
                file.write(after)
                file.flush()
                os.fsync(file.fileno())
            os.chmod(temporary, path.stat().st_mode & 0o777)
            if path.read_text() != before:
                raise RuntimeError("authorized_keys changed during the update; retry")
            os.replace(temporary, path)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)
    print(f"Updated {count} Phren iPhone key(s) for localhost web previews.")


if __name__ == "__main__":
    main()
