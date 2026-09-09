"""Contract tests for the read-only SSH progress reader; uses disposable logs."""
import json
import importlib.util
import os
from pathlib import Path
import select
import subprocess
import sys
import tempfile
import unittest
import uuid

SCRIPT = Path(__file__).resolve().parents[1] / "PhrenLive/Sources/PhrenLive/Resources/chat-progress.py"


class ProgressReaderTests(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory(prefix="phren-progress-")
        self.root = Path(self.folder.name)
        self.session = str(uuid.uuid4())
        self.path = self.root / ("sessions/2026/09/09/rollout-2026-09-09T00-00-00-" + self.session + ".jsonl")
        self.path.parent.mkdir(parents=True)
        self.process = None

    def tearDown(self):
        if self.process:
            self.process.stdin.close()
            self.process.wait(timeout=5)
            self.process.stdout.close()
            self.process.stderr.close()
        self.folder.cleanup()

    def append(self, row):
        with self.path.open("ab") as file:
            file.write(json.dumps(row).encode() + b"\n")

    def meta(self):
        self.append({"type": "session_meta", "payload": {"id": self.session}})

    def event(self, kind, **fields):
        self.append({"type": "event_msg", "payload": {"type": kind, **fields}})

    def start(self, source="codex"):
        env = dict(os.environ, CODEX_HOME=str(self.root), CLAUDE_CONFIG_DIR=str(self.root))
        self.process = subprocess.Popen([sys.executable, "-u", str(SCRIPT), source, self.session],
                                        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)

    def frame(self):
        ready, _, _ = select.select([self.process.stdout], [], [], 5)
        self.assertTrue(ready, "Progress update timed out")
        row = self.process.stdout.readline()
        self.assertTrue(row, "Progress reader closed early")
        return json.loads(row)

    def test_filters_content_and_streams_complete_rows_and_turn_state(self):
        self.meta()
        self.append({"type": "response_item", "payload": {"type": "message", "role": "assistant", "content": "private content"}})
        self.event("agent_reasoning", text="private reasoning")
        self.event("task_started", started_at=1234, prompt="private prompt")
        self.start()
        first = self.frame()
        self.assertEqual(first["type"], "backlog")
        self.assertEqual(first["totalLines"], 4)
        self.assertNotIn("private", json.dumps(first))
        self.assertEqual(first["entries"][0]["line"], 3)
        row = json.dumps({"type": "event_msg", "payload": {"type": "token_count", "info": {
            "last_token_usage": {"input_tokens": 123, "output_tokens": 17, "cached_input_tokens": 100, "private": "secret"}}}}).encode()
        with self.path.open("ab") as file:
            file.write(row[:30])
        self.assertFalse(select.select([self.process.stdout], [], [], 1.2)[0])
        with self.path.open("ab") as file:
            file.write(row[30:] + b"\n")
        update = self.frame()
        self.assertEqual(update["type"], "append")
        self.assertEqual(update["entries"][0]["line"], 4)
        self.assertNotIn("private", json.dumps(update))
        self.assertEqual(update["entries"][0]["raw"]["payload"]["info"]["last_token_usage"]["output_tokens"], 17)
        self.event("task_complete", completed_at=1244, last_agent_message="private reply")
        self.assertNotIn("private", json.dumps(self.frame()))

    def test_truncation_resets_snapshot_and_stdin_eof_ends_reader(self):
        self.meta()
        self.event("token_count", info={"last_token_usage": {"input_tokens": 200, "output_tokens": 50}})
        self.start()
        self.assertEqual(self.frame()["totalLines"], 2)
        self.path.write_bytes(b"")
        self.meta()
        frame = self.frame()
        self.assertEqual(frame["type"], "backlog")
        self.assertEqual(frame["entries"], [])
        self.process.stdin.close()
        self.assertEqual(self.process.wait(timeout=5), 0)

    def test_wrong_session_is_rejected(self):
        self.append({"type": "session_meta", "payload": {"id": str(uuid.uuid4())}})
        self.start()
        self.assertEqual(self.process.wait(timeout=5), 1)
        self.assertEqual(self.process.stdout.read(), b"")

    def test_ambiguous_files_are_rejected(self):
        self.meta()
        self.path.with_name("rollout-other-" + self.session + ".jsonl").write_bytes(self.path.read_bytes())
        self.start()
        self.assertEqual(self.process.wait(timeout=5), 1)
        self.assertEqual(self.process.stdout.read(), b"")

    def test_forced_command_rejects_shell_and_malformed_requests(self):
        for command in ("", "sh", "touch /tmp/unwanted", "phren-chat-progress codex ../../etc/passwd",
                        "phren-chat-progress codex " + self.session + "; echo nope",
                        "phren-chat-progress codex " + self.session + " extra"):
            result = subprocess.run([sys.executable, str(SCRIPT)], env=dict(os.environ, SSH_ORIGINAL_COMMAND=command),
                                    capture_output=True, timeout=5)
            self.assertNotEqual(result.returncode, 0)
            self.assertEqual(result.stdout, b"")

    def test_key_upgrade_preserves_restrictions_and_other_keys(self):
        spec = importlib.util.spec_from_file_location("setup", Path(__file__).with_name("enable-chat-progress.py"))
        setup = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(setup)
        old = 'restrict,port-forwarding,permitopen="127.0.0.1:*",permitopen="[::1]:*",command="/usr/bin/false" ssh-ed25519 AAAA phren-iphone\n'
        unrelated = old.replace("phren-iphone", "another-device")
        after, count = setup.upgrade(old + unrelated)
        self.assertEqual(count, 1)
        self.assertIn(unrelated, after)
        self.assertIn(old.split('command=')[0], after)
        self.assertIn(setup.COMMAND, after)
        self.assertEqual(setup.upgrade(after), (after, 0))

    def test_claude_counters_exclude_sidechains_and_message_content(self):
        self.path = self.root / ("projects/test/" + self.session + ".jsonl")
        self.path.parent.mkdir(parents=True)
        self.append({"type": "assistant", "sessionId": self.session, "message": {"role": "assistant", "content": "private reply",
            "usage": {"input_tokens": 12, "output_tokens": 7, "cache_read_input_tokens": 5}}})
        self.append({"type": "assistant", "isSidechain": True, "message": {"role": "assistant", "usage": {"output_tokens": 999}}})
        self.start("claude")
        frame = self.frame()
        self.assertEqual(len(frame["entries"]), 1)
        self.assertNotIn("private", json.dumps(frame))
        self.assertEqual(frame["entries"][0]["raw"]["message"]["usage"]["output_tokens"], 7)


if __name__ == "__main__":
    unittest.main()
