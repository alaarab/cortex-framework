"""Copilot bridge contracts: disposable logs and mocked Herdr, never live input."""
import base64
from contextlib import redirect_stdout
import importlib.util
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

SCRIPT = Path(__file__).resolve().parents[1] / "PhrenLive/Sources/PhrenLive/Resources/copilot-chat.py"
spec = importlib.util.spec_from_file_location("bridge", SCRIPT)
bridge = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bridge)
SESSION = "00000000-0000-0000-0000-000000000023"


class CopilotBridgeTests(unittest.TestCase):
    def setUp(self):
        self.request = dict(action="send", server="work", workspace="w1", tab="w1:t2", pane="w1:p3", session=SESSION, text="/model next")
        self.pane = dict(pane_id="w1:p3", workspace_id="w1", tab_id="w1:t2", agent="copilot", agent_status="idle",
                         agent_session=dict(kind="id", agent="copilot", value=SESSION))

    def run_request(self, request):
        output = io.StringIO()
        with redirect_stdout(output):
            bridge.main(base64.b64encode(json.dumps(request).encode()).decode())
        return json.loads(output.getvalue())

    def test_explicit_pane_and_session_before_one_mutation(self):
        with patch.object(bridge, "herdr", side_effect=[{"pane": self.pane}, {}]) as cli:
            self.assertEqual(self.run_request(self.request), {"ok": True})
            self.assertEqual(cli.call_count, 2)
            self.assertEqual(cli.call_args.args[1:], ("agent", "prompt", "w1:p3", "/model next"))
            self.assertEqual(cli.call_args.args[0]["server"], "work")
        for field, replacement in [("tab_id", "w1:t9"), ("agent", "codex"), ("agent_status", "blocked"), ("agent_session", {})]:
            with patch.object(bridge, "herdr", return_value={"pane": {**self.pane, field: replacement}}) as cli:
                self.assertIn("error", self.run_request(self.request))
                self.assertEqual(cli.call_count, 1)

    def test_timeout_never_retries_and_control_characters_never_reach_herdr(self):
        with patch.object(bridge, "herdr", side_effect=[{"pane": self.pane}, OSError("lost reply")]) as cli:
            self.assertIn("error", self.run_request(self.request))
            self.assertEqual(cli.call_count, 2)
        for changes in [dict(text="\x1b[2J"), dict(session="../../secret"), dict(server="work;sh"), dict(action="shell"), dict(text="x" * 32769)]:
            with patch.object(bridge, "herdr") as cli:
                self.assertIn("error", self.run_request({**self.request, **changes}))
                cli.assert_not_called()

    def test_shell_metacharacters_remain_literal_arguments(self):
        response = type("Result", (), dict(returncode=0, stdout=b'{"result":{}}'))()
        with patch.object(bridge.shutil, "which", return_value="/opt/homebrew/bin/herdr"), patch.object(bridge.subprocess, "run", return_value=response) as run:
            text = '/custom `literal` $(literal) "quoted"\nsecond line'
            bridge.herdr(self.request, "agent", "prompt", "w1:p3", text)
            self.assertEqual(run.call_args.args[0], ["/opt/homebrew/bin/herdr", "--session", "work", "agent", "prompt", "w1:p3", text])
            self.assertNotIn("shell", run.call_args.kwargs)
            self.assertFalse(any(k.startswith("HERDR_") for k in run.call_args.kwargs["env"]))

    def test_discovery_uses_exact_tab_and_copilot_session_only(self):
        other = {**self.pane, "pane_id": "w1:p4", "tab_id": "w1:t9"}
        with patch.object(bridge, "herdr", return_value={"panes": [self.pane, other]}):
            result = self.run_request({**self.request, "action": "panes"})
            self.assertEqual([p["id"] for p in result["panes"]], ["w1:p3"])
            self.assertEqual(result["panes"][0]["sessionId"], SESSION)

    def test_history_is_bounded_and_excludes_unfinished_rows_and_private_fields(self):
        with tempfile.TemporaryDirectory() as tmp, patch.dict(os.environ, COPILOT_HOME=tmp):
            file = Path(tmp) / "session-state" / SESSION / "events.jsonl"
            file.parent.mkdir(parents=True)
            rows = [{"type": "session.start", "data": {"sessionId": SESSION}}]
            rows += [{"type": "assistant.message", "data": {"content": f"Message {n}", "reasoningText": "private"}} for n in range(250)]
            file.write_text("".join(json.dumps(r) + "\n" for r in rows) + '{"type":')
            with patch.object(bridge, "herdr", return_value={"pane": self.pane}):
                result = self.run_request({**self.request, "action": "history", "before": 240})
            self.assertEqual(result["type"], "older")
            self.assertEqual(result["totalLines"], 251)
            self.assertEqual(len(result["entries"]), 200)
            self.assertEqual(result["entries"][-1]["line"], 239)
            self.assertTrue(result["hasMore"])
            self.assertNotIn("private", json.dumps(result))
            rows[0]["data"]["sessionId"] = "different"
            file.write_text(json.dumps(rows[0]) + "\n")
            with patch.object(bridge, "herdr", return_value={"pane": self.pane}):
                self.assertIn("error", self.run_request({**self.request, "action": "history", "before": 240}))

    def test_symlink_logs_are_rejected(self):
        with tempfile.TemporaryDirectory() as tmp, patch.dict(os.environ, COPILOT_HOME=tmp):
            file = Path(tmp) / "session-state" / SESSION / "events.jsonl"
            file.parent.mkdir(parents=True)
            outside = Path(tmp) / "other.jsonl"
            outside.write_text("secret")
            file.symlink_to(outside)
            with self.assertRaises(ValueError):
                bridge.log_path(self.request)


if __name__ == "__main__":
    unittest.main()
