"""A tiny local webhook catcher — stands in for Slack/PagerDuty while learning.

Run it in its own terminal:

    uv run python alert_listener.py

Then run `07_alerts.py` in another terminal and watch failure alerts arrive here.
In production you'd delete this and point the notification block at a real Slack
incoming webhook instead (see lesson 7).
"""

import json
from http.server import BaseHTTPRequestHandler, HTTPServer

PORT = 8077


class Handler(BaseHTTPRequestHandler):
    def do_POST(self) -> None:
        length = int(self.headers.get("Content-Length", 0))
        raw = self.rfile.read(length).decode()
        try:
            text = json.loads(raw).get("text", raw)
        except json.JSONDecodeError:
            text = raw
        print("\n" + "=" * 60)
        print("🔔 ALERT RECEIVED:")
        print(text)
        print("=" * 60, flush=True)
        self.send_response(200)
        self.end_headers()

    def log_message(self, *args: object) -> None:
        pass  # silence the default request logging


if __name__ == "__main__":
    print(f"listening for alerts on http://127.0.0.1:{PORT}/  (Ctrl-C to stop)")
    HTTPServer(("127.0.0.1", PORT), Handler).serve_forever()
