"""
KoNiS Flash Dashboard — Local web server.

Serves the trading dashboard HTML and proxies bot commands via Redis.
The bot publishes its state to Redis (konis:tui:<bot_id>), and this server
reads it and serves it as JSON to the dashboard frontend.

Requirements:
  pip install redis
  Redis running on localhost:6379 (or set REDIS_HOST/REDIS_PORT env vars)

Usage:
  python dashboard/dashboard-server.py
  Then open http://localhost:8901 in your browser.
"""

import json
import os
from http.server import HTTPServer, BaseHTTPRequestHandler
from pathlib import Path
from urllib.parse import urlparse, parse_qs

import redis

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
API_PORT = int(os.getenv("WEB_DASHBOARD_PORT", "8901"))
DASHBOARD_DIR = Path(__file__).resolve().parent

r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


class DashboardHandler(BaseHTTPRequestHandler):
    def _send_json(self, data: str, status: int = 200):
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write((data or "{}").encode())

    def _serve_html(self):
        html_path = DASHBOARD_DIR / "konis-trading.html"
        if not html_path.exists():
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b"Dashboard HTML not found")
            return
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()
        self.wfile.write(html_path.read_bytes())

    def do_GET(self):
        parsed = urlparse(self.path)

        # Serve dashboard HTML at root
        if parsed.path in ("/", "/index.html", "/konis-trading.html"):
            self._serve_html()
            return

        if parsed.path == "/api/dashboard":
            qs = parse_qs(parsed.query)
            bot = qs.get("bot", [None])[0]
            if bot:
                val = r.get(f"konis:tui:{bot}")
                self._send_json(val)
            else:
                keys = r.keys("konis:tui:*")
                if not keys:
                    self._send_json("[]")
                    return
                bots = []
                for key in keys:
                    val = r.get(key)
                    if val:
                        try:
                            bots.append(json.loads(val))
                        except (json.JSONDecodeError, TypeError):
                            pass
                self._send_json(json.dumps(bots, default=str))

        elif parsed.path == "/api/dashboard/exit-status":
            qs = parse_qs(parsed.query)
            bot = qs.get("bot", [None])[0]
            if not bot:
                self._send_json('{"error":"missing bot param"}', 400)
                return
            result = r.get(f"konis:cmd_result:{bot}")
            self._send_json(result or '{"status":"pending"}')
        else:
            self.send_response(404)
            self.end_headers()

    def _get_targets(self, qs=None):
        bot = qs.get("bot", [None])[0] if qs else None
        if bot:
            return [bot]
        keys = r.keys("konis:tui:*")
        return [k.split(":")[-1] for k in keys] if keys else []

    def _read_body(self):
        length = int(self.headers.get("Content-Length", 0))
        if length == 0:
            return {}
        return json.loads(self.rfile.read(length))

    def do_POST(self):
        parsed = urlparse(self.path)
        qs = parse_qs(parsed.query)

        if parsed.path == "/api/dashboard/exit-all":
            targets = self._get_targets(qs)
            if not targets:
                self._send_json('{"error":"no bots found"}', 404)
                return
            for t in targets:
                r.set(f"konis:cmd:{t}", "EXIT_ALL", ex=30)
                r.delete(f"konis:cmd_result:{t}")
            self._send_json(json.dumps({"ok": True, "targets": targets}))

        elif parsed.path in (
                "/api/dashboard/loosen-tp", "/api/dashboard/loosen-sl",
                "/api/dashboard/tighten-tp", "/api/dashboard/tighten-sl"):
            targets = self._get_targets(qs)
            if not targets:
                self._send_json('{"error":"no bots found"}', 404)
                return
            _action = "LOOSEN" if "loosen" in parsed.path else "TIGHTEN"
            cmd = f"{_action}_TP" if parsed.path.endswith("tp") else f"{_action}_SL"
            for t in targets:
                r.set(f"konis:cmd:{t}", cmd, ex=30)
                r.delete(f"konis:cmd_result:{t}")
            self._send_json(json.dumps({"ok": True, "targets": targets, "cmd": cmd}))

        elif parsed.path == "/api/dashboard/manual-entry":
            body = self._read_body()
            cmd = body.get("cmd", "")
            if not cmd:
                self._send_json('{"error":"missing cmd"}', 400)
                return
            targets = self._get_targets(qs)
            if not targets:
                self._send_json('{"error":"no bots found"}', 404)
                return
            for t in targets:
                r.set(f"konis:cmd:{t}", cmd, ex=30)
                r.delete(f"konis:cmd_result:{t}")
            self._send_json(json.dumps({"ok": True, "targets": targets, "cmd": cmd}))

        elif parsed.path == "/api/dashboard/set-config":
            body = self._read_body()
            cmd = body.get("cmd", "")
            if not cmd:
                self._send_json('{"error":"missing cmd"}', 400)
                return
            targets = self._get_targets(qs)
            if not targets:
                self._send_json('{"error":"no bots found"}', 404)
                return
            for t in targets:
                r.set(f"konis:cmd:{t}", cmd, ex=30)
                r.delete(f"konis:cmd_result:{t}")
            self._send_json(json.dumps({"ok": True, "targets": targets, "cmd": cmd}))

        else:
            self.send_response(404)
            self.end_headers()

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def log_message(self, format, *args):
        pass


if __name__ == "__main__":
    server = HTTPServer(("0.0.0.0", API_PORT), DashboardHandler)
    print(f"KoNiS Flash Dashboard")
    print(f"  URL:   http://localhost:{API_PORT}")
    print(f"  Redis: {REDIS_HOST}:{REDIS_PORT}")
    print(f"  Press Ctrl+C to stop")
    server.serve_forever()
