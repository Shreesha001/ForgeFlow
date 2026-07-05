"""A fake downstream service for the http-executor demo.

Fails the first two calls with 500, succeeds from the third on -- exactly how
a flaky dependency behaves, so you can watch ForgeFlow retry with backoff.
Run: python3 demo/flaky-service.py  (listens on 0.0.0.0:9000)
"""
from http.server import HTTPServer, BaseHTTPRequestHandler

count = 0


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        global count
        count += 1
        ok = count >= 3
        print(f"--> call #{count}: {'SUCCESS' if ok else 'FAILING (500)'}", flush=True)
        self.send_response(200 if ok else 500)
        self.end_headers()
        self.wfile.write(b"billed 42 customers" if ok else b"db locked, try later")

    def log_message(self, *args):
        pass  # keep output to our own prints


if __name__ == "__main__":
    print("flaky service listening on :9000 (fails twice, then succeeds)")
    HTTPServer(("0.0.0.0", 9000), Handler).serve_forever()
