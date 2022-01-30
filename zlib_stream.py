import json
import zlib
import sys
import base64
import urllib.parse

with open(sys.argv[1], "r") as fd:
    har_data = json.load(fd)

for entry in har_data["log"]["entries"]:
    request_url = entry["request"]["url"]
    parsed = urllib.parse.urlparse(request_url)
    if parsed.scheme not in ("ws", "wss") or "etf" in request_url:
        continue
    print("found entry", request_url, file=sys.stderr)

    zlib_ctx = zlib.decompressobj()
    for message in entry["_webSocketMessages"]:
        if message["type"] == "send":
            print(message["data"], flush=True)
        elif message["type"] == "receive":
            decoded = base64.b64decode(message["data"].encode())
            decompressed = zlib_ctx.decompress(decoded)
            print(decompressed.decode(), flush=True)
