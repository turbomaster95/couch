#!/usr/bin/env python3

import pathlib
import sys

output = pathlib.Path(sys.argv[1])
assets = [
    ("index.html", pathlib.Path(sys.argv[2]), "text/html; charset=utf-8"),
    ("job.html", pathlib.Path(sys.argv[3]), "text/html; charset=utf-8"),
    ("app.js", pathlib.Path(sys.argv[4]), "application/javascript; charset=utf-8"),
    ("style.css", pathlib.Path(sys.argv[5]), "text/css; charset=utf-8"),
]

def c_bytes(data):
    return ", ".join(f"0x{byte:02x}" for byte in data) if data else "0"

lines = [
    '#include "embed.h"',
    "",
]

for index, (name, path, content_type) in enumerate(assets):
    data = path.read_bytes()
    lines.append(f"static const unsigned char asset_{index}[] = {{{c_bytes(data)}}};")

lines.extend(["", "const couch_web_asset_t couch_web_assets[] = {"])

for index, (name, path, content_type) in enumerate(assets):
    data = path.read_bytes()
    lines.append(f'    {{"/{name}", asset_{index}, {len(data)}, "{content_type}"}},')

lines.extend([
    "};",
    "",
    "const size_t couch_web_asset_count = sizeof(couch_web_assets) / sizeof(couch_web_assets[0]);",
    "",
])

output.parent.mkdir(parents=True, exist_ok=True)
output.write_text("\n".join(lines), encoding="utf-8")
