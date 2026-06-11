"""One-off inspection of Zacks's obj_data JSON blob structure.
Brace-walks the assignment, parses the JSON, prints table shapes."""
import json
import re

with open(r"tests/_zacks_aapl_probe.html", "r", encoding="utf-8") as f:
    html = f.read()

m = re.search(r"document\.obj_data\s*=\s*\{", html)
if not m:
    raise SystemExit("no obj_data assignment found")

start = m.end() - 1
depth = 0
in_str = False
escape = False
end = None
for i in range(start, len(html)):
    c = html[i]
    if escape:
        escape = False
        continue
    if in_str:
        if c == "\\":
            escape = True
        elif c == '"':
            in_str = False
        continue
    if c == '"':
        in_str = True
    elif c == "{":
        depth += 1
    elif c == "}":
        depth -= 1
        if depth == 0:
            end = i + 1
            break

if end is None:
    raise SystemExit("never closed")

raw = html[start:end]
print(f"extracted obj_data: {len(raw)} chars")

data = json.loads(raw)
print("\ntop-level keys:")
for k, v in data.items():
    n = len(v) if isinstance(v, list) else "?"
    if isinstance(v, list):
        print(f"  {k!r}  -> list[{n}]")
    else:
        print(f"  {k!r}  -> {type(v).__name__}")

print("\n=== EPS table sample ===")
eps = data.get("earnings_announcements_earnings_table", [])
print(f"rows: {len(eps)}")
for r in eps[:3]:
    print(f"  cols={len(r)}: {r}")

print("\n=== Sales table sample ===")
sales = data.get("earnings_announcements_sales_table", [])
print(f"rows: {len(sales)}")
for r in sales[:3]:
    print(f"  cols={len(r)}: {r}")
