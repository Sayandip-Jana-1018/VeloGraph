"""Export contracts (roadmap P7.2): every serving file has the envelope; every file the website loads exists.

    cd python; uv run python ../scripts/validate_exports.py

The website parses each file with its own zod schema at build time; this check runs before that, on the data
folder alone, and names the file and the field that break the contract.
"""

from __future__ import annotations

import json
import re
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SERVING = ROOT / "data" / "serving"
DATA_TS = ROOT / "web" / "src" / "lib" / "data.ts"


def problems_of(path: Path) -> list[str]:
    """What is wrong with one export's envelope (an empty list when nothing is)."""
    try:
        doc = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        return [f"not readable JSON ({exc})"]
    if not isinstance(doc, dict) or not isinstance(doc.get("meta"), dict) or "data" not in doc:
        return ["no {meta, data} envelope"]
    meta, found = doc["meta"], []
    if not str(meta.get("schema", "")).startswith("velograph."):
        found.append("meta.schema does not start with 'velograph.'")
    if not isinstance(meta.get("version"), int):
        found.append("meta.version is not an integer")
    if not isinstance(meta.get("job"), str) or not meta["job"]:
        found.append("meta.job is missing")
    try:
        datetime.fromisoformat(str(meta.get("generatedAt")).replace("Z", "+00:00"))
    except ValueError:
        found.append("meta.generatedAt is not an ISO timestamp")
    return found


def required_files() -> list[str]:
    """The exports the website loads, read from its loaders: `load("file.json", ...)` in lib/data.ts."""
    return sorted(set(re.findall(r'load\(\s*[`"]([^`"$]+\.json)[`"]', DATA_TS.read_text(encoding="utf-8"))))


def main() -> int:
    exports = sorted(SERVING.rglob("*.json"))
    broken = {p.relative_to(SERVING).as_posix(): problems_of(p) for p in exports}
    broken = {name: issues for name, issues in broken.items() if issues}
    missing = [f for f in required_files() if not (SERVING / f).exists()]
    for name, issues in broken.items():
        print(f"BROKEN  {name}: {'; '.join(issues)}")
    for name in missing:
        print(f"MISSING {name} (loaded by the website)")
    print(
        f"{len(exports)} exports checked, {len(required_files())} required by the website: "
        f"{len(broken)} broken, {len(missing)} missing"
    )
    return 1 if broken or missing else 0


if __name__ == "__main__":
    sys.exit(main())
