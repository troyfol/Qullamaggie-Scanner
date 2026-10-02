"""
Colour-rule favorites (v8.0.1) — named single rules shared by every preset.

A favorite is a COPY of one rule taken when it was saved: editing that rule
afterwards does not change the favorite (save it again under the same name to
update it). Adding a favorite back into a rule list always makes a fresh copy
with a new id, so one favorite can sit in any number of presets independently.

Pure data + JSON, no Qt: the Color Rules dialog owns every prompt.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from .. import config
from . import coloring as C

log = logging.getLogger("scanner.gui")

_FILE_VERSION = 1


@dataclass
class Favorite:
    name: str
    rule: C.Rule
    saved_at: str = ""


def _favorites_path() -> Path:
    """Resolved at call time so tests that monkeypatch config.DATA_DIR are
    honoured."""
    return config.DATA_DIR / config.COLOR_RULE_FAVORITES_FILE


def _key(name: str) -> str:
    return (name or "").strip().casefold()


def sort_favorites(favs) -> list:
    return sorted(favs, key=lambda f: _key(f.name))


def load_favorites() -> list:
    """Every saved favorite, sorted by name. A missing or unreadable file is
    "no favorites"; one unreadable entry is dropped with a log line rather
    than costing the rest."""
    try:
        data = json.loads(_favorites_path().read_text(encoding="utf-8"))
    except FileNotFoundError:
        return []
    except (OSError, UnicodeDecodeError, ValueError) as exc:
        log.warning("colour favorites unreadable, ignoring them: %s", exc)
        return []
    items = data.get("favorites") if isinstance(data, dict) else None
    if not isinstance(items, list):
        return []
    out, seen = [], set()
    for item in items:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name") or "").strip()
        rule = C.Rule.from_dict(item.get("rule"))
        if not name or rule is None or _key(name) in seen:
            log.warning("colour favorites: skipped an unreadable entry: %r",
                        item.get("name"))
            continue
        seen.add(_key(name))
        out.append(Favorite(name, rule, str(item.get("saved_at") or "")))
    return sort_favorites(out)


def save_favorites(favs) -> bool:
    """Atomic write. Returns False (and logs) instead of raising."""
    payload = {
        "version": _FILE_VERSION,
        "rules_version": C.RULES_VERSION,
        "favorites": [{"name": f.name, "saved_at": f.saved_at,
                       "rule": f.rule.to_dict()}
                      for f in sort_favorites(favs)],
    }
    try:
        config.atomic_write_text(_favorites_path(),
                                 json.dumps(payload, indent=1))
        return True
    except OSError as exc:
        log.warning("colour favorites write failed: %s", exc)
        return False


def find(favs, name: str):
    """The favorite called `name` (case-insensitive), or None."""
    k = _key(name)
    return next((f for f in favs if _key(f.name) == k), None)


def upsert(favs, name: str, rule: C.Rule, *, now=None) -> list:
    """`favs` with `rule` saved as `name`, replacing a same-named one."""
    name = name.strip()
    if not name:
        raise ValueError("a favorite needs a name")
    stamp = (now or datetime.now().astimezone()).isoformat(timespec="seconds")
    kept = [f for f in favs if _key(f.name) != _key(name)]
    return sort_favorites(kept + [Favorite(name, rule.copy(), stamp)])


def rename(favs, old: str, new: str) -> list:
    new = new.strip()
    if not new:
        raise ValueError("a favorite needs a name")
    target = find(favs, old)
    if target is None:
        raise KeyError(old)
    clash = find(favs, new)
    if clash is not None and clash is not target:
        raise ValueError(f"a favorite called {clash.name!r} already exists")
    return sort_favorites(
        [Favorite(new, f.rule, f.saved_at) if f is target else f
         for f in favs])


def delete(favs, name: str) -> list:
    k = _key(name)
    return [f for f in favs if _key(f.name) != k]


def signature(rule: C.Rule) -> dict:
    """What a rule DOES — everything but its id, name and on/off switch — so
    adding a favorite already in the list can select it instead of adding a
    duplicate."""
    d = rule.to_dict()
    for k in ("id", "name", "enabled"):
        d.pop(k, None)
    return d


def instantiate(fav: Favorite) -> C.Rule:
    """A fresh rule from `fav`: new id, switched on, named after the
    favorite."""
    rule = fav.rule.copy()
    rule.id = C.Rule().id
    rule.enabled = True
    rule.name = fav.name
    return rule
