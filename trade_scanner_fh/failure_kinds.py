"""Shared classification of per-ticker fill-failure kinds (v6.3.2).

Each earnings client publishes its own ``FAIL_*`` sentinels, but they agree on
the spelling wherever they agree on the meaning — ``network`` means the same
thing to finviz, finnhub and zacks. That makes one taxonomy enough for all
three, and lets the failure dialog be written once and parameterised by source
rather than three times.

The classification exists to answer one question the GUI has to get right:
**is it safe to add this ticker to the source's skip list?**

  ``PERMANENT``  a property of the TICKER. It failed because this source does
                 not cover it, or never will. Safe to skip-list; several of
                 these are already auto-added by the fill itself.
  ``TRANSIENT``  a property of the MOMENT. Rate limits, timeouts, 5xx. The
                 same ticker will very likely succeed on the next run, so
                 skip-listing it silently removes coverage you still have.
  ``UPSTREAM``   a property of the SOURCE, and the dangerous group. A parse
                 error means the page format changed; a block means we were
                 challenged. Neither is the ticker's fault.

That last group is why this module exists rather than a flat list. The
parse-failure spike alarm (``config.PARSE_SPIKE_*``) halts a run at >=40%
parse errors and deliberately blacklists NOTHING, precisely because a format
break is upstream's fault. A selective-add UI that offered ``parse_error`` as
an ordinary choice would hand the user a one-click way to undo that guarantee
and permanently skip-list hundreds of perfectly good tickers. The GUI defaults
every non-PERMANENT group off and confirms separately before adding UPSTREAM
kinds — the user can still do it, but never by reflex.
"""
from __future__ import annotations

PERMANENT = "permanent"
TRANSIENT = "transient"
UPSTREAM = "upstream"
NEVER_SKIP = "never_skip"

# A property of the ticker: this source does not cover it.
#   not_found        zacks  — no obj_data / no EPS+revenue tables
#   empty            finviz, finnhub — 200 with no earnings payload (finviz
#                    also maps its 404 here deliberately, so a dead symbol
#                    cannot trip the consecutive-block streak)
#   forbidden        finviz, finnhub — 403, symbol outside the plan
#   rejected_symbol  zacks  — refused by our own URL allowlist before the
#                    request was ever made
#   http_4xx         zacks  — 4xx other than 429; gone or denied
_PERMANENT = frozenset({
    "not_found", "empty", "forbidden", "rejected_symbol", "http_4xx",
})

# A property of the moment: retry later, do not skip-list.
_TRANSIENT = frozenset({
    "network", "server_error", "rate_limited", "http_5xx", "http_429",
})

# A property of the source. Skip-listing these blames the ticker for our
# problem, and at scale it is self-inflicted data loss.
#   parse_error  all    — the payload was found but could not be read
#   blocked      zacks, finviz — bot challenge / Imperva interstitial
#   oversized    zacks  — body past the sanity cap, almost always a block page
#   too_large    finviz — same
_UPSTREAM = frozenset({"parse_error", "blocked", "oversized", "too_large"})

# Not about a ticker at all. A revoked API key fails every symbol equally, so
# offering it as a skip-list choice could empty a universe on one bad key.
_NEVER_SKIP = frozenset({"auth"})


def classify(kind: str) -> str:
    """Group one failure kind. Anything unrecognised is treated as UPSTREAM —
    the conservative default, since a kind we cannot reason about should not
    be offered as a safe skip."""
    k = (kind or "").strip().lower()
    if k in _PERMANENT:
        return PERMANENT
    if k in _TRANSIENT:
        return TRANSIENT
    if k in _NEVER_SKIP:
        return NEVER_SKIP
    return UPSTREAM


def is_skippable(kind: str) -> bool:
    """False only for kinds that must never reach a skip list at all."""
    return classify(kind) != NEVER_SKIP


def default_checked(kind: str) -> bool:
    """Whether the dialog pre-selects this kind. Only PERMANENT."""
    return classify(kind) == PERMANENT


# Display order and human wording, per group.
GROUP_ORDER: tuple = (PERMANENT, TRANSIENT, UPSTREAM, NEVER_SKIP)

GROUP_LABEL: dict = {
    PERMANENT: "Permanent — the source does not cover this ticker",
    TRANSIENT: "Transient — likely to succeed on the next run",
    UPSTREAM: "Upstream fault — not the ticker's problem",
    NEVER_SKIP: "Not ticker-specific — cannot be skip-listed",
}

GROUP_HINT: dict = {
    PERMANENT: "Safe to add. Several of these are auto-added by the fill "
               "already.",
    TRANSIENT: "Adding these removes coverage you still have. Off by default.",
    UPSTREAM: "A parse error or block means the SOURCE changed or challenged "
              "us. Adding these at scale is self-inflicted data loss — the "
              "parse-spike alarm exists to prevent exactly that.",
    NEVER_SKIP: "A revoked key fails every symbol equally; skip-listing would "
                "empty the universe.",
}

# Per-kind one-liners. Keys are the shared sentinel strings.
KIND_HELP: dict = {
    "not_found": "No earnings tables on the page — Zacks does not cover it.",
    "empty": "200 OK with no earnings payload (ETF / fund / pre-IPO), or a "
             "404 for a delisted symbol.",
    "forbidden": "403 — outside the account's plan or access denied.",
    "rejected_symbol": "Refused by our own URL allowlist; never requested.",
    "http_4xx": "4xx other than 429 — gone or denied.",
    "network": "Timeout / DNS / connection reset.",
    "server_error": "5xx — the source's own failure.",
    "rate_limited": "429 — throttled. Raise the per-request delay.",
    "http_5xx": "5xx — the source's own failure.",
    "http_429": "429 — throttled. Raise the per-request delay.",
    "parse_error": "The payload was found but could not be read — the page "
                   "format has probably changed.",
    "blocked": "Bot challenge / interstitial. Refresh cookies.",
    "oversized": "Response past the sanity cap — almost always a block page.",
    "too_large": "Response past the sanity cap — almost always a block page.",
    "auth": "401 — bad or revoked API key. Fatal for the whole run.",
    "unknown": "Unclassified failure.",
}
