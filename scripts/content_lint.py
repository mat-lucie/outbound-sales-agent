"""Content linter for prospect-facing copy.

Exposes ``lint_content(messages, emails, claims) -> list[Finding]`` and a CLI
entry point that prints findings and exits 1 on any BLOCK-severity finding.

Rules
-----
1. **claim-gate**
   Quantified-outcome patterns in prospect-facing strings that match no
   registry entry → BLOCK.  Registry entries with ``status: needs_evidence``
   that are found in content → WARN.

2. **banned-phrases**
   Built-in editorial bans (``muchas ganas``) plus any operator-defined
   patterns from the registry's optional ``banned_phrases`` key (canonical
   use: misspellings of the operator's product name) → BLOCK.

3. **worst-case render**
   Every template rendered via ``personalize()`` with empty personalization
   data must have no surviving ``[...]``-style tokens → BLOCK.

4. **register-mixing (ES only, conservative)**
   Within one persona's sequence, co-occurrence of explicit informal markers
   (``tú``, `` ti ``, ``tienes``, ``quieres``, ``tu planta``) and explicit
   formal markers (``usted``, ``le explico``, ``su planta``) → WARN.

5. **CTA presence**
   Every dm/email body must contain a ``?`` or a known CTA keyword
   (``agendar``, ``demo``, ``espacios``, ``time slots``, ``call``) → WARN.

6. **cold-email site link**
   ``email1`` body must contain an ``href=`` attribute (a site link). Operators
   are expected to link to their own domain. → BLOCK when no href is found.

CLI usage
---------
    python3 scripts/content_lint.py

Exits 0 if no BLOCK findings; exits 1 if any BLOCK finding is present.
WARNs are printed but do not affect the exit code.
"""

from __future__ import annotations

import json
import re
import sys
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

# Ensure the project root (parent of scripts/) is on sys.path so that
# ``from models.campaign import ...`` works whether this module is invoked
# as ``python3 scripts/content_lint.py`` (which adds scripts/ to sys.path)
# or as ``python3 -m scripts.content_lint`` (which has the root already).
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from workflows.email_compliance import html_to_text  # noqa: E402

# ---------------------------------------------------------------------------
# Public types
# ---------------------------------------------------------------------------


class Severity(Enum):
    BLOCK = "BLOCK"
    WARN = "WARN"


@dataclass
class Finding:
    severity: Severity
    rule: str
    location: str
    detail: str

    def __str__(self) -> str:
        return f"[{self.severity.value}] {self.rule} @ {self.location}: {self.detail}"


# ---------------------------------------------------------------------------
# Paths / helpers
# ---------------------------------------------------------------------------

CONTENT_DIR = Path(__file__).resolve().parent.parent / "content"


def _load_default_messages() -> dict:
    with open(CONTENT_DIR / "messages.json") as f:
        return json.load(f)


def _load_default_emails() -> dict:
    with open(CONTENT_DIR / "emails.json") as f:
        return json.load(f)


def _load_default_claims() -> dict:
    with open(CONTENT_DIR / "claims.json") as f:
        return json.load(f)


def _iter_strings(data: object, path: str = "") -> list[tuple[str, str]]:
    """Recursively yield (path, text) for every string leaf."""
    results: list[tuple[str, str]] = []
    if isinstance(data, dict):
        for k, v in data.items():
            results.extend(_iter_strings(v, f"{path}.{k}" if path else k))
    elif isinstance(data, list):
        for i, v in enumerate(data):
            results.extend(_iter_strings(v, f"{path}[{i}]"))
    elif isinstance(data, str):
        results.append((path, data))
    return results


# ---------------------------------------------------------------------------
# Rule 1: claim-gate
# ---------------------------------------------------------------------------

# Patterns that signal a quantified outcome/stat claim.
# Matched against every prospect-facing string.
_CLAIM_GATE_PATTERNS: list[re.Pattern[str]] = [
    re.compile(r"\d+[-–]\d+\s*(puntos|points|pp)\b", re.IGNORECASE),
    re.compile(r"\bROI\b"),
    re.compile(r"\bEBITDA\b", re.IGNORECASE),
    re.compile(r"\b\d+\s*(puntos|points)\b.*\b(trimestre|quarter)\b", re.IGNORECASE),
    re.compile(r"\bsemanas?,\s*no\s*meses?\b", re.IGNORECASE),
    re.compile(r"\bweeks?,\s*not\s*months?\b", re.IGNORECASE),
    re.compile(r"\bsemanas?\s*[,\.]\s*n[aã]o\s*meses?\b", re.IGNORECASE),
    re.compile(r"~?\d+\s*%\s*(of|del|do|de)\b", re.IGNORECASE),
    re.compile(r"\bOTIF\s*[+\-±]\s*\d+", re.IGNORECASE),
]


def _build_registry_fragments(claims: dict) -> tuple[list[str], list[str]]:
    """Return (demoable_fragments, needs_evidence_fragments) from the registry.

    Each fragment is a meaningful keyword extracted from the claim text.
    For the claim-gate matching, we use short anchor tokens (3+ char words)
    from each claim variant so that "ROI" matches "roi-weeks" even when the
    surrounding phrasing differs.
    """
    # Stop words to skip when building keyword anchors
    _STOP = {
        "en", "de", "el", "la", "los", "las", "un", "una", "y", "o",
        "a", "al", "del", "que", "se", "es", "no", "su", "sus",
        "in", "of", "the", "an", "and", "or", "to", "for",
        "with", "not",
        "em", "do", "da", "os", "as", "um", "e",
    }
    demoable: list[str] = []
    needs_ev: list[str] = []
    for entry in claims.get("claims", []):
        raw = entry.get("claim", "")
        # Split on " / " separators common in the registry format.
        parts = [p.strip() for p in re.split(r"\s*/\s*", raw) if p.strip()]
        for part in parts:
            # Extract anchor keywords (3+ chars, not stop words)
            words = re.findall(r"[a-záéíóúüñãâêôàèìòùç]+", part.lower())
            anchors = [w for w in words if len(w) >= 3 and w not in _STOP]
            for anchor in anchors:
                if entry.get("status") == "needs_evidence":
                    needs_ev.append(anchor)
                else:
                    demoable.append(anchor)
    return demoable, needs_ev


def _claim_gate_rule(
    text: str,
    location: str,
    demoable_fragments: list[str],
    needs_evidence_fragments: list[str],
) -> list[Finding]:
    """Check for quantified outcome claims not covered by the registry.

    Fragment matching uses keyword anchors extracted at registry-build time.
    A claim pattern match is considered "covered" if any registry keyword
    anchor appears in the text (demoable = skip; needs_evidence = WARN).
    If no anchor from either set matches, the claim is unregistered → BLOCK.
    """
    findings: list[Finding] = []
    text_lower = text.lower()

    for pat in _CLAIM_GATE_PATTERNS:
        m = pat.search(text)
        if not m:
            continue
        snippet = m.group(0)

        in_needs_evidence = any(frag in text_lower for frag in needs_evidence_fragments)
        in_demoable = any(frag in text_lower for frag in demoable_fragments)

        if in_demoable:
            # Covered by a demoable registry entry — no finding.
            continue
        elif in_needs_evidence:
            findings.append(Finding(
                severity=Severity.WARN,
                rule="claim-gate",
                location=location,
                detail=(
                    f"Claim matched pattern {pat.pattern!r} with registered "
                    f"status=needs_evidence near: {snippet!r}. "
                    "Evidence must be sourced before this copy can ship."
                ),
            ))
        else:
            findings.append(Finding(
                severity=Severity.BLOCK,
                rule="claim-gate",
                location=location,
                detail=(
                    f"Quantified outcome pattern {pat.pattern!r} matched "
                    f"near {snippet!r} but no registry entry covers it. "
                    "Either add evidence to claims.json or remove the claim."
                ),
            ))
    return findings


# ---------------------------------------------------------------------------
# Rule 2: banned-phrases
# ---------------------------------------------------------------------------

# Built-in defaults: editorial phrases that read as over-eager in cold ES
# outreach. Operators extend the list per content pack via the optional
# ``banned_phrases`` key in claims.json — the canonical use case is banning
# misspellings of their own product name (see examples/acme/content/claims.json).
_DEFAULT_BANNED_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    ("muchas ganas", re.compile(r"muchas\s+ganas", re.IGNORECASE)),
    # Slop punctuation: dash-style asides read as AI-generated. Restructure
    # into plain sentences instead.
    ("em dash", re.compile(r"—")),
    ("en dash", re.compile(r"–")),
    ("double hyphen", re.compile(r"--")),
    # A hyphen with spaces around it is a pseudo-dash; hyphenated compounds
    # ("last-minute") and numeric ranges ("3-4 horas") don't match.
    ("spaced hyphen used as dash", re.compile(r"\s-\s")),
    # "not just X" contrast constructions
    ("'not just' contrast", re.compile(r"\b(?:isn't|not)\s+just\b", re.IGNORECASE)),
    ("'no solo' contrast", re.compile(r"\bno\s+s[oó]lo\b", re.IGNORECASE)),
    ("'não apenas' contrast", re.compile(r"\bn[aã]o\s+apenas\b", re.IGNORECASE)),
    ("'the real difference'", re.compile(r"\bthe\s+real\s+difference\b", re.IGNORECASE)),
]


def _banned_patterns_from_claims(
    claims: dict,
) -> list[tuple[str, re.Pattern[str]]]:
    """Compile the registry's optional ``banned_phrases`` entries.

    Each entry is ``{"label": str, "pattern": str, "case_sensitive": bool}``
    (``case_sensitive`` defaults to false). A malformed entry fails loudly —
    a silently-dropped ban is exactly what this linter exists to prevent.
    """
    patterns = list(_DEFAULT_BANNED_PATTERNS)
    for entry in claims.get("banned_phrases", []):
        if not isinstance(entry, dict) or "pattern" not in entry:
            raise ValueError(
                f"claims.json banned_phrases entry must be a mapping with a "
                f"'pattern' key, got: {entry!r}"
            )
        flags = 0 if entry.get("case_sensitive", False) else re.IGNORECASE
        label = entry.get("label", entry["pattern"])
        patterns.append((label, re.compile(entry["pattern"], flags)))
    return patterns


def _banned_phrases_rule(
    text: str,
    location: str,
    patterns: list[tuple[str, re.Pattern[str]]] | None = None,
) -> list[Finding]:
    findings: list[Finding] = []
    for label, pat in patterns if patterns is not None else _DEFAULT_BANNED_PATTERNS:
        if pat.search(text):
            findings.append(Finding(
                severity=Severity.BLOCK,
                rule="banned-phrases",
                location=location,
                detail=f"Banned phrase {label!r} found. Remove or rephrase.",
            ))
    return findings


# ---------------------------------------------------------------------------
# Rule 3: worst-case render
# ---------------------------------------------------------------------------

# Reuse the same regex used in daily_check_helpers for consistency.
_PLACEHOLDER_RE = re.compile(r"\[[^\[\]\n]+\]")


def _personalize_worst_case(template: str, lang_code: str = "es") -> str:
    """Render template with worst-case (all-empty) personalization data.

    Uses the language code from the location path to ensure language-specific
    placeholders (e.g. ``[industry_clause_en]``) are resolved with the
    correct language variant.
    """
    # Import here to avoid circular issues at module load and to keep the
    # linter dependency-light for standalone use.
    try:
        from models.campaign import Language, personalize
        lang_map = {"es": Language.ES, "en": Language.EN, "pt": Language.PT}
        lang = lang_map.get(lang_code, Language.ES)
    except ImportError:
        # Fallback: just do simple [Name]/[Company] replacement.
        return template.replace("[Name]", "").replace("[Company]", "")
    return personalize(template, name="TestName", company="TestCo", industry="", language=lang)


def _worst_case_render_rule(text: str, location: str) -> list[Finding]:
    """Check that no [token] survives after worst-case personalization.

    Infers the language code from the location path (last path segment before
    any field name) so language-specific placeholders are resolved correctly.
    """
    # Infer language from location: path ends in e.g. ".es" or ".en.body_html"
    parts = location.split(".")
    lang_code = "es"
    for part in reversed(parts):
        if part in ("es", "en", "pt"):
            lang_code = part
            break

    rendered = _personalize_worst_case(text, lang_code=lang_code)
    m = _PLACEHOLDER_RE.search(rendered)
    if m:
        return [Finding(
            severity=Severity.BLOCK,
            rule="worst-case-render",
            location=location,
            detail=(
                f"Unresolved placeholder {m.group(0)!r} survives worst-case render. "
                "Add handling in personalize() or remove the token."
            ),
        )]
    return []


# ---------------------------------------------------------------------------
# Rule 4: register-mixing (ES only)
# ---------------------------------------------------------------------------

_INFORMAL_MARKERS: list[re.Pattern[str]] = [
    re.compile(r"\btú\b", re.IGNORECASE),
    re.compile(r"\b ti \b", re.IGNORECASE),
    re.compile(r"\btienes\b", re.IGNORECASE),
    re.compile(r"\bquieres\b", re.IGNORECASE),
    re.compile(r"\btu planta\b", re.IGNORECASE),
]

_FORMAL_MARKERS: list[re.Pattern[str]] = [
    re.compile(r"\busted\b", re.IGNORECASE),
    re.compile(r"\ble explico\b", re.IGNORECASE),
    re.compile(r"\bsu planta\b", re.IGNORECASE),
]


def _register_mixing_rule(
    persona: str,
    persona_messages: dict,
) -> list[Finding]:
    """Check for informal/formal co-occurrence within one persona's ES sequence."""
    findings: list[Finding] = []
    # Collect all ES strings for this persona's sequence.
    es_texts: list[tuple[str, str]] = []  # (step_key, text)
    for step_key, step_val in persona_messages.items():
        if isinstance(step_val, dict):
            es_text = step_val.get("es", "")
            if es_text:
                es_texts.append((step_key, es_text))

    for step_key, text in es_texts:
        has_informal = any(p.search(text) for p in _INFORMAL_MARKERS)
        has_formal = any(p.search(text) for p in _FORMAL_MARKERS)
        if has_informal and has_formal:
            findings.append(Finding(
                severity=Severity.WARN,
                rule="register-mixing",
                location=f"messages.{persona}.{step_key}.es",
                detail=(
                    "Co-occurrence of informal (tú/tienes/quieres/tu planta) and formal "
                    "(usted/le explico/su planta) register markers in the same ES message. "
                    "Pick one register per persona sequence."
                ),
            ))
    return findings


# ---------------------------------------------------------------------------
# Rule 5: CTA presence
# ---------------------------------------------------------------------------

_CTA_PATTERN = re.compile(
    r"\?|agendar|demo|espacios|time slots?|call\b",
    re.IGNORECASE,
)

# Steps that require a CTA (dm messages and email bodies — not dm3/closer steps).
_NO_CTA_REQUIRED_STEPS = {"dm3", "dm3_v1", "connection_note", "email3"}


def _is_cta_required(step_key: str) -> bool:
    for skip in _NO_CTA_REQUIRED_STEPS:
        if step_key == skip or step_key.startswith("dm3"):
            return False
    # connection_note is short and doesn't need a CTA. The prefix match
    # covers the pain-signal lane's connection_note_poster /
    # connection_note_liker variants (same short-note class).
    return not step_key.startswith("connection_note")


def _cta_presence_rule(
    text: str,
    location: str,
    step_key: str,
) -> list[Finding]:
    if not _is_cta_required(step_key):
        return []
    if not _CTA_PATTERN.search(text):
        return [Finding(
            severity=Severity.WARN,
            rule="cta-presence",
            location=location,
            detail=(
                "No CTA detected (no '?' or agendar/demo/espacios/time slots/call). "
                "Every dm and email body must include a clear call to action."
            ),
        )]
    return []


# ---------------------------------------------------------------------------
# Rule 6: cold-email site link
# ---------------------------------------------------------------------------

# Operators must include a link to their own domain in the cold email.
# Rule checks for any href= attribute in email1 body_html (operator-agnostic).
_SITE_LINK_PATTERN = re.compile(r'href\s*=\s*["\']https?://', re.IGNORECASE)

# The shipped placeholder sentinel — bodies still carrying this token have not
# been replaced by the operator yet and are skipped by rule 6 (the operator
# will add their site link when they write real copy).
_PLACEHOLDER_SENTINEL = "REPLACE_THIS_TEMPLATE"


def _cold_email_site_link_rule(emails: dict) -> list[Finding]:
    findings: list[Finding] = []
    email1 = emails.get("email1")
    if not isinstance(email1, dict):
        return findings
    for lang, content in email1.items():
        if isinstance(content, dict):
            body = content.get("body_html", "")
            # Skip placeholder bodies — the operator has not written copy yet.
            if _PLACEHOLDER_SENTINEL in body:
                continue
            if not _SITE_LINK_PATTERN.search(body):
                findings.append(Finding(
                    severity=Severity.BLOCK,
                    rule="cold-email-site-link",
                    location=f"emails.email1.{lang}.body_html",
                    detail=(
                        "email1 body must contain an href link to the operator's site "
                        "(cold-email site link rule). Add an <a href=\"https://...\"> link."
                    ),
                ))
    return findings


# ---------------------------------------------------------------------------
# Main linter
# ---------------------------------------------------------------------------

def lint_content(
    messages: dict,
    emails: dict,
    claims: dict,
) -> list[Finding]:
    """Run all lint rules over messages + emails.

    Returns a list of :class:`Finding` instances.  BLOCK findings must be
    resolved before the copy ships; WARN findings are surfaced for review.
    """
    findings: list[Finding] = []
    demoable_frags, needs_evidence_frags = _build_registry_fragments(claims)
    banned_patterns = _banned_patterns_from_claims(claims)

    # --- messages.json ---
    for persona, persona_msgs in messages.items():
        if not isinstance(persona_msgs, dict):
            continue

        # Rule 4: register-mixing per persona
        findings.extend(_register_mixing_rule(persona, persona_msgs))

        for step_key, step_val in persona_msgs.items():
            if not isinstance(step_val, dict):
                continue
            for lang, text in step_val.items():
                if not isinstance(text, str):
                    continue
                location = f"messages.{persona}.{step_key}.{lang}"

                # Rule 1: claim-gate
                findings.extend(_claim_gate_rule(text, location, demoable_frags, needs_evidence_frags))
                # Rule 2: banned-phrases
                findings.extend(_banned_phrases_rule(text, location, banned_patterns))
                # Rule 3: worst-case render
                findings.extend(_worst_case_render_rule(text, location))
                # Rule 5: CTA presence
                findings.extend(_cta_presence_rule(text, location, step_key))

    # --- emails.json ---
    for email_key, email_val in emails.items():
        if not isinstance(email_val, dict):
            findings.append(Finding(Severity.BLOCK, "blank-copy", f"emails.{email_key}",
                                    "Email template must contain language-specific subject and body_html fields."))
            continue
        for lang, content in email_val.items():
            if not isinstance(content, dict):
                findings.append(Finding(Severity.BLOCK, "blank-copy", f"emails.{email_key}.{lang}",
                                        "Email language must contain subject and body_html fields."))
                continue
            findings.extend(_required_email_fields_rule(content, f"emails.{email_key}.{lang}"))
            for field_name, text in content.items():
                if not isinstance(text, str):
                    continue
                location = f"emails.{email_key}.{lang}.{field_name}"
                if field_name in _REQUIRED_EMAIL_FIELDS:
                    findings.extend(_blank_copy_rule(text, location))

                # Rule 1: claim-gate (only on body, not subject)
                if field_name != "subject":
                    findings.extend(_claim_gate_rule(text, location, demoable_frags, needs_evidence_frags))
                # Rule 2: banned-phrases (all fields)
                findings.extend(_banned_phrases_rule(text, location, banned_patterns))
                # Rule 3: worst-case render
                findings.extend(_worst_case_render_rule(text, location))
                # Rule 5: CTA presence (email bodies only, skip email3)
                if field_name == "body_html":
                    findings.extend(_cta_presence_rule(text, location, email_key))

    # Rule 6: cold-email site link
    findings.extend(_cold_email_site_link_rule(emails))

    return findings


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main() -> None:
    """CLI: lint the committed content files and report findings.

    Exits 0 if no BLOCK findings; exits 1 if any BLOCK finding is found.
    """
    messages = _load_default_messages()
    emails = _load_default_emails()
    claims = _load_default_claims()

    findings = lint_content(messages, emails, claims)

    blocks = [f for f in findings if f.severity == Severity.BLOCK]
    warns = [f for f in findings if f.severity == Severity.WARN]

    if warns:
        print("=== WARNINGS ===")
        for f in warns:
            print(f"  {f}")
        print()

    if blocks:
        print("=== BLOCKERS (fix before shipping) ===")
        for f in blocks:
            print(f"  {f}")
        print()
        print(f"Content lint FAILED: {len(blocks)} blocker(s), {len(warns)} warning(s).")
        sys.exit(1)
    else:
        print(f"Content lint passed. {len(warns)} warning(s) noted above.")
        sys.exit(0)


if __name__ == "__main__":
    main()


def _blank_copy_rule(text: str, location: str) -> list[Finding]:
    """BLOCK when a subject/body renders to blank text.

    Rendered via ``html_to_text`` (tags stripped, entities unescaped), so
    markup-only bodies like ``<p>&nbsp;</p>`` count as blank.
    """
    if not html_to_text(text):
        return [Finding(
            severity=Severity.BLOCK,
            rule="blank-copy",
            location=location,
            detail=(
                "Renders to blank text (tags stripped, entities unescaped). "
                "An emptied template would send a blank email; restore the copy."
            ),
        )]
    return []



def _required_email_fields_rule(content: dict, location: str) -> list[Finding]:
    """BLOCK when a template is missing ``subject`` or ``body_html``.

    A missing field sends blank (or crashes) at send time, same failure class
    as an emptied string.
    """
    findings: list[Finding] = []
    for field_name in _REQUIRED_EMAIL_FIELDS:
        if not isinstance(content.get(field_name), str):
            findings.append(Finding(
                severity=Severity.BLOCK,
                rule="blank-copy",
                location=f"{location}.{field_name}",
                detail=(
                    f"Required field {field_name!r} is missing or not a string. "
                    "Every email template needs both a subject and a body_html."
                ),
            ))
    return findings



_REQUIRED_EMAIL_FIELDS = ("subject", "body_html")
