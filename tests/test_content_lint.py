"""Tests for scripts/content_lint.py — deterministic content gate.

Structure
---------
- Unit tests per rule using minimal fixture content.
- ``test_live_content_has_no_blockers()`` runs the linter over the real
  committed placeholder content files (content/) and asserts zero BLOCK
  findings (WARNs printed to stdout for visibility).
- ``test_acme_content_has_no_blockers()`` runs the linter over the bundled
  Acme example operator content (examples/acme/content/) with its own
  claims registry and asserts zero BLOCK findings.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.content_lint import (
    Finding,
    Severity,
    _banned_patterns_from_claims,
    _banned_phrases_rule,
    _blank_copy_rule,
    _build_registry_fragments,
    _claim_gate_rule,
    _cold_email_site_link_rule,
    _cta_presence_rule,
    _load_default_claims,
    _load_default_emails,
    _load_default_messages,
    _register_mixing_rule,
    _required_email_fields_rule,
    _worst_case_render_rule,
    lint_content,
)

_REPO_ROOT = Path(__file__).resolve().parent.parent
_ACME_CONTENT = _REPO_ROOT / "examples" / "acme" / "content"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _blocks(findings: list[Finding]) -> list[Finding]:
    return [f for f in findings if f.severity == Severity.BLOCK]


def _warns(findings: list[Finding]) -> list[Finding]:
    return [f for f in findings if f.severity == Severity.WARN]


def _frags(claims: dict):
    return _build_registry_fragments(claims)


# ---------------------------------------------------------------------------
# Rule 1: claim-gate
# ---------------------------------------------------------------------------

class TestClaimGate:
    CLAIMS_DEMOABLE = {
        "claims": [
            {
                "id": "reschedule-seconds",
                "claim": "reschedules in seconds / reprograma en segundos",
                "languages": ["en", "es"],
                "type": "capability",
                "evidence": "Live demo",
                "status": "demoable",
            }
        ]
    }

    CLAIMS_NEEDS_EVIDENCE = {
        "claims": [
            {
                "id": "roi-weeks",
                "claim": "ROI in weeks / ROI en semanas",
                "languages": ["en", "es"],
                "type": "outcome",
                "evidence": "Unverified",
                "status": "needs_evidence",
            }
        ]
    }

    CLAIMS_EMPTY = {"claims": []}

    def test_no_quantified_claim_passes(self):
        text = "We work with manufacturing plants to optimize their production scheduling."
        demoable, needs_ev = _frags(self.CLAIMS_EMPTY)
        findings = _claim_gate_rule(text, "loc", demoable, needs_ev)
        assert findings == []

    def test_registered_demoable_claim_no_finding(self):
        text = "With Acme, the scheduler reschedules in seconds with real constraints."
        demoable, needs_ev = _frags(self.CLAIMS_DEMOABLE)
        findings = _claim_gate_rule(text, "loc", demoable, needs_ev)
        assert _blocks(findings) == [], "Demoable claim should not block"

    def test_unregistered_quantified_claim_blocks(self):
        # OTIF +5-8 puntos is unregistered in empty claims
        text = "Result: OTIF +5-8 puntos en el primer trimestre."
        demoable, needs_ev = _frags(self.CLAIMS_EMPTY)
        findings = _claim_gate_rule(text, "loc", demoable, needs_ev)
        assert len(_blocks(findings)) >= 1, "Unregistered OTIF claim should BLOCK"

    def test_needs_evidence_claim_warns_not_blocks(self):
        text = "ROI shows up in weeks, not months."
        demoable, needs_ev = _frags(self.CLAIMS_NEEDS_EVIDENCE)
        findings = _claim_gate_rule(text, "loc", demoable, needs_ev)
        assert _blocks(findings) == [], "needs_evidence should WARN not BLOCK"
        assert len(_warns(findings)) >= 1, "needs_evidence should emit a WARN"

    def test_ebitda_unregistered_blocks(self):
        text = "Companies lose about 15% of total EBITDA on operative supply chain mistakes."
        demoable, needs_ev = _frags(self.CLAIMS_EMPTY)
        findings = _claim_gate_rule(text, "loc", demoable, needs_ev)
        assert len(_blocks(findings)) >= 1, "Unregistered EBITDA claim should BLOCK"

    def test_percentage_stat_unregistered_blocks(self):
        text = "retailers charge ~3% of non-compliant shipment volume"
        demoable, needs_ev = _frags(self.CLAIMS_EMPTY)
        findings = _claim_gate_rule(text, "loc", demoable, needs_ev)
        assert len(_blocks(findings)) >= 1, "Unregistered % stat should BLOCK"


# ---------------------------------------------------------------------------
# Rule 2: banned-phrases
# ---------------------------------------------------------------------------

class TestBannedPhrases:
    def test_muchas_ganas_blocks(self):
        text = "Muchas ganas de conectar contigo."
        findings = _banned_phrases_rule(text, "loc")
        assert len(_blocks(findings)) >= 1

    def test_muchas_ganas_case_insensitive_blocks(self):
        text = "muchas ganas de hablar."
        findings = _banned_phrases_rule(text, "loc")
        assert len(_blocks(findings)) >= 1

    _TYPO_REGISTRY = {
        "banned_phrases": [
            {"label": "product typo", "pattern": r"\bAcmee\b", "case_sensitive": True},
        ]
    }

    def test_registry_banned_pattern_blocks(self):
        # Operators register misspellings of their own product name.
        patterns = _banned_patterns_from_claims(self._TYPO_REGISTRY)
        text = "Hi John, I'm with Acmee. We build AI for production scheduling."
        findings = _banned_phrases_rule(text, "loc", patterns)
        blocks = _blocks(findings)
        assert len(blocks) >= 1
        assert any("product typo" in f.detail for f in blocks)

    def test_correct_product_name_not_blocked(self):
        # "Acme" is NOT "Acmee" — the word-boundary pattern must not fire.
        patterns = _banned_patterns_from_claims(self._TYPO_REGISTRY)
        text = "I'm with Acme. We build AI for production scheduling."
        findings = _banned_phrases_rule(text, "loc", patterns)
        assert _blocks(findings) == [], "the correct spelling should not be flagged"

    def test_registry_pattern_embedded_in_word_not_blocked(self):
        patterns = _banned_patterns_from_claims(self._TYPO_REGISTRY)
        text = "Check out https://example.com/acmeefactory for details."
        findings = _banned_phrases_rule(text, "loc", patterns)
        assert _blocks(findings) == [], "embedded match should not trigger the rule"

    def test_malformed_banned_phrases_entry_fails_loudly(self):
        # A silently-dropped ban is what the linter exists to prevent.
        with pytest.raises(ValueError, match="banned_phrases"):
            _banned_patterns_from_claims({"banned_phrases": ["not-a-mapping"]})

    def test_no_registry_key_uses_defaults_only(self):
        patterns = _banned_patterns_from_claims({})
        text = "Hi John, I'm with Acmee. Muchas ganas de hablar."
        findings = _banned_phrases_rule(text, "loc", patterns)
        blocks = _blocks(findings)
        assert len(blocks) == 1, "only the built-in ban should fire"

    def test_acme_name_not_blocked(self):
        text = "I'm with Acme. We build AI for production scheduling."
        findings = _banned_phrases_rule(text, "loc")
        assert findings == []

    # --- slop punctuation / phrasing ---

    def test_em_dash_blocks(self):
        text = "I'm with Acme — we build AI for production scheduling."
        findings = _banned_phrases_rule(text, "loc")
        assert len(_blocks(findings)) >= 1, "Em dash should BLOCK"

    def test_en_dash_blocks(self):
        text = "The plan – rebuilt in seconds."
        findings = _banned_phrases_rule(text, "loc")
        assert len(_blocks(findings)) >= 1, "En dash should BLOCK"

    def test_double_hyphen_blocks(self):
        text = "We build AI -- for production scheduling."
        findings = _banned_phrases_rule(text, "loc")
        assert len(_blocks(findings)) >= 1, "Double hyphen should BLOCK"

    def test_spaced_hyphen_blocks(self):
        text = "Trabajamos con plantas como la tuya - sin proyecto de TI."
        findings = _banned_phrases_rule(text, "loc")
        assert len(_blocks(findings)) >= 1, "Spaced hyphen used as dash should BLOCK"

    def test_hyphenated_compound_not_blocked(self):
        text = "Every week it fell apart: shortages, last-minute changes."
        findings = _banned_phrases_rule(text, "loc")
        assert _blocks(findings) == [], "Hyphenated compounds should not be flagged"

    def test_numeric_range_not_blocked(self):
        text = "El programador se aventaba 3-4 horas diarias armando el plan."
        findings = _banned_phrases_rule(text, "loc")
        assert _blocks(findings) == [], "Numeric ranges should not be flagged"

    def test_not_just_blocks(self):
        text = "It's not just scheduling, it's the whole operation."
        findings = _banned_phrases_rule(text, "loc")
        assert len(_blocks(findings)) >= 1, "'not just' contrast should BLOCK"

    def test_no_solo_blocks(self):
        text = "Optimiza para el objetivo real de la planta, no solo productividad."
        findings = _banned_phrases_rule(text, "loc")
        assert len(_blocks(findings)) >= 1, "'no solo' contrast should BLOCK"

    def test_nao_apenas_blocks(self):
        text = "Otimiza para o objetivo real da planta, não apenas produtividade."
        findings = _banned_phrases_rule(text, "loc")
        assert len(_blocks(findings)) >= 1, "'não apenas' contrast should BLOCK"

    def test_the_real_difference_blocks(self):
        text = "The real difference is how fast the team recovers."
        findings = _banned_phrases_rule(text, "loc")
        assert len(_blocks(findings)) >= 1, "'the real difference' should BLOCK"

    def test_clean_text_no_findings(self):
        text = "Hi, I'd love to connect and share what we're seeing in production planning."
        findings = _banned_phrases_rule(text, "loc")
        assert findings == []


# ---------------------------------------------------------------------------
# Rule 3: worst-case render
# ---------------------------------------------------------------------------

class TestWorstCaseRender:
    def test_template_with_no_placeholders_passes(self):
        text = "Hi TestName, let me give you a real example at another plant."
        findings = _worst_case_render_rule(text, "messages.ops.dm2.en")
        assert findings == []

    def test_name_placeholder_resolved_passes(self):
        # [Name] and [Company] are standard and resolved by personalize
        text = "[Name], let me tell you about [Company]."
        findings = _worst_case_render_rule(text, "messages.ops.dm1.en")
        assert findings == [], "[Name]/[Company] should resolve, no block"

    def test_industry_clause_es_resolved_with_empty_industry(self):
        # The structural placeholder must resolve even with empty industry
        text = "[Name], real example: [industry_clause_es], the scheduler..."
        findings = _worst_case_render_rule(text, "messages.operations_leaders.dm2.es")
        assert findings == [], "[industry_clause_es] with empty industry should resolve to 'en otra planta'"

    def test_industry_clause_en_resolved_with_empty_industry(self):
        text = "[Name], real example: [industry_clause_en]. The scheduler..."
        findings = _worst_case_render_rule(text, "messages.operations_leaders.dm2.en")
        assert findings == [], "[industry_clause_en] with empty industry should resolve to 'at another plant'"

    def test_industry_clause_pt_resolved_with_empty_industry(self):
        text = "[Name], exemplo real: [industry_clause_pt]. O programador..."
        findings = _worst_case_render_rule(text, "messages.operations_leaders.dm2.pt")
        assert findings == [], "[industry_clause_pt] with empty industry should resolve to 'em outra planta'"

    def test_unknown_placeholder_blocks(self):
        text = "Hi [Name], visit [UnresolvedToken] for details."
        findings = _worst_case_render_rule(text, "loc.en")
        assert len(_blocks(findings)) >= 1, "Unknown token should BLOCK"

    def test_industria_similar_legacy_resolved(self):
        # Legacy [industria similar] token still resolves via the legacy path
        text = "[Name], en una planta de [industria similar], el programador..."
        findings = _worst_case_render_rule(text, "messages.ops.dm2.es")
        assert findings == [], "[industria similar] should resolve via legacy path"


# ---------------------------------------------------------------------------
# Rule 4: register-mixing
# ---------------------------------------------------------------------------

class TestRegisterMixing:
    def test_pure_informal_no_warning(self):
        persona_msgs = {
            "dm1": {"es": "Hola, ¿cómo estás tú? Cuéntame lo que tienes en mente."},
            "dm2": {"es": "¿Tienes 15 minutos para ver la demo? Tu planta nos importa."},
        }
        findings = _register_mixing_rule("test_persona", persona_msgs)
        assert _warns(findings) == []

    def test_pure_formal_no_warning(self):
        persona_msgs = {
            "dm1": {"es": "Hola, ¿cómo está usted? Le explico nuestra solución."},
            "dm2": {"es": "¿Tiene 15 minutos? Podemos ver su planta como ejemplo."},
        }
        findings = _register_mixing_rule("test_persona", persona_msgs)
        assert _warns(findings) == []

    def test_mixed_register_warns(self):
        persona_msgs = {
            "dm2": {
                "es": (
                    "Hola, quieres ver la demo? Usted puede agendar directamente. "
                    "Le explico el proceso. Tu planta importa."
                ),
            },
        }
        findings = _register_mixing_rule("test_persona", persona_msgs)
        assert len(_warns(findings)) >= 1, "Mixed register should produce WARN"

    def test_non_es_languages_not_checked(self):
        # Rule only applies to ES; EN text with Spanish-looking words should not fire
        persona_msgs = {
            "dm2": {
                "en": "Hi, do you have 15 minutes? Would you like a quick demo?",
            },
        }
        findings = _register_mixing_rule("test_persona", persona_msgs)
        assert findings == []


# ---------------------------------------------------------------------------
# Rule 5: CTA presence
# ---------------------------------------------------------------------------

class TestCtaPresence:
    def test_question_mark_passes(self):
        text = "Do you have 15 minutes to chat this week?"
        findings = _cta_presence_rule(text, "loc", "dm2")
        assert findings == []

    def test_agendar_passes(self):
        text = "Podemos agendar una demo rápida de 15 minutos."
        findings = _cta_presence_rule(text, "loc", "dm2")
        assert findings == []

    def test_demo_passes(self):
        text = "Send me 2 timeslots and I'll book a demo."
        findings = _cta_presence_rule(text, "loc", "email1")
        assert findings == []

    def test_espacios_passes(self):
        text = "me mandarías 2 espacios disponibles"
        findings = _cta_presence_rule(text, "loc", "dm2_v1")
        assert findings == []

    def test_time_slots_passes(self):
        text = "Send me 2 available time slots and we can set it up."
        findings = _cta_presence_rule(text, "loc", "email1")
        assert findings == []

    def test_call_passes(self):
        text = "Happy to jump on a call if you're interested."
        findings = _cta_presence_rule(text, "loc", "email1")
        assert findings == []

    def test_no_cta_warns(self):
        text = "This is a message with no invitation to engage whatsoever."
        findings = _cta_presence_rule(text, "loc", "dm2")
        assert len(_warns(findings)) >= 1, "Missing CTA should WARN"

    def test_dm3_skipped(self):
        # dm3 is a closer — no CTA required
        text = "Last message. Best regards."
        findings = _cta_presence_rule(text, "loc", "dm3")
        assert findings == []

    def test_dm3_v1_skipped(self):
        text = "Last message from me — no worries."
        findings = _cta_presence_rule(text, "loc", "dm3_v1")
        assert findings == []

    def test_connection_note_skipped(self):
        # connection_note is short; no CTA required
        text = "Hi, I'd love to connect."
        findings = _cta_presence_rule(text, "loc", "connection_note")
        assert findings == []


# ---------------------------------------------------------------------------
# Rule 6: cold-email site link
# ---------------------------------------------------------------------------

class TestColdEmailSiteLink:
    def test_href_present_passes(self):
        emails = {
            "email1": {
                "en": {"body_html": '<p>Check <a href="https://acme.example.com">acme.example.com</a>.</p>'},
            }
        }
        findings = _cold_email_site_link_rule(emails)
        assert _blocks(findings) == []

    def test_href_missing_blocks(self):
        emails = {
            "email1": {
                "en": {"body_html": "<p>Hi, check out our website for more info.</p>"},
            }
        }
        findings = _cold_email_site_link_rule(emails)
        assert len(_blocks(findings)) >= 1, "Missing href should BLOCK"

    def test_no_email1_no_finding(self):
        emails = {
            "email2": {
                "en": {"body_html": "<p>No site link needed here.</p>"},
            }
        }
        findings = _cold_email_site_link_rule(emails)
        assert findings == []

    def test_all_langs_checked(self):
        emails = {
            "email1": {
                "en": {"body_html": '<p><a href="https://acme.example.com">link</a></p>'},
                "es": {"body_html": "<p>Sin enlace aquí.</p>"},
                "pt": {"body_html": '<p><a href="https://acme.example.com">link</a></p>'},
            }
        }
        findings = _cold_email_site_link_rule(emails)
        blocks = _blocks(findings)
        assert len(blocks) == 1
        assert "es" in blocks[0].location, "Only the ES body should block"

    def test_placeholder_body_skipped(self):
        # Bodies still containing the shipped placeholder sentinel are skipped
        emails = {
            "email1": {
                "en": {"body_html": "Hi {{first_name}}, REPLACE_THIS_TEMPLATE — your email goes here."},
            }
        }
        findings = _cold_email_site_link_rule(emails)
        assert _blocks(findings) == [], "Placeholder body should not trigger rule 6"


# ---------------------------------------------------------------------------
# Rule 7: blank-copy
# ---------------------------------------------------------------------------

class TestBlankCopy:
    def test_normal_subject_passes(self):
        assert _blank_copy_rule("Quick question", "emails.email1.en.subject") == []

    def test_normal_body_passes(self):
        assert _blank_copy_rule("<p>Hola, ¿tienes 15 minutos?</p>", "emails.email1.es.body_html") == []

    def test_empty_string_blocks(self):
        findings = _blank_copy_rule("", "emails.email2.es.subject")
        assert len(_blocks(findings)) == 1
        assert findings[0].rule == "blank-copy"

    def test_whitespace_only_blocks(self):
        findings = _blank_copy_rule("   \n\t ", "emails.email3.en.subject")
        assert len(_blocks(findings)) == 1

    def test_markup_only_body_blocks(self):
        # Tags stripped + entities unescaped → "<p>&nbsp;</p>" is blank.
        findings = _blank_copy_rule("<p>&nbsp;</p>", "emails.wave2.es.body_html")
        assert len(_blocks(findings)) == 1

    def test_br_only_body_blocks(self):
        findings = _blank_copy_rule("<p><br></p><p></p>", "emails.email2.pt.body_html")
        assert len(_blocks(findings)) == 1

    def test_entity_whitespace_blocks(self):
        findings = _blank_copy_rule("&nbsp; &#160;", "emails.email2.en.body_html")
        assert len(_blocks(findings)) == 1

    def test_missing_subject_blocks(self):
        findings = _required_email_fields_rule({"body_html": "<p>Hola</p>"}, "emails.email2.es")
        assert len(_blocks(findings)) == 1
        assert findings[0].location == "emails.email2.es.subject"

    def test_missing_body_blocks(self):
        findings = _required_email_fields_rule({"subject": "Hola"}, "emails.email2.es")
        assert len(_blocks(findings)) == 1
        assert findings[0].location == "emails.email2.es.body_html"

    def test_non_string_field_blocks(self):
        findings = _required_email_fields_rule({"subject": None, "body_html": "<p>x</p>"}, "emails.email2.es")
        assert len(_blocks(findings)) == 1

    def test_both_fields_present_passes(self):
        content = {"subject": "Hola", "body_html": "<p>Hola</p>"}
        assert _required_email_fields_rule(content, "emails.email2.es") == []


# ---------------------------------------------------------------------------
# Integration: lint_content() with fixture data
# ---------------------------------------------------------------------------

class TestLintContentIntegration:
    CLEAN_MESSAGES = {
        "test_persona": {
            "connection_note": {
                "es": "Hola [Name], me gustaría conectar contigo en [Company].",
            },
            "dm1": {
                "es": "Hola [Name], gracias por aceptar. Hacemos IA para manufactura. ¿En [Company] cómo manejan la programación?",
            },
            "dm2": {
                "es": "[Name], con Acme los programadores replanejan en segundos. ¿Tienes 15 minutos esta semana para una demo?",
            },
            "dm3": {
                "es": "[Name], último mensaje. Saludos,",
            },
        }
    }

    CLEAN_EMAILS = {
        "email1": {
            "en": {
                "subject": "Quick question",
                "body_html": '<p>Hi {{first_name}}, check <a href="https://acme.example.com">acme.example.com</a>. Do you have 15 minutes?</p>',
            }
        }
    }

    CLEAN_CLAIMS = {"claims": []}

    def test_clean_content_no_blockers(self):
        findings = lint_content(self.CLEAN_MESSAGES, self.CLEAN_EMAILS, self.CLEAN_CLAIMS)
        assert _blocks(findings) == [], f"Expected no blockers, got: {_blocks(findings)}"

    def test_registry_product_typo_blocks(self):
        messages = {
            "test_persona": {
                "dm1": {
                    "es": "Soy de Acmee — hacemos IA. ¿Quieres ver una demo?",
                }
            }
        }
        claims = {
            **self.CLEAN_CLAIMS,
            "banned_phrases": [
                {"label": "product typo", "pattern": r"\bAcmee\b", "case_sensitive": True},
            ],
        }
        findings = lint_content(messages, self.CLEAN_EMAILS, claims)
        assert len(_blocks(findings)) >= 1

    def test_muchas_ganas_blocks(self):
        messages = {
            "test_persona": {
                "dm1": {
                    "es": "Muchas ganas de hablar contigo. ¿Tienes 15 minutos?",
                }
            }
        }
        findings = lint_content(messages, self.CLEAN_EMAILS, self.CLEAN_CLAIMS)
        assert len(_blocks(findings)) >= 1

    def test_unregistered_ebitda_blocks(self):
        messages = {
            "test_persona": {
                "dm2": {
                    "en": "Companies lose 15% of EBITDA. Got 15 minutes for a demo?",
                }
            }
        }
        findings = lint_content(messages, self.CLEAN_EMAILS, self.CLEAN_CLAIMS)
        assert len(_blocks(findings)) >= 1

    def test_missing_site_link_in_email1_blocks(self):
        emails = {
            "email1": {
                "en": {
                    "subject": "Quick question",
                    "body_html": "<p>Hi {{first_name}}, send me 2 timeslots!</p>",
                }
            }
        }
        findings = lint_content(self.CLEAN_MESSAGES, emails, self.CLEAN_CLAIMS)
        assert len(_blocks(findings)) >= 1

    def test_emptied_email_subject_blocks(self):
        emails = {
            "email2": {
                "es": {
                    "subject": "",
                    "body_html": "<p>Hola, ¿tienes 15 minutos para una demo?</p>",
                }
            }
        }
        findings = lint_content(self.CLEAN_MESSAGES, emails, self.CLEAN_CLAIMS)
        blocks = _blocks(findings)
        assert any(f.rule == "blank-copy" and f.location == "emails.email2.es.subject" for f in blocks)

    @pytest.mark.parametrize("emails, location", [
        ({"email2": ""}, "emails.email2"),
        ({"email2": {"es": ""}}, "emails.email2.es"),
    ])
    def test_malformed_email_container_blocks(self, emails, location):
        findings = lint_content(self.CLEAN_MESSAGES, emails, self.CLEAN_CLAIMS)
        assert any(f.rule == "blank-copy" and f.location == location for f in _blocks(findings))

    def test_emptied_email_body_blocks(self):
        emails = {
            "email3": {
                "en": {
                    "subject": "Closing the loop",
                    "body_html": "<p>&nbsp;</p>",
                }
            }
        }
        findings = lint_content(self.CLEAN_MESSAGES, emails, self.CLEAN_CLAIMS)
        blocks = _blocks(findings)
        assert any(f.rule == "blank-copy" and f.location == "emails.email3.en.body_html" for f in blocks)

    def test_email_missing_subject_field_blocks(self):
        emails = {
            "wave2": {
                "pt": {
                    "body_html": "<p>Olá, tem 15 minutos para uma demo?</p>",
                }
            }
        }
        findings = lint_content(self.CLEAN_MESSAGES, emails, self.CLEAN_CLAIMS)
        blocks = _blocks(findings)
        assert any(f.rule == "blank-copy" and f.location == "emails.wave2.pt.subject" for f in blocks)


# ---------------------------------------------------------------------------
# Live content gate — zero BLOCK rule (shipped placeholder content/)
# ---------------------------------------------------------------------------

def test_live_content_has_no_blockers():
    """Gate: the committed placeholder content files must pass all BLOCK-severity rules.

    Lints content/ (repo-root shipped defaults). The placeholder bodies carry
    REPLACE_THIS_TEMPLATE which is neutral for claim-gate and banner rules, and
    causes rule 6 to skip (placeholder-aware). WARNs (CTA on placeholder bodies)
    are printed to stdout for visibility but do not fail the test.
    """
    messages = _load_default_messages()
    emails = _load_default_emails()
    claims = _load_default_claims()
    findings = lint_content(messages, emails, claims)
    warns = _warns(findings)
    blocks = _blocks(findings)

    if warns:
        print(f"\n=== Content lint warnings on shipped placeholders ({len(warns)} total) ===")
        for w in warns:
            print(f"  {w}")

    assert blocks == [], (
        f"Shipped placeholder content has {len(blocks)} BLOCK finding(s):\n"
        + "\n".join(f"  {b}" for b in blocks)
    )


# ---------------------------------------------------------------------------
# Acme example content gate — zero BLOCK rule (examples/acme/content/)
# ---------------------------------------------------------------------------

def test_acme_content_has_no_blockers():
    """Gate: the bundled Acme example operator content must pass all BLOCK-severity rules.

    Lints examples/acme/content/ with its own claims registry. The acme content
    has real copy with quantified claims — all registered (demoable or
    needs_evidence). needs_evidence entries produce WARNs (printed) but not BLOCKs.
    """
    with open(_ACME_CONTENT / "messages.json") as f:
        messages = json.load(f)
    with open(_ACME_CONTENT / "emails.json") as f:
        emails = json.load(f)
    with open(_ACME_CONTENT / "claims.json") as f:
        claims = json.load(f)

    findings = lint_content(messages, emails, claims)
    warns = _warns(findings)
    blocks = _blocks(findings)

    if warns:
        print(f"\n=== Acme content lint warnings ({len(warns)} total) ===")
        for w in warns:
            print(f"  {w}")

    assert blocks == [], (
        f"Acme example content has {len(blocks)} BLOCK finding(s):\n"
        + "\n".join(f"  {b}" for b in blocks)
    )


# ---------------------------------------------------------------------------
# Personalize behavior for industry clause (regression)
# ---------------------------------------------------------------------------

class TestPersonalizeIndustryClauses:
    """Unit tests for the two rendering paths in personalize() for dm2."""

    def test_known_industry_renders_specific_clause_es(self):
        from models.campaign import Language, personalize
        result = personalize(
            "[Name], real example: [industry_clause_es], the scheduler...",
            name="Ana",
            company="Acero",
            industry="automotive",
            language=Language.ES,
        )
        assert "en una planta de automotive" in result
        assert "[industry_clause_es]" not in result

    def test_known_industry_renders_specific_clause_en(self):
        from models.campaign import Language, personalize
        result = personalize(
            "[Name], real example: [industry_clause_en]. The scheduler...",
            name="Ana",
            company="Acero",
            industry="automotive",
            language=Language.EN,
        )
        assert "at a automotive plant" in result
        assert "[industry_clause_en]" not in result

    def test_known_industry_renders_specific_clause_pt(self):
        from models.campaign import Language, personalize
        result = personalize(
            "[Name], real example: [industry_clause_pt]. O programador...",
            name="Ana",
            company="Acero",
            industry="automotive",
            language=Language.PT,
        )
        assert "em uma planta de automotive" in result
        assert "[industry_clause_pt]" not in result

    def test_empty_industry_drops_clause_es(self):
        from models.campaign import Language, personalize
        result = personalize(
            "[Name], real example: [industry_clause_es], the scheduler...",
            name="Ana",
            company="Acero",
            industry="",
            language=Language.ES,
        )
        assert "en otra planta" in result
        assert "[industry_clause_es]" not in result
        # The generic "manufactura" must NOT appear as a deflating filler
        assert "en una planta de manufactura" not in result

    def test_empty_industry_drops_clause_en(self):
        from models.campaign import Language, personalize
        result = personalize(
            "[Name], real example: [industry_clause_en]. The scheduler...",
            name="Ana",
            company="Acero",
            industry="",
            language=Language.EN,
        )
        assert "at another plant" in result
        assert "[industry_clause_en]" not in result
        assert "at a manufacturing plant" not in result

    def test_empty_industry_drops_clause_pt(self):
        from models.campaign import Language, personalize
        result = personalize(
            "[Name], real example: [industry_clause_pt]. O programador...",
            name="Ana",
            company="Acero",
            industry="",
            language=Language.PT,
        )
        assert "em outra planta" in result
        assert "[industry_clause_pt]" not in result
        assert "em uma planta de manufatura" not in result
