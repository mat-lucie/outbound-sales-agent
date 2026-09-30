"""Reason and recovery guidance beneath the legacy canary halt token."""
import httpx

from clients.attio import AmbiguousAttioWrite


def canary_failure_detail(exc: Exception) -> str:
    if isinstance(exc, AmbiguousAttioWrite):
        return (
            "reason=ambiguous_write; creation may have committed. "
            "Inspect canary notes and reconcile before rerunning; do not blindly retry."
        )
    if isinstance(exc, httpx.TransportError):
        return (
            "reason=network_error; check DNS/connectivity and command-specific Codex "
            "network permission for this canary command. This does not establish a "
            "credential/scope failure. Do not enable project-wide network access."
        )
    if isinstance(exc, httpx.HTTPStatusError):
        if exc.response.status_code in (401, 403):
            return (
                "reason=authorization_error; verify ATTIO_API_KEY, workspace access "
                "and note write/delete scopes."
            )
        return "reason=http_error; inspect the HTTP status and Attio service response."
    return "reason=unexpected_error; inspect the failure before changing credentials or retrying."
