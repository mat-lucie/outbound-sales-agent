from unittest.mock import Mock

import pytest

from workflows.detect_responses import IdentityResolutionHalt
from workflows.phase_checkpoint import AcceptanceCheckpoint


def checkpoint(path, **kwargs):
    return AcceptanceCheckpoint(path=path, operator_id="operator", day="2026-09-29",
                                provenance=kwargs.get("provenance", {"sha": "abc", "dirty": False}),
                                backend="sales_nav", scraper_id="scraper")


def halt():
    # Constructor also carries diagnostics; the checkpoint keys off the type.
    return IdentityResolutionHalt.__new__(IdentityResolutionHalt)


def test_completed_acceptance_resumes_after_identity_halt_only(tmp_path):
    path = tmp_path / "state.json"
    action = Mock(return_value={"accepted": 2, "complete": True})
    with pytest.raises(IdentityResolutionHalt), checkpoint(path) as phase:
        phase.run(action)
        raise halt()
    with checkpoint(path) as phase:
        assert phase.run(action)["accepted"] == 2
    assert action.call_count == 1
    with checkpoint(path) as phase:
        phase.run(action)
    assert action.call_count == 2  # Normal next run refreshes acceptance coverage.


@pytest.mark.parametrize("complete,changed", [(False, False), (True, True)])
def test_partial_phase_or_changed_code_cannot_resume(tmp_path, complete, changed):
    path = tmp_path / "state.json"
    action = Mock(return_value={"complete": complete})
    with pytest.raises(IdentityResolutionHalt), checkpoint(path) as phase:
        phase.run(action)
        raise halt()
    with checkpoint(path, provenance={"sha": "def" if changed else "abc", "dirty": False}) as phase:
        phase.run(action)
    assert action.call_count == 2


def test_unrelated_error_never_authorizes_resume(tmp_path):
    path = tmp_path / "state.json"
    action = Mock(return_value={"complete": True})
    with pytest.raises(RuntimeError), checkpoint(path) as phase:
        phase.run(action)
        raise RuntimeError("sender failed")
    with checkpoint(path) as phase:
        phase.run(action)
    assert action.call_count == 2


@pytest.mark.parametrize("provenance", [
    {"sha": "abc", "dirty": True}, {"sha": "abc", "dirty": None},
    {"sha": "abc"}, {"sha": "unknown", "dirty": False}, {},
])
def test_dirty_or_unknown_code_never_reuses_checkpoint(tmp_path, provenance):
    path = tmp_path / "state.json"
    action = Mock(return_value={"complete": True})
    with pytest.raises(IdentityResolutionHalt), checkpoint(path, provenance=provenance) as phase:
        phase.run(action)
        raise halt()
    with checkpoint(path, provenance=provenance) as phase:
        phase.run(action)
    assert action.call_count == 2


def test_checkpoint_write_failure_preserves_original_halt(tmp_path, monkeypatch, capsys):
    phase = checkpoint(tmp_path / "state.json")
    with pytest.raises(IdentityResolutionHalt), phase:
        phase.run(lambda: {"complete": True})
        monkeypatch.setattr(phase, "_save", Mock(side_effect=OSError("disk")))
        raise halt()
    assert "checkpoint write failed" in capsys.readouterr().err


def test_corrupt_checkpoint_reports_fresh_acceptance(tmp_path, capsys):
    path = tmp_path / "state.json"
    path.write_text("invalid-json")
    action = Mock(return_value={"complete": True})
    with checkpoint(path) as phase:
        phase.run(action)
    action.assert_called_once()
    warning = capsys.readouterr().err
    assert str(path) in warning
    assert "JSONDecodeError" in warning
    assert "acceptance will run fresh" in warning
