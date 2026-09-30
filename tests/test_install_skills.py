"""Selected skill installation must include its referenced files."""
import os
import subprocess
from pathlib import Path


def test_selected_codex_skill_includes_references_and_is_repeatable(tmp_path):
    root = Path(__file__).resolve().parents[1]
    destination = tmp_path / "installed"
    env = {**os.environ, "OUTBOUND_SKILLS_DIR": str(destination)}
    args = ["bash", str(root / "scripts/install-skills.sh"), "--codex", "sales-daily"]
    for _ in range(2):
        result = subprocess.run(args, env=env, capture_output=True, text=True)
        assert result.returncode == 0, result.stderr
        assert (destination / "sales-daily/SKILL.md").read_bytes() == (root / "skills/sales-daily/SKILL.md").read_bytes()
        assert (destination / "sales-daily/references/followup-review.md").read_bytes() == (root / "skills/sales-daily/references/followup-review.md").read_bytes()
    assert sorted(p.name for p in destination.iterdir()) == ["sales-daily"]


def test_invalid_skill_name_cannot_escape_destination(tmp_path):
    root = Path(__file__).resolve().parents[1]
    result = subprocess.run(
        ["bash", str(root / "scripts/install-skills.sh"), "--codex", "../escape"],
        env={**os.environ, "OUTBOUND_SKILLS_DIR": str(tmp_path / "installed")},
        capture_output=True, text=True,
    )
    assert result.returncode == 2
    assert not (tmp_path / "escape").exists()
