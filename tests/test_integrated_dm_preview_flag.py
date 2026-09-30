"""Guard the integrated DM rehearsal CLI boundary."""

import pytest
from click.testing import CliRunner

from cli import cli


@pytest.mark.parametrize("args", [
    ["daily", "--preview-dms-after-invites"],
    ["daily", "--skip-dms", "--preview-dms-after-invites", "--dry-run"],
])
def test_integrated_dm_rehearsal_requires_live_invite_only_run(args):
    result = CliRunner().invoke(cli, args)
    assert result.exit_code == 2
    assert "requires --skip-dms in a live daily run" in result.output
