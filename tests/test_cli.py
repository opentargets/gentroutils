"""CLI tests."""

from click.testing import CliRunner
from gentroutils import cli
import pytest


@pytest.mark.integration_test
def test_help():
    """Test --help flag."""
    runner = CliRunner()
    result = runner.invoke(cli, ["--help"])
    assert result.exit_code == 0
    assert "Gentroutils Command Line Interface." in result.output


@pytest.mark.integration_test
def test_run_without_command():
    """Test run without flags and commands."""
    runner = CliRunner()
    result = runner.invoke(cli)
    print(result.output)
    assert "Gentroutils Command Line Interface." in result.output
