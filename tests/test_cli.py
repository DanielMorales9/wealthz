from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from wealthz.cli import cli  # update as needed

TEST_PIPELINE_YAML = """
schema: public
name: transactions_raw
datasource:
  type: gsheet
  sheet_id: dummy
  sheet_range: Transactions!A1:G
  credentials_file: dummy.json
columns:
  - name: Date
    type: string
"""


@pytest.fixture
def runner():
    return CliRunner()


def test_cli_run_command(runner, tmp_path):
    # Prepare dirs
    config_dir = tmp_path / "config"
    config_dir.mkdir(parents=True)
    config_file = config_dir / "example.yaml"
    config_file.write_text(TEST_PIPELINE_YAML)

    # Patch constants and dependencies
    with (
        patch("wealthz.cli.CONFIG_DIR", config_dir),
        patch("wealthz.cli.DUCKDB_META_PATH", tmp_path / "meta.duckdb"),
        patch("wealthz.cli.DUCKDB_DATA_PATH", tmp_path / "data"),
        patch("wealthz.cli.GoogleSheetFetcherFactory") as mock_factory,
        patch("wealthz.cli.DuckDBLoader") as mock_loader,
    ):
        # Mock return values
        mock_df = MagicMock(name="DataFrame")
        fetcher_instance = MagicMock()
        fetcher_instance.fetch.return_value = mock_df
        mock_factory.return_value.create.return_value = fetcher_instance

        loader_instance = MagicMock()
        mock_loader.return_value = loader_instance

        # Run CLI
        result = runner.invoke(cli, ["run", "example"])
        assert result.exit_code == 0, f"CLI failed: {result.output}"

        # Assertions
        mock_factory.assert_called_once()
        fetcher_instance.fetch.assert_called_once()
        loader_instance.load.assert_called_once()
