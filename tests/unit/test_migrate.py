"""Unit tests for ClickHouse migration runner and SQL parser utilities."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from clickhouse.migrate import (
    main,
    parse_sql_statements,
    run_migrations,
    validate_identifier,
)


class TestMigrateUnit:
    """Unit test suite verifying migration utility functions and CLI behavior."""

    def test_validate_identifier_accepts_valid_names(self) -> None:
        """Valid alphanumeric and underscore identifiers are returned unchanged."""
        assert validate_identifier("crypto") == "crypto"
        assert validate_identifier("silver_candles_1m") == "silver_candles_1m"
        assert validate_identifier("db_123") == "db_123"

    def test_validate_identifier_rejects_special_characters(self) -> None:
        """Identifiers containing SQL injection attempts or invalid characters raise ValueError."""
        with pytest.raises(ValueError, match="Invalid identifier"):
            validate_identifier("crypto; DROP TABLE users;")

        with pytest.raises(ValueError, match="Invalid identifier"):
            validate_identifier("db-name")

        with pytest.raises(ValueError, match="Invalid identifier"):
            validate_identifier("db name")

    def test_parse_sql_statements_handles_comments_and_semicolons(self) -> None:
        """Comments are stripped, empty lines ignored, and distinct statements split."""
        raw_sql = """
        -- Single line comment
        CREATE DATABASE IF NOT EXISTS crypto;

        /* Multi-line block comment
           explaining table structure */
        CREATE TABLE crypto.test (
            id UInt64,
            name String -- inline comment
        ) ENGINE = MergeTree()
        ORDER BY id;

        -- Trailing comment without statement
        """
        statements = parse_sql_statements(raw_sql)
        assert len(statements) == 2
        assert statements[0] == "CREATE DATABASE IF NOT EXISTS crypto"
        assert "CREATE TABLE crypto.test" in statements[1]
        assert "Multi-line block comment" not in statements[1]

    def test_run_migrations_raises_on_missing_directory(self, tmp_path: Path) -> None:
        """Non-existent migrations directory triggers FileNotFoundError."""
        non_existent_dir = tmp_path / "does_not_exist"
        with pytest.raises(FileNotFoundError, match="Migrations directory not found"):
            run_migrations(migrations_dir=non_existent_dir)

    def test_main_cli_success(self) -> None:
        """CLI main returns 0 on successful migration run."""
        with patch("clickhouse.migrate.run_migrations", return_value=["001_bronze.sql"]):
            exit_code = main(["--host", "localhost", "--port", "8123"])
            assert exit_code == 0

    def test_main_cli_failure_returns_one(self) -> None:
        """CLI main returns 1 and logs error when exception is raised."""
        with patch(
            "clickhouse.migrate.run_migrations",
            side_effect=RuntimeError("Connection lost"),
        ):
            exit_code = main(["--host", "invalid_host"])
            assert exit_code == 1
