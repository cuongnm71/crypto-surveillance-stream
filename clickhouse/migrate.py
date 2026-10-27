"""Idempotent database migration runner for ClickHouse Medallion schemas."""

from __future__ import annotations

import argparse
import logging
import os
import re
import sys
from collections.abc import Sequence
from pathlib import Path

import clickhouse_connect
from clickhouse_connect.driver.client import Client

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("clickhouse.migrate")

DEFAULT_MIGRATIONS_DIR = Path(__file__).resolve().parent / "migrations"
_IDENTIFIER_PATTERN = re.compile(r"^[a-zA-Z0-9_]+$")


def validate_identifier(name: str) -> str:
    """Validate that an identifier contains only alphanumeric and underscore characters."""
    if not _IDENTIFIER_PATTERN.match(name):
        raise ValueError(
            f"Invalid identifier '{name}': only alphanumeric characters and '_' allowed"
        )
    return name


def parse_sql_statements(sql_content: str) -> list[str]:
    """Strip SQL comments and split into individual executable statements."""
    content_without_block_comments = re.sub(r"/\*.*?\*/", "", sql_content, flags=re.DOTALL)
    lines: list[str] = []
    for line in content_without_block_comments.splitlines():
        stripped_line = re.sub(r"--.*$", "", line).strip()
        if stripped_line:
            lines.append(stripped_line)

    clean_content = " ".join(lines)
    raw_statements = [stmt.strip() for stmt in clean_content.split(";")]
    return [stmt for stmt in raw_statements if stmt]


def ensure_database_and_tracker(
    host: str,
    port: int,
    username: str,
    password: str,
    database: str,
) -> None:
    """Ensure target database and schema_migrations tracking table exist."""
    valid_db = validate_identifier(database)
    root_client = clickhouse_connect.get_client(
        host=host,
        port=port,
        username=username,
        password=password,
    )
    root_client.command(f"CREATE DATABASE IF NOT EXISTS {valid_db}")

    db_client = clickhouse_connect.get_client(
        host=host,
        port=port,
        username=username,
        password=password,
        database=valid_db,
    )
    db_client.command(
        """
        CREATE TABLE IF NOT EXISTS schema_migrations (
            name String,
            applied_at DateTime DEFAULT now()
        ) ENGINE = MergeTree()
        ORDER BY name
        """
    )


def get_applied_migrations(client: Client) -> set[str]:
    """Retrieve the set of migration file names already recorded in schema_migrations."""
    result = client.query("SELECT name FROM schema_migrations")
    return {str(row[0]) for row in result.result_rows}


def apply_migration_file(client: Client, file_path: Path) -> None:
    """Read and sequentially execute all DDL statements in a migration file."""
    logger.info("Applying migration: %s", file_path.name)
    sql_text = file_path.read_text(encoding="utf-8")
    statements = parse_sql_statements(sql_text)

    for idx, stmt in enumerate(statements, start=1):
        logger.debug("Executing [%s] statement %d/%d", file_path.name, idx, len(statements))
        client.command(stmt)

    client.insert("schema_migrations", [[file_path.name]], column_names=["name"])
    logger.info("Successfully applied: %s", file_path.name)


def run_migrations(
    host: str = "localhost",
    port: int = 8123,
    username: str = "default",
    password: str = "",
    database: str = "crypto",
    migrations_dir: Path | None = None,
) -> list[str]:
    """Execute all pending SQL migrations in alphabetical order."""
    valid_db = validate_identifier(database)
    target_dir = migrations_dir or DEFAULT_MIGRATIONS_DIR
    if not target_dir.is_dir():
        raise FileNotFoundError(f"Migrations directory not found: {target_dir}")

    ensure_database_and_tracker(
        host=host,
        port=port,
        username=username,
        password=password,
        database=valid_db,
    )

    client = clickhouse_connect.get_client(
        host=host,
        port=port,
        username=username,
        password=password,
        database=valid_db,
    )

    applied_set = get_applied_migrations(client)
    migration_files = sorted(target_dir.glob("*.sql"))

    newly_applied: list[str] = []
    for file_path in migration_files:
        if file_path.name in applied_set:
            logger.info("Skipping already applied migration: %s", file_path.name)
            continue

        apply_migration_file(client, file_path)
        newly_applied.append(file_path.name)

    logger.info(
        "Migration complete. Total applied in this run: %d, Total tracked: %d",
        len(newly_applied),
        len(applied_set) + len(newly_applied),
    )
    return newly_applied


def main(argv: Sequence[str] | None = None) -> int:
    """CLI entrypoint for running ClickHouse schema migrations."""
    parser = argparse.ArgumentParser(description="ClickHouse Schema Migration CLI")
    parser.add_argument(
        "--host",
        default=os.getenv("CLICKHOUSE_HOST", "localhost"),
        help="ClickHouse HTTP host (default: localhost)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.getenv("CLICKHOUSE_PORT", "8123")),
        help="ClickHouse HTTP port (default: 8123)",
    )
    parser.add_argument(
        "--user",
        default=os.getenv("CLICKHOUSE_USER", "default"),
        help="ClickHouse user (default: default)",
    )
    parser.add_argument(
        "--password",
        default=os.getenv("CLICKHOUSE_PASSWORD", ""),
        help="ClickHouse password",
    )
    parser.add_argument(
        "--database",
        default=os.getenv("CLICKHOUSE_DB", "crypto"),
        help="Target database name (default: crypto)",
    )
    parser.add_argument(
        "--migrations-dir",
        type=Path,
        default=DEFAULT_MIGRATIONS_DIR,
        help="Directory containing .sql migration files",
    )

    args = parser.parse_args(argv)

    try:
        run_migrations(
            host=args.host,
            port=args.port,
            username=args.user,
            password=args.password,
            database=args.database,
            migrations_dir=args.migrations_dir,
        )
        return 0
    except Exception as exc:
        logger.error("Migration failed: %s", exc, exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
