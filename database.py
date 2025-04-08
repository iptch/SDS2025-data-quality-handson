import glob
import os
import re
import sqlite3
import sys
from typing import Dict, List, Optional, Set, Tuple, Union, Any
from loguru import logger


def connect_to_db(db_path: str) -> sqlite3.Connection:
    """Connect to the SQLite database."""
    try:
        connection = sqlite3.connect(db_path)
        connection.execute("PRAGMA foreign_keys = ON")
        return connection
    except Exception as e:
        logger.error(f"Error connecting to database: {e}")
        sys.exit(1)


def setup_migration_tracking(conn: sqlite3.Connection) -> None:
    """Create migration tracking table if it doesn't exist."""
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS migration_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            filename TEXT UNIQUE NOT NULL,
            dir_prefix TEXT NOT NULL,
            applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """
    )
    conn.commit()


def get_applied_migrations(conn: sqlite3.Connection) -> List[Tuple[str, str]]:
    """Get list of already applied migrations."""
    cursor = conn.cursor()
    cursor.execute("SELECT filename, dir_prefix FROM migration_history ORDER BY id")
    return [(row[0], row[1]) for row in cursor.fetchall()]


def reset_database(conn: sqlite3.Connection) -> None:
    """Reset the database by dropping all tables."""
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")

    for table in cursor.fetchall():
        if table[0] != "sqlite_sequence":
            cursor.execute(f"DROP TABLE IF EXISTS {table[0]};")

    setup_migration_tracking(conn)
    conn.commit()
    logger.info("Database reset completed")


def apply_migration(conn: sqlite3.Connection, sql_file: str, dir_prefix: str) -> bool:
    """Apply SQL file and record it in history."""
    try:
        cursor = conn.cursor()
        filename = os.path.basename(sql_file)

        with open(sql_file, "r") as f:
            sql_content = f.read()

        for statement in sql_content.split(";"):
            statement = statement.strip()
            if statement:
                try:
                    cursor.execute(statement)
                except sqlite3.OperationalError as e:
                    if "already exists" in str(e):
                        logger.warning(f"{e} in {filename} - continuing")
                    else:
                        raise
                except sqlite3.IntegrityError as e:
                    if "UNIQUE constraint failed" in str(e):
                        logger.warning(f"Skipped duplicate record in {filename}: {e}")
                    else:
                        raise

        cursor.execute(
            "INSERT INTO migration_history (filename, dir_prefix) VALUES (?, ?)",
            (filename, dir_prefix)
        )

        conn.commit()
        logger.info(f"Migration {filename} from {dir_prefix} - Successfully applied")
        return True
    except Exception as e:
        conn.rollback()
        logger.error(f"Error applying migration {sql_file}: {e}")
        return False


def get_all_data_directories(base_dir: str = "data") -> List[Tuple[int, str]]:
    """Find all numbered data directories and sort them."""
    pattern = re.compile(r"^(\d+)_")
    dirs: List[Tuple[int, str]] = []

    try:
        for item in os.listdir(base_dir):
            item_path = os.path.join(base_dir, item)
            if os.path.isdir(item_path) and pattern.match(item):
                prefix = int(pattern.match(item).group(1))
                dirs.append((prefix, item_path))
        return sorted(dirs)
    except FileNotFoundError:
        logger.error(f"Base directory '{base_dir}' not found")
        return []


def get_sql_files_in_dir(directory: str) -> List[str]:
    """Get all SQL files in a directory."""
    return sorted(glob.glob(os.path.join(directory, "*.sql")))


# Global variables
_conn: Optional[sqlite3.Connection] = None
_data_dir: str = "data"
_db_path: str = "database.db"


def init(db_path: str = "database.db", data_dir: str = "data") -> bool:
    """Initialize database connection and setup migration tracking"""
    global _conn, _data_dir, _db_path

    _db_path = db_path
    _data_dir = data_dir

    if _conn:
        _conn.close()

    _conn = connect_to_db(db_path)
    reset_database(_conn)

    data_dirs = get_all_data_directories(_data_dir)
    step0_dir = next((dir_path for prefix, dir_path in data_dirs if prefix == 0), None)

    if step0_dir:
        dir_name = os.path.basename(step0_dir)
        logger.info(f"Initializing database to step 0: {dir_name}")

        for sql_file in get_sql_files_in_dir(step0_dir):
            apply_migration(_conn, sql_file, dir_name)
    else:
        logger.warning("No directory with prefix '0_' found. Database initialized but empty.")

    return True


def status() -> Union[Dict[str, Any], bool]:
    """Show applied migrations status"""
    global _conn, _data_dir

    if not _conn:
        logger.error("Database not initialized. Call init() first.")
        return False

    data_dirs = get_all_data_directories(_data_dir)
    if not data_dirs:
        logger.error(f"No numbered data directories found in '{_data_dir}'")
        return False

    applied_migrations = get_applied_migrations(_conn)
    result: Dict[str, Any] = {"total_applied": len(applied_migrations), "directories": {}}
    logger.info(f"Applied migrations: {len(applied_migrations)}")

    dir_counts: Dict[str, int] = {}
    for _, dir_prefix in applied_migrations:
        dir_counts[dir_prefix] = dir_counts.get(dir_prefix, 0) + 1

    for _, dir_path in data_dirs:
        dir_name = os.path.basename(dir_path)
        count = dir_counts.get(dir_name, 0)
        total = len(get_sql_files_in_dir(dir_path))
        logger.info(f"Directory {dir_name}: {count}/{total} files applied")
        result["directories"][dir_name] = {"applied": count, "total": total}

    return result


def set_step(target_step: int) -> bool:
    """Apply all migrations up to and including the target step"""
    global _conn, _data_dir

    if not _conn:
        logger.error("Database not initialized. Call init() first.")
        return False

    data_dirs = get_all_data_directories(_data_dir)
    if not data_dirs:
        logger.error(f"No numbered data directories found in '{_data_dir}'")
        return False

    applied_migrations = get_applied_migrations(_conn)
    applied_files: Dict[str, str] = {filename: dir_prefix for filename, dir_prefix in applied_migrations}

    applied_step_prefixes: Set[int] = set()
    for _, dir_prefix in applied_migrations:
        match = re.match(r'^(\d+)_', dir_prefix)
        if match:
            applied_step_prefixes.add(int(match.group(1)))

    max_applied_step = max(applied_step_prefixes) if applied_step_prefixes else -1
    available_steps = [prefix for prefix, _ in data_dirs]

    if target_step not in available_steps:
        logger.error(f"Step {target_step} not found. Available steps: {available_steps}")
        return False

    if target_step < max_applied_step:
        logger.error(
            f"Cannot set to step {target_step} when migrations for step {max_applied_step} are already applied.")
        logger.error("You should call init() first to reset the database.")
        return False

    for prefix, dir_path in data_dirs:
        if prefix <= target_step:
            dir_name = os.path.basename(dir_path)
            logger.info(f"Processing directory: {dir_name}")

            for sql_file in get_sql_files_in_dir(dir_path):
                filename = os.path.basename(sql_file)
                if filename not in applied_files or applied_files[filename] != dir_name:
                    apply_migration(_conn, sql_file, dir_name)
                else:
                    logger.debug(f"Skipping {filename} (already applied)")

        if prefix == target_step:
            break

    return True


def close() -> bool:
    """Close the database connection"""
    global _conn
    if _conn:
        _conn.close()
        _conn = None
        return True
    return False


if __name__ == "__main__":
    # Configure logger
    logger.remove()
    logger.add(sys.stderr, format="{time} | {level} | {message}", level="INFO")
    logger.add("database.log", rotation="10 MB", level="DEBUG")

    init()