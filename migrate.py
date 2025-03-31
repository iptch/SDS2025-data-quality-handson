import yaml
import sqlite3
import argparse
import sys
from pathlib import Path
from datetime import datetime


def load_migrations(yaml_file):
    """Load migrations from YAML file."""
    with open(yaml_file, 'r') as file:
        data = yaml.safe_load(file)
    return data['migrations']


def connect_to_db(db_path):
    """Connect to the SQLite database."""
    try:
        connection = sqlite3.connect(db_path)
        # Enable foreign keys
        connection.execute("PRAGMA foreign_keys = ON")
        return connection
    except Exception as e:
        print(f"Error connecting to database: {e}")
        sys.exit(1)


def setup_migration_tracking(conn):
    """Create migration tracking table if it doesn't exist."""
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS migration_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            migration_id INTEGER UNIQUE NOT NULL,
            description TEXT,
            tag TEXT,
            applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()


def get_applied_migrations(conn):
    """Get list of already applied migrations."""
    cursor = conn.cursor()
    cursor.execute("SELECT migration_id FROM migration_history ORDER BY migration_id")
    return [row[0] for row in cursor.fetchall()]


def apply_migration(conn, migration):
    """Apply a single migration and record it in history."""
    try:
        cursor = conn.cursor()

        # SQLite-specific adjustments for SQL commands
        sql = migration['sql']
        # Replace PostgreSQL's SERIAL with SQLite's equivalent
        sql = sql.replace("SERIAL PRIMARY KEY", "INTEGER PRIMARY KEY AUTOINCREMENT")

        # Don't do generic replacements for CONCAT that could break other SQL
        # Only replace full CONCAT function calls if needed
        # This would need more sophisticated parsing for complex queries

        # Execute the SQL commands (splitting by semicolon to execute multiple statements)
        for statement in sql.split(';'):
            if statement.strip():
                cursor.execute(statement)

        # Record the migration in history table
        cursor.execute("""
            INSERT INTO migration_history (migration_id, description, tag)
            VALUES (?, ?, ?)
        """, (migration['id'], migration['description'], migration['tag']))

        conn.commit()
        print(f"Migration {migration['id']}: {migration['description']} - Successfully applied")
        return True
    except Exception as e:
        conn.rollback()
        print(f"Error applying migration {migration['id']}: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(description="SQLite Database Migration Tool")
    parser.add_argument("--db-path", default="database.db",
                        help="Path to SQLite database file (default: database.db)")
    parser.add_argument("--migrations-file", default="migrations.yaml",
                        help="Path to migrations YAML file")
    parser.add_argument("--next", action="store_true", help="Apply the next migration")
    parser.add_argument("--init", action="store_true", help="Initialize the database")
    parser.add_argument("--status", action="store_true", help="Show migration status")

    args = parser.parse_args()

    # Ensure migrations file exists
    migrations_path = Path(args.migrations_file)
    if not migrations_path.exists():
        print(f"Migrations file not found: {args.migrations_file}")
        sys.exit(1)

    # Load migrations
    migrations = load_migrations(args.migrations_file)

    # Connect to the database
    conn = connect_to_db(args.db_path)

    # Set up migration tracking
    setup_migration_tracking(conn)

    # Get applied migrations
    applied = get_applied_migrations(conn)

    if args.status:
        print(f"Total migrations: {len(migrations)}")
        print(f"Applied migrations: {len(applied)}")
        for m in migrations:
            status = "APPLIED" if m['id'] in applied else "PENDING"
            print(f"Migration {m['id']}: {m['tag']} - {m['description']} [{status}]")

    elif args.init:
        if applied:
            print("Database already initialized. Use --next to apply more migrations.")
        else:
            # Apply the initial migration (first one)
            if migrations:
                print(f"Initializing database with migration {migrations[0]['id']}")
                apply_migration(conn, migrations[0])
            else:
                print("No migrations found in file.")

    elif args.next:
        if not applied and migrations:
            print("Database not initialized. Use --init first.")
        else:
            # Find the next migration to apply
            next_id = max(applied) + 1 if applied else 1
            next_migration = next((m for m in migrations if m['id'] == next_id), None)

            if next_migration:
                print(f"Applying migration {next_migration['id']}: {next_migration['description']}")
                apply_migration(conn, next_migration)
            else:
                print("No more migrations to apply.")

    else:
        parser.print_help()

    conn.close()


if __name__ == "__main__":
    main()