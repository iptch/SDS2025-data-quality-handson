import sqlite3
import sys
import os
import re
from pathlib import Path
import glob


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
            filename TEXT UNIQUE NOT NULL,
            dir_prefix TEXT NOT NULL,
            applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()


def get_applied_migrations(conn):
    """Get list of already applied migrations."""
    cursor = conn.cursor()
    cursor.execute("SELECT filename, dir_prefix FROM migration_history ORDER BY id")
    return [(row[0], row[1]) for row in cursor.fetchall()]


def reset_database(conn):
    """Reset the database by dropping all tables."""
    cursor = conn.cursor()
    # Get all tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    
    # Drop all tables
    for table in tables:
        table_name = table[0]
        if table_name != 'sqlite_sequence':  # Skip SQLite internal tables
            cursor.execute(f"DROP TABLE IF EXISTS {table_name};")
    
    # Recreate migration tracking table
    setup_migration_tracking(conn)
    conn.commit()
    print("Database reset completed")


def apply_migration(conn, sql_file, dir_prefix):
    """Apply SQL file and record it in history."""
    try:
        cursor = conn.cursor()
        filename = os.path.basename(sql_file)
        
        # Read SQL content
        with open(sql_file, 'r') as f:
            sql_content = f.read()
        
        # Execute the SQL statements (split by semicolon)
        for statement in sql_content.split(';'):
            statement = statement.strip()
            if statement:
                try:
                    cursor.execute(statement)
                except sqlite3.OperationalError as e:
                    # Handle table already exists error
                    if "already exists" in str(e):
                        print(f"Warning: {e} in {filename} - continuing")
                    else:
                        raise
                except sqlite3.IntegrityError as e:
                    # Log warning for integrity errors but continue
                    if "UNIQUE constraint failed" in str(e):
                        print(f"Warning: Skipped duplicate record in {filename}: {e}")
                    else:
                        raise
        
        # Record the migration in history table
        cursor.execute("""
            INSERT INTO migration_history (filename, dir_prefix)
            VALUES (?, ?)
        """, (filename, dir_prefix))

        conn.commit()
        print(f"Migration {filename} from {dir_prefix} - Successfully applied")
        return True
    except Exception as e:
        conn.rollback()
        print(f"Error applying migration {sql_file}: {e}")
        return False


def get_all_data_directories(base_dir="data"):
    """Find all numbered data directories and sort them."""
    pattern = re.compile(r'^(\d+)_')
    dirs = []
    
    # Get all immediate subdirectories in the base directory
    try:
        for item in os.listdir(base_dir):
            item_path = os.path.join(base_dir, item)
            if os.path.isdir(item_path) and pattern.match(item):
                # Extract the number prefix for sorting
                prefix = pattern.match(item).group(1)
                dirs.append((int(prefix), item_path))
        
        # Sort directories by their numeric prefix
        return sorted(dirs)
    except FileNotFoundError:
        print(f"Base directory '{base_dir}' not found")
        return []


def get_sql_files_in_dir(directory):
    """Get all SQL files in a directory."""
    return sorted(glob.glob(os.path.join(directory, "*.sql")))


# Global connection object
_conn = None
_data_dir = "data"
_db_path = "database.db"

def init(db_path="database.db", data_dir="data"):
    """Initialize database connection and setup migration tracking"""
    global _conn, _data_dir, _db_path
    
    _db_path = db_path
    _data_dir = data_dir
    
    # Close existing connection if any
    if _conn:
        _conn.close()
        
    # Connect to the database
    _conn = connect_to_db(db_path)
    
    # Reset database
    reset_database(_conn)
    
    # Get all data directories
    data_dirs = get_all_data_directories(_data_dir)
    
    # Find directory with prefix 0
    step0_dir = next((dir_path for prefix, dir_path in data_dirs if prefix == 0), None)
    
    if step0_dir:
        dir_name = os.path.basename(step0_dir)
        print(f"Initializing database to step 0: {dir_name}")
        
        # Apply all SQL files in the step 0 directory
        sql_files = get_sql_files_in_dir(step0_dir)
        for sql_file in sql_files:
            apply_migration(_conn, sql_file, dir_name)
    else:
        print("No directory with prefix '0_' found. Database initialized but empty.")
    
    return True


def status():
    """Show applied migrations status"""
    global _conn, _data_dir
    
    if not _conn:
        print("Database not initialized. Call init() first.")
        return False
        
    # Get all data directories
    data_dirs = get_all_data_directories(_data_dir)
    
    if not data_dirs:
        print(f"No numbered data directories found in '{_data_dir}'")
        return False

    # Get applied migrations
    applied_migrations = get_applied_migrations(_conn)
    
    result = {
        "total_applied": len(applied_migrations),
        "directories": {}
    }
    
    print(f"Applied migrations: {len(applied_migrations)}")
    
    # Group by directory
    dir_counts = {}
    for _, dir_prefix in applied_migrations:
        dir_counts[dir_prefix] = dir_counts.get(dir_prefix, 0) + 1
    
    # Print counts by directory
    for dir_num, dir_path in data_dirs:
        dir_name = os.path.basename(dir_path)
        count = dir_counts.get(dir_name, 0)
        total = len(get_sql_files_in_dir(dir_path))
        print(f"Directory {dir_name}: {count}/{total} files applied")
        
        result["directories"][dir_name] = {
            "applied": count,
            "total": total
        }
            
    return result


def set(target_step):
    """Apply all migrations up to and including the target step"""
    global _conn, _data_dir
    
    if not _conn:
        print("Database not initialized. Call init() first.")
        return False
    
    # Get all data directories
    data_dirs = get_all_data_directories(_data_dir)
    
    if not data_dirs:
        print(f"No numbered data directories found in '{_data_dir}'")
        return False

    # Get applied migrations
    applied_migrations = get_applied_migrations(_conn)
    applied_files = {filename: dir_prefix for filename, dir_prefix in applied_migrations}
        
    # Validate target step
    available_steps = [prefix for prefix, _ in data_dirs]
    if target_step not in available_steps:
        print(f"Step {target_step} not found. Available steps: {available_steps}")
        return False
        
    # Apply all migrations up to and including the target step
    for prefix, dir_path in data_dirs:
        dir_name = os.path.basename(dir_path)
        
        if prefix <= target_step:
            print(f"Processing directory: {dir_name}")
            sql_files = get_sql_files_in_dir(dir_path)
            
            for sql_file in sql_files:
                filename = os.path.basename(sql_file)
                # Apply only if not already applied
                if filename not in applied_files or applied_files[filename] != dir_name:
                    apply_migration(_conn, sql_file, dir_name)
                else:
                    print(f"Skipping {filename} (already applied)")
        
        if prefix == target_step:
            break
            
    return True


def reset():
    """Reset the database and apply migrations from directory starting with '0'"""
    global _conn, _data_dir
    
    if not _conn:
        print("Database not initialized. Call init() first.")
        return False
        
    # Get all data directories
    data_dirs = get_all_data_directories(_data_dir)
    
    if not data_dirs:
        print(f"No numbered data directories found in '{_data_dir}'")
        return False
        
    # Reset database
    reset_database(_conn)
    
    # Find directory starting with '0'
    initial_dir = next((dir_path for prefix, dir_path in data_dirs if prefix == 0), None)
    
    if not initial_dir:
        print("No directory starting with '0' found")
        return False
        
    # Apply all SQL files in the initial directory
    dir_name = os.path.basename(initial_dir)
    print(f"Applying SQL files from initial directory: {dir_name}")
    
    sql_files = get_sql_files_in_dir(initial_dir)
    if not sql_files:
        print(f"No SQL files found in {dir_name}")
        return False
    
    for sql_file in sql_files:
        apply_migration(_conn, sql_file, dir_name)
        
    return True


def close():
    """Close the database connection"""
    global _conn
    
    if _conn:
        _conn.close()
        _conn = None
        return True
    
    return False

if __name__ == "__main__":
    """Main function for testing"""
    init()