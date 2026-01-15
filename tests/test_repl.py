"""
Basic tests for REPL functionality
Run with: python tests/test_repl.py
"""

import sys
import os
import shutil
from pathlib import Path
from io import StringIO

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from simpldb.repl import SimplDBREPL


def cleanup():
    """Clean up test data"""
    test_dir = Path("test_repl_data")
    if test_dir.exists():
        shutil.rmtree(test_dir)


def test_repl_creation():
    """Test REPL initialization"""
    print("Testing REPL creation...")
    
    cleanup()
    repl = SimplDBREPL(db_name="test_repl", data_dir="test_repl_data")
    
    assert repl.db is not None
    assert repl.running is True
    assert repl.db.name == "test_repl"
    
    repl.exit_repl()
    cleanup()
    print("✅ REPL creation test passed")


def test_execute_sql():
    """Test SQL execution through REPL"""
    print("\nTesting SQL execution...")
    
    cleanup()
    repl = SimplDBREPL(data_dir="test_repl_data")
    
    # Capture output
    old_stdout = sys.stdout
    sys.stdout = StringIO()
    
    # Create table
    repl.execute_sql("CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(100));")
    output = sys.stdout.getvalue()
    
    sys.stdout = old_stdout
    
    assert "created successfully" in output.lower() or "executed" in output.lower()
    assert "users" in repl.db.list_tables()
    
    repl.exit_repl()
    cleanup()
    print("✅ SQL execution test passed")


def test_meta_commands():
    """Test meta commands"""
    print("\nTesting meta commands...")
    
    cleanup()
    repl = SimplDBREPL(data_dir="test_repl_data")
    
    # Create test table
    repl.db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(100))")
    repl.db.execute("INSERT INTO users (id, name) VALUES (1, 'Alice')")
    
    # Test .tables
    old_stdout = sys.stdout
    sys.stdout = StringIO()
    
    repl.show_tables([])
    output = sys.stdout.getvalue()
    
    sys.stdout = old_stdout
    
    assert "users" in output
    
    repl.exit_repl()
    cleanup()
    print("✅ Meta commands test passed")


def test_display_results():
    """Test result display"""
    print("\nTesting result display...")
    
    cleanup()
    repl = SimplDBREPL(data_dir="test_repl_data")
    
    # Create and populate table
    repl.db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(100), age INTEGER)")
    repl.db.execute("INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)")
    repl.db.execute("INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25)")
    
    # Get results
    result = repl.db.execute("SELECT * FROM users")
    
    # Test display
    old_stdout = sys.stdout
    sys.stdout = StringIO()
    
    repl.display_results(result.rows)
    output = sys.stdout.getvalue()
    
    sys.stdout = old_stdout
    
    assert "Alice" in output
    assert "Bob" in output
    assert "id" in output  # Header
    
    repl.exit_repl()
    cleanup()
    print("✅ Result display test passed")


def test_describe_table():
    """Test describe table functionality"""
    print("\nTesting describe table...")
    
    cleanup()
    repl = SimplDBREPL(data_dir="test_repl_data")
    
    # Create table
    repl.db.execute("""
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            username VARCHAR(50) UNIQUE NOT NULL,
            age INTEGER
        )
    """)
    
    old_stdout = sys.stdout
    sys.stdout = StringIO()
    
    repl.describe_table(['users'])
    output = sys.stdout.getvalue()
    
    sys.stdout = old_stdout
    
    assert "users" in output.lower()
    assert "id" in output
    assert "username" in output
    assert "PRIMARY_KEY" in output or "primary" in output.lower()
    
    repl.exit_repl()
    cleanup()
    print("✅ Describe table test passed")


def test_stats():
    """Test statistics display"""
    print("\nTesting statistics...")
    
    cleanup()
    repl = SimplDBREPL(data_dir="test_repl_data")
    
    # Create and populate table
    repl.db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(100))")
    repl.db.execute("INSERT INTO users (id, name) VALUES (1, 'Alice')")
    repl.db.execute("INSERT INTO users (id, name) VALUES (2, 'Bob')")
    
    old_stdout = sys.stdout
    sys.stdout = StringIO()
    
    repl.show_stats([])
    output = sys.stdout.getvalue()
    
    sys.stdout = old_stdout
    
    assert "users" in output.lower()
    assert "2" in output  # Row count
    
    repl.exit_repl()
    cleanup()
    print("✅ Statistics test passed")


def test_export_import():
    """Test schema export/import"""
    print("\nTesting export/import...")
    
    cleanup()
    repl = SimplDBREPL(data_dir="test_repl_data")
    
    # Create table
    repl.db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(100))")
    
    # Export
    export_file = "test_repl_data/test_export.json"
    
    old_stdout = sys.stdout
    sys.stdout = StringIO()
    
    repl.export_schema([export_file])
    
    sys.stdout = old_stdout
    
    assert Path(export_file).exists()
    
    # Drop table
    repl.db.execute("DROP TABLE users")
    assert "users" not in repl.db.list_tables()
    
    # Import
    old_stdout = sys.stdout
    sys.stdout = StringIO()
    
    repl.import_schema([export_file])
    
    sys.stdout = old_stdout
    
    assert "users" in repl.db.list_tables()
    
    repl.exit_repl()
    cleanup()
    print("✅ Export/import test passed")


def run_all_tests():
    """Run all REPL tests"""
    print("=" * 70)
    print("Running REPL Tests")
    print("=" * 70)
    
    test_repl_creation()
    test_execute_sql()
    test_meta_commands()
    test_display_results()
    test_describe_table()
    test_stats()
    test_export_import()
    
    print("\n" + "=" * 70)
    print("✅ All REPL tests passed!")
    print("=" * 70)
    print("\nNote: Interactive features (readline, tab completion) require")
    print("manual testing. Run: python -m simpldb.repl")


if __name__ == "__main__":
    run_all_tests()