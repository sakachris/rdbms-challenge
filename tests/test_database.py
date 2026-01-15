"""
Unit tests for Database Core
Run with: python tests/test_database.py
"""

import sys
import os
import shutil
from pathlib import Path

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from simpldb.database import Database, DatabaseCatalog


def cleanup():
    """Clean up test data"""
    test_dir = Path("test_db_data")
    if test_dir.exists():
        shutil.rmtree(test_dir)


def test_database_creation():
    """Test database initialization"""
    print("Testing database creation...")
    
    cleanup()
    db = Database(name="testdb", data_dir="test_db_data")
    
    assert db.name == "testdb"
    assert db.data_dir.exists()
    assert db.catalog.catalog_file.exists()
    
    db.close()
    cleanup()
    print("✅ Database creation test passed")


def test_execute_create_table():
    """Test CREATE TABLE via Database.execute()"""
    print("\nTesting CREATE TABLE execution...")
    
    cleanup()
    db = Database(data_dir="test_db_data")
    
    sql = "CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(100))"
    result = db.execute(sql)
    
    assert result.success
    assert "users" in db.list_tables()
    
    db.close()
    cleanup()
    print("✅ CREATE TABLE execution test passed")


def test_execute_insert_select():
    """Test INSERT and SELECT"""
    print("\nTesting INSERT and SELECT...")
    
    cleanup()
    db = Database(data_dir="test_db_data")
    
    # Create table
    db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(100), age INTEGER)")
    
    # Insert data
    result = db.execute("INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)")
    assert result.success
    assert result.rows_affected == 1
    
    # Select data
    result = db.execute("SELECT * FROM users")
    assert result.success
    assert len(result.rows) == 1
    assert result.rows[0]['name'] == 'Alice'
    
    db.close()
    cleanup()
    print("✅ INSERT and SELECT test passed")


def test_execute_update_delete():
    """Test UPDATE and DELETE"""
    print("\nTesting UPDATE and DELETE...")
    
    cleanup()
    db = Database(data_dir="test_db_data")
    
    # Setup
    db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(100), age INTEGER)")
    db.execute("INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)")
    db.execute("INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25)")
    
    # Update
    result = db.execute("UPDATE users SET age = 31 WHERE id = 1")
    assert result.success
    assert result.rows_affected == 1
    
    # Verify update
    result = db.execute("SELECT age FROM users WHERE id = 1")
    assert result.rows[0]['age'] == 31
    
    # Delete
    result = db.execute("DELETE FROM users WHERE id = 2")
    assert result.success
    assert result.rows_affected == 1
    
    # Verify delete
    result = db.execute("SELECT * FROM users")
    assert len(result.rows) == 1
    
    db.close()
    cleanup()
    print("✅ UPDATE and DELETE test passed")


def test_execute_many():
    """Test execute_many()"""
    print("\nTesting execute_many...")
    
    cleanup()
    db = Database(data_dir="test_db_data")
    
    sqls = [
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(100))",
        "INSERT INTO users (id, name) VALUES (1, 'Alice')",
        "INSERT INTO users (id, name) VALUES (2, 'Bob')",
        "INSERT INTO users (id, name) VALUES (3, 'Charlie')"
    ]
    
    results = db.execute_many(sqls)
    
    assert len(results) == 4
    assert all(r.success for r in results)
    
    # Verify
    result = db.execute("SELECT * FROM users")
    assert len(result.rows) == 3
    
    db.close()
    cleanup()
    print("✅ execute_many test passed")


def test_list_tables():
    """Test list_tables()"""
    print("\nTesting list_tables...")
    
    cleanup()
    db = Database(data_dir="test_db_data")
    
    # Initially empty
    assert len(db.list_tables()) == 0
    
    # Create tables
    db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY)")
    db.execute("CREATE TABLE posts (id INTEGER PRIMARY KEY)")
    
    tables = db.list_tables()
    assert len(tables) == 2
    assert "users" in tables
    assert "posts" in tables
    
    db.close()
    cleanup()
    print("✅ list_tables test passed")


def test_describe_table():
    """Test describe_table()"""
    print("\nTesting describe_table...")
    
    cleanup()
    db = Database(data_dir="test_db_data")
    
    db.execute("""
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            age INTEGER
        )
    """)
    
    info = db.describe_table("users")
    
    assert info is not None
    assert 'columns' in info
    assert len(info['columns']) == 3
    assert info['columns'][0]['name'] == 'id'
    assert 'PRIMARY_KEY' in info['columns'][0]['constraints']
    
    # Non-existent table
    info = db.describe_table("nonexistent")
    assert info is None
    
    db.close()
    cleanup()
    print("✅ describe_table test passed")


def test_table_stats():
    """Test get_table_stats()"""
    print("\nTesting get_table_stats...")
    
    cleanup()
    db = Database(data_dir="test_db_data")
    
    db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(100))")
    db.execute("INSERT INTO users (id, name) VALUES (1, 'Alice')")
    db.execute("INSERT INTO users (id, name) VALUES (2, 'Bob')")
    db.execute("CREATE INDEX idx_name ON users(name)")
    
    stats = db.get_table_stats("users")
    
    assert stats is not None
    assert stats['table_name'] == 'users'
    assert stats['row_count'] == 2
    assert len(stats['indexes']) == 2  # id (primary key) and name
    
    db.close()
    cleanup()
    print("✅ get_table_stats test passed")


def test_database_info():
    """Test get_database_info()"""
    print("\nTesting get_database_info...")
    
    cleanup()
    db = Database(name="testdb", data_dir="test_db_data")
    
    db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY)")
    db.execute("CREATE TABLE posts (id INTEGER PRIMARY KEY)")
    db.execute("INSERT INTO users (id) VALUES (1)")
    db.execute("INSERT INTO posts (id) VALUES (1)")
    
    info = db.get_database_info()
    
    assert info['name'] == 'testdb'
    assert info['total_tables'] == 2
    assert info['total_rows'] == 2
    
    db.close()
    cleanup()
    print("✅ get_database_info test passed")


def test_catalog_persistence():
    """Test catalog persistence across sessions"""
    print("\nTesting catalog persistence...")
    
    cleanup()
    
    # Create database and table
    db1 = Database(data_dir="test_db_data")
    db1.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(100))")
    db1.execute("INSERT INTO users (id, name) VALUES (1, 'Alice')")
    db1.close()
    
    # Open new database instance
    db2 = Database(data_dir="test_db_data")
    
    # Check that schema was loaded
    assert "users" in db2.list_tables()
    
    # Check that data persists
    result = db2.execute("SELECT * FROM users")
    assert len(result.rows) == 1
    assert result.rows[0]['name'] == 'Alice'
    
    db2.close()
    cleanup()
    print("✅ Catalog persistence test passed")


def test_transactions():
    """Test basic transaction management"""
    print("\nTesting transactions...")
    
    cleanup()
    db = Database(data_dir="test_db_data")
    
    # Begin transaction
    tx_id = db.begin_transaction()
    assert tx_id > 0
    assert tx_id in db.transactions
    
    # Commit transaction
    success = db.commit_transaction(tx_id)
    assert success
    assert not db.transactions[tx_id].is_active
    
    # Rollback transaction
    tx_id2 = db.begin_transaction()
    success = db.rollback_transaction(tx_id2)
    assert success
    assert not db.transactions[tx_id2].is_active
    
    db.close()
    cleanup()
    print("✅ Transactions test passed")


def test_schema_export_import():
    """Test schema export and import"""
    print("\nTesting schema export/import...")
    
    cleanup()
    db = Database(data_dir="test_db_data")
    
    # Create schema
    db.execute("""
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            age INTEGER
        )
    """)
    db.execute("CREATE INDEX idx_age ON users(age)")
    
    # Export schema
    export_file = "test_db_data/test_schema.json"
    db.export_schema(export_file)
    
    assert Path(export_file).exists()
    
    # Drop table
    db.execute("DROP TABLE users")
    assert "users" not in db.list_tables()
    
    # Import schema
    results = db.import_schema(export_file)
    
    assert all(r.success for r in results)
    assert "users" in db.list_tables()
    
    # Verify structure
    info = db.describe_table("users")
    assert len(info['columns']) == 3
    
    db.close()
    cleanup()
    print("✅ Schema export/import test passed")


def test_catalog_operations():
    """Test catalog operations"""
    print("\nTesting catalog operations...")
    
    cleanup()
    db = Database(data_dir="test_db_data")
    
    # Create table
    db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(100))")
    
    # Check catalog
    assert "users" in db.catalog.get_all_tables()
    
    table_info = db.catalog.get_table_info("users")
    assert table_info is not None
    assert 'schema' in table_info
    
    # Create index
    db.execute("CREATE INDEX idx_name ON users(name)")
    
    indexes = db.catalog.get_all_indexes()
    assert len(indexes) >= 1
    
    # Set metadata
    db.catalog.set_metadata("custom_key", "custom_value")
    assert db.catalog.get_metadata("custom_key") == "custom_value"
    
    db.close()
    cleanup()
    print("✅ Catalog operations test passed")


def test_error_handling():
    """Test error handling"""
    print("\nTesting error handling...")
    
    cleanup()
    db = Database(data_dir="test_db_data")
    
    # Invalid SQL
    result = db.execute("INVALID SQL QUERY")
    assert not result.success
    
    # Table doesn't exist
    result = db.execute("SELECT * FROM nonexistent")
    assert not result.success
    
    # Duplicate table
    db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY)")
    result = db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY)")
    assert not result.success
    
    db.close()
    cleanup()
    print("✅ Error handling test passed")


def run_all_tests():
    """Run all database tests"""
    print("=" * 70)
    print("Running Database Core Tests")
    print("=" * 70)
    
    test_database_creation()
    test_execute_create_table()
    test_execute_insert_select()
    test_execute_update_delete()
    test_execute_many()
    test_list_tables()
    test_describe_table()
    test_table_stats()
    test_database_info()
    test_catalog_persistence()
    test_transactions()
    test_schema_export_import()
    test_catalog_operations()
    test_error_handling()
    
    print("\n" + "=" * 70)
    print("✅ All database core tests passed!")
    print("=" * 70)


if __name__ == "__main__":
    run_all_tests()