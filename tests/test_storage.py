"""
Unit tests for the Storage Layer
Run with: python -m pytest tests/test_storage.py -v
or simply: python tests/test_storage.py
"""

import sys
import os
import shutil
from pathlib import Path

# Add parent directory to path so we can import simpldb
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from simpldb.storage import Storage, TableStorage, Row


def setup_test_storage():
    """Create a clean test storage"""
    test_dir = Path("test_data")
    if test_dir.exists():
        shutil.rmtree(test_dir)
    return Storage("test_data")


def cleanup_test_storage():
    """Remove test data"""
    test_dir = Path("test_data")
    if test_dir.exists():
        shutil.rmtree(test_dir)


def test_row_creation():
    """Test Row class"""
    print("Testing Row creation...")
    row = Row({"name": "Alice", "age": 30}, row_id=1)
    assert row['name'] == "Alice"
    assert row['age'] == 30
    assert row.row_id == 1
    
    # Test dict-like access
    row['age'] = 31
    assert row['age'] == 31
    print("✅ Row creation test passed")


def test_insert_and_select():
    """Test insert and select operations"""
    print("\nTesting insert and select...")
    storage = setup_test_storage()
    users = storage.get_table("users")
    
    # Insert
    id1 = users.insert({"name": "Alice", "age": 30})
    id2 = users.insert({"name": "Bob", "age": 25})
    
    assert id1 == 1
    assert id2 == 2
    
    # Select all
    all_users = users.select_all()
    assert len(all_users) == 2
    
    # Select by ID
    alice = users.select_by_id(1)
    assert alice is not None
    assert alice['name'] == "Alice"
    
    cleanup_test_storage()
    print("✅ Insert and select test passed")


def test_update():
    """Test update operations"""
    print("\nTesting update...")
    storage = setup_test_storage()
    users = storage.get_table("users")
    
    id1 = users.insert({"name": "Alice", "age": 30})
    
    # Update
    success = users.update_by_id(id1, {"age": 31, "city": "NYC"})
    assert success is True
    
    # Verify update
    alice = users.select_by_id(id1)
    assert alice['age'] == 31
    assert alice['city'] == "NYC"
    
    # Update non-existent
    success = users.update_by_id(999, {"age": 40})
    assert success is False
    
    cleanup_test_storage()
    print("✅ Update test passed")


def test_delete():
    """Test delete operations"""
    print("\nTesting delete...")
    storage = setup_test_storage()
    users = storage.get_table("users")
    
    id1 = users.insert({"name": "Alice", "age": 30})
    id2 = users.insert({"name": "Bob", "age": 25})
    
    # Delete one
    success = users.delete_by_id(id1)
    assert success is True
    assert users.count() == 1
    
    # Delete non-existent
    success = users.delete_by_id(999)
    assert success is False
    
    # Delete all
    count = users.delete_all()
    assert count == 1
    assert users.count() == 0
    
    cleanup_test_storage()
    print("✅ Delete test passed")


def test_insert_many():
    """Test bulk insert"""
    print("\nTesting bulk insert...")
    storage = setup_test_storage()
    users = storage.get_table("users")
    
    data = [
        {"name": "Alice", "age": 30},
        {"name": "Bob", "age": 25},
        {"name": "Charlie", "age": 35},
    ]
    
    ids = users.insert_many(data)
    assert len(ids) == 3
    assert ids == [1, 2, 3]
    assert users.count() == 3
    
    cleanup_test_storage()
    print("✅ Bulk insert test passed")


def test_metadata():
    """Test metadata storage"""
    print("\nTesting metadata...")
    storage = setup_test_storage()
    users = storage.get_table("users")
    
    users.set_metadata("created_by", "admin")
    users.set_metadata("version", 1)
    
    assert users.get_metadata("created_by") == "admin"
    assert users.get_metadata("version") == 1
    assert users.get_metadata("nonexistent", "default") == "default"
    
    cleanup_test_storage()
    print("✅ Metadata test passed")


def test_table_management():
    """Test table creation and listing"""
    print("\nTesting table management...")
    storage = setup_test_storage()
    
    # Create tables
    storage.get_table("users")
    storage.get_table("posts")
    storage.get_table("comments")
    
    # List tables
    tables = storage.list_tables()
    assert len(tables) == 3
    assert "users" in tables
    assert "posts" in tables
    
    # Drop table
    storage.drop_table("comments")
    tables = storage.list_tables()
    assert len(tables) == 2
    assert "comments" not in tables
    
    cleanup_test_storage()
    print("✅ Table management test passed")


def test_persistence():
    """Test data persistence across storage instances"""
    print("\nTesting persistence...")
    
    # Create storage and insert data
    storage1 = Storage("test_data")
    users1 = storage1.get_table("users")
    users1.insert({"name": "Alice", "age": 30})
    
    # Create new storage instance and read data
    storage2 = Storage("test_data")
    users2 = storage2.get_table("users")
    all_users = users2.select_all()
    
    assert len(all_users) == 1
    assert all_users[0]['name'] == "Alice"
    
    cleanup_test_storage()
    print("✅ Persistence test passed")


def run_all_tests():
    """Run all tests"""
    print("=" * 60)
    print("Running Storage Layer Tests")
    print("=" * 60)
    
    test_row_creation()
    test_insert_and_select()
    test_update()
    test_delete()
    test_insert_many()
    test_metadata()
    test_table_management()
    test_persistence()
    
    print("\n" + "=" * 60)
    print("✅ All tests passed!")
    print("=" * 60)


if __name__ == "__main__":
    run_all_tests()