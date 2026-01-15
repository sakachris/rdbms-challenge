"""
Unit tests for the Index Manager
Run with: python tests/test_indexes.py
"""

import sys
import os
import shutil
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from simpldb.indexes import BTreeNode, Index, IndexManager


def cleanup():
    """Clean up test data"""
    test_dir = Path("test_index_data")
    if test_dir.exists():
        shutil.rmtree(test_dir)


def test_btree_node():
    """Test BTreeNode operations"""
    print("Testing BTreeNode...")
    
    node = BTreeNode()
    
    # Insert values
    node.insert(5, 1)
    node.insert(3, 2)
    node.insert(7, 3)
    node.insert(3, 4)  # Duplicate key, different row_id
    
    assert node.keys == [3, 5, 7]
    assert 2 in node.values[0]  # Key 3 has row_id 2
    assert 4 in node.values[0]  # Key 3 has row_id 4
    
    # Search
    results = node.search(3)
    assert 2 in results and 4 in results
    
    results = node.search(10)
    assert results == []
    
    # Delete specific row_id
    node.delete(3, 2)
    results = node.search(3)
    assert 2 not in results and 4 in results
    
    # Delete entire key
    node.delete(3)
    results = node.search(3)
    assert results == []
    
    print("✅ BTreeNode test passed")


def test_btree_range_search():
    """Test range search in BTreeNode"""
    print("\nTesting BTreeNode range search...")
    
    node = BTreeNode()
    
    # Insert values
    for i in range(10):
        node.insert(i, i)
    
    # Range search [3, 7]
    results = node.range_search(3, 7)
    assert sorted(results) == [3, 4, 5, 6, 7]
    
    # Range search [3, 7)
    results = node.range_search(3, 7, include_end=False)
    assert sorted(results) == [3, 4, 5, 6]
    
    # Range search (3, 7]
    results = node.range_search(3, 7, include_start=False)
    assert sorted(results) == [4, 5, 6, 7]
    
    print("✅ BTreeNode range search test passed")


def test_index_basic():
    """Test basic Index operations"""
    print("\nTesting Index basic operations...")
    
    idx = Index("users", "email", unique=True)
    
    # Insert
    success, error = idx.insert("alice@example.com", 1)
    assert success
    
    success, error = idx.insert("bob@example.com", 2)
    assert success
    
    # Search
    results = idx.search("alice@example.com")
    assert results == [1]
    
    results = idx.search("unknown@example.com")
    assert results == []
    
    # Update
    success, error = idx.update("alice@example.com", "alice.new@example.com", 1)
    assert success
    
    results = idx.search("alice.new@example.com")
    assert results == [1]
    
    results = idx.search("alice@example.com")
    assert results == []
    
    # Delete
    idx.delete("bob@example.com", 2)
    results = idx.search("bob@example.com")
    assert results == []
    
    print("✅ Index basic operations test passed")


def test_index_unique_constraint():
    """Test unique constraint in Index"""
    print("\nTesting Index unique constraint...")
    
    idx = Index("users", "username", unique=True)
    
    # Insert first value
    success, error = idx.insert("alice", 1)
    assert success
    
    # Try to insert duplicate
    success, error = idx.insert("alice", 2)
    assert not success
    assert "constraint violation" in error.lower()
    
    # Non-unique index allows duplicates
    non_unique_idx = Index("users", "age", unique=False)
    success, _ = non_unique_idx.insert(30, 1)
    assert success
    
    success, _ = non_unique_idx.insert(30, 2)
    assert success
    
    results = non_unique_idx.search(30)
    assert 1 in results and 2 in results
    
    print("✅ Index unique constraint test passed")


def test_index_range_search():
    """Test range search in Index"""
    print("\nTesting Index range search...")
    
    idx = Index("products", "price", unique=False)
    
    # Insert prices
    prices = [(10.0, 1), (15.0, 2), (20.0, 3), (25.0, 4), (30.0, 5)]
    for price, row_id in prices:
        idx.insert(price, row_id)
    
    # Range search [15, 25]
    results = idx.range_search(15.0, 25.0)
    assert sorted(results) == [2, 3, 4]
    
    # Range search [15, 25)
    results = idx.range_search(15.0, 25.0, include_end=False)
    assert sorted(results) == [2, 3]
    
    # All values
    results = idx.range_search()
    assert sorted(results) == [1, 2, 3, 4, 5]
    
    print("✅ Index range search test passed")


def test_index_stats():
    """Test index statistics"""
    print("\nTesting Index statistics...")
    
    idx = Index("users", "age", unique=False)
    
    idx.insert(30, 1)
    idx.insert(25, 2)
    idx.insert(30, 3)  # Duplicate age
    
    stats = idx.get_stats()
    
    assert stats['table'] == 'users'
    assert stats['column'] == 'age'
    assert stats['unique'] == False
    assert stats['distinct_keys'] == 2  # 25 and 30
    assert stats['total_entries'] == 3  # 3 row_ids total
    
    print("✅ Index statistics test passed")


def test_index_serialization():
    """Test index serialization"""
    print("\nTesting Index serialization...")
    
    idx = Index("users", "email", unique=True)
    idx.insert("alice@example.com", 1)
    idx.insert("bob@example.com", 2)
    
    # Serialize
    idx_dict = idx.to_dict()
    
    # Deserialize
    restored = Index.from_dict(idx_dict)
    
    assert restored.table_name == "users"
    assert restored.column_name == "email"
    assert restored.unique == True
    
    results = restored.search("alice@example.com")
    assert results == [1]
    
    print("✅ Index serialization test passed")


def test_index_manager():
    """Test IndexManager"""
    print("\nTesting IndexManager...")
    
    cleanup()
    manager = IndexManager("test_index_data")
    
    # Create indexes
    idx1 = manager.create_index("users", "id", unique=True)
    idx2 = manager.create_index("users", "email", unique=True)
    idx3 = manager.create_index("users", "age", unique=False)
    
    assert manager.has_index("users", "id")
    assert manager.has_index("users", "email")
    assert manager.has_index("users", "age")
    assert not manager.has_index("users", "name")
    
    # List indexes
    indexes = manager.list_indexes("users")
    assert "id" in indexes
    assert "email" in indexes
    assert "age" in indexes
    
    # Try to create duplicate
    try:
        manager.create_index("users", "id", unique=True)
        assert False, "Should have raised ValueError"
    except ValueError as e:
        assert "already exists" in str(e)
    
    cleanup()
    print("✅ IndexManager test passed")


def test_index_manager_insert():
    """Test IndexManager insert operations"""
    print("\nTesting IndexManager insert operations...")
    
    cleanup()
    manager = IndexManager("test_index_data")
    
    manager.create_index("users", "id", unique=True)
    manager.create_index("users", "email", unique=True)
    manager.create_index("users", "age", unique=False)
    
    # Insert row
    row_data = {"id": 1, "email": "alice@example.com", "age": 30}
    success, error = manager.insert_into_indexes("users", 1, row_data)
    assert success
    
    # Verify insertion
    idx = manager.get_index("users", "email")
    results = idx.search("alice@example.com")
    assert results == [1]
    
    # Try duplicate
    row_data2 = {"id": 2, "email": "alice@example.com", "age": 25}
    success, error = manager.insert_into_indexes("users", 2, row_data2)
    assert not success
    assert "constraint violation" in error.lower()
    
    # Verify rollback - id should not be indexed
    id_idx = manager.get_index("users", "id")
    results = id_idx.search(2)
    assert results == []
    
    cleanup()
    print("✅ IndexManager insert test passed")


def test_index_manager_update():
    """Test IndexManager update operations"""
    print("\nTesting IndexManager update operations...")
    
    cleanup()
    manager = IndexManager("test_index_data")
    
    manager.create_index("users", "id", unique=True)
    manager.create_index("users", "email", unique=True)
    
    # Insert initial data
    manager.insert_into_indexes("users", 1, {"id": 1, "email": "alice@example.com"})
    manager.insert_into_indexes("users", 2, {"id": 2, "email": "bob@example.com"})
    
    # Update
    old_data = {"id": 1, "email": "alice@example.com"}
    new_data = {"id": 1, "email": "alice.new@example.com"}
    
    success, error = manager.update_indexes("users", 1, old_data, new_data)
    assert success
    
    # Verify update
    email_idx = manager.get_index("users", "email")
    assert email_idx.search("alice.new@example.com") == [1]
    assert email_idx.search("alice@example.com") == []
    
    # Try update to duplicate
    old_data2 = {"id": 2, "email": "bob@example.com"}
    new_data2 = {"id": 2, "email": "alice.new@example.com"}
    
    success, error = manager.update_indexes("users", 2, old_data2, new_data2)
    assert not success
    
    # Verify rollback - bob's email should still be indexed
    assert email_idx.search("bob@example.com") == [2]
    
    cleanup()
    print("✅ IndexManager update test passed")


def test_index_manager_delete():
    """Test IndexManager delete operations"""
    print("\nTesting IndexManager delete operations...")
    
    cleanup()
    manager = IndexManager("test_index_data")
    
    manager.create_index("users", "id", unique=True)
    manager.create_index("users", "email", unique=True)
    
    # Insert data
    manager.insert_into_indexes("users", 1, {"id": 1, "email": "alice@example.com"})
    
    # Delete
    manager.delete_from_indexes("users", 1, {"id": 1, "email": "alice@example.com"})
    
    # Verify deletion
    email_idx = manager.get_index("users", "email")
    assert email_idx.search("alice@example.com") == []
    
    id_idx = manager.get_index("users", "id")
    assert id_idx.search(1) == []
    
    cleanup()
    print("✅ IndexManager delete test passed")


def test_index_manager_rebuild():
    """Test index rebuilding"""
    print("\nTesting index rebuild...")
    
    cleanup()
    manager = IndexManager("test_index_data")
    
    manager.create_index("users", "age", unique=False)
    
    # Existing rows
    rows = [
        {"row_id": 1, "data": {"id": 1, "age": 30}},
        {"row_id": 2, "data": {"id": 2, "age": 25}},
        {"row_id": 3, "data": {"id": 3, "age": 30}}
    ]
    
    # Rebuild
    success = manager.rebuild_index("users", "age", rows)
    assert success
    
    # Verify
    age_idx = manager.get_index("users", "age")
    results = age_idx.search(30)
    assert sorted(results) == [1, 3]
    
    cleanup()
    print("✅ Index rebuild test passed")


def test_index_manager_persistence():
    """Test index persistence"""
    print("\nTesting index persistence...")
    
    cleanup()
    
    # Create manager and indexes
    manager1 = IndexManager("test_index_data")
    manager1.create_index("users", "email", unique=True)
    manager1.insert_into_indexes("users", 1, {"email": "alice@example.com"})
    
    # Create new manager instance (loads from disk)
    manager2 = IndexManager("test_index_data")
    
    # Verify data was persisted
    assert manager2.has_index("users", "email")
    email_idx = manager2.get_index("users", "email")
    results = email_idx.search("alice@example.com")
    assert results == [1]
    
    cleanup()
    print("✅ Index persistence test passed")


def test_index_manager_stats():
    """Test index statistics"""
    print("\nTesting index statistics...")
    
    cleanup()
    manager = IndexManager("test_index_data")
    
    manager.create_index("users", "age", unique=False)
    manager.insert_into_indexes("users", 1, {"age": 30})
    manager.insert_into_indexes("users", 2, {"age": 25})
    manager.insert_into_indexes("users", 3, {"age": 30})
    
    stats = manager.get_all_stats()
    
    assert "users" in stats
    user_stats = stats["users"]
    assert len(user_stats) == 1
    
    age_stats = user_stats[0]
    assert age_stats["column"] == "age"
    assert age_stats["distinct_keys"] == 2
    assert age_stats["total_entries"] == 3
    
    cleanup()
    print("✅ Index statistics test passed")


def run_all_tests():
    """Run all index tests"""
    print("=" * 60)
    print("Running Index Manager Tests")
    print("=" * 60)
    
    test_btree_node()
    test_btree_range_search()
    test_index_basic()
    test_index_unique_constraint()
    test_index_range_search()
    test_index_stats()
    test_index_serialization()
    test_index_manager()
    test_index_manager_insert()
    test_index_manager_update()
    test_index_manager_delete()
    test_index_manager_rebuild()
    test_index_manager_persistence()
    test_index_manager_stats()
    
    print("\n" + "=" * 60)
    print("✅ All index tests passed!")
    print("=" * 60)


if __name__ == "__main__":
    run_all_tests()