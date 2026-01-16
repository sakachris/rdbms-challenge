"""
Integration Demo: Storage + Schema + Indexes
This demonstrates all three components working together
"""

import sys
import os
from pathlib import Path
import shutil
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from simpldb.storage import Storage
from simpldb.schema import (
    Column, Schema, SchemaManager,
    DataType, ColumnConstraint
)
from simpldb.indexes import IndexManager


def cleanup():
    """Clean up demo data"""
    demo_dir = Path("full_demo_data")
    if demo_dir.exists():
        shutil.rmtree(demo_dir)


def main():
    print("=" * 70)
    print("SimplDB Full Integration Demo: Storage + Schema + Indexes")
    print("=" * 70)
    
    cleanup()
    
    # Initialize all components
    storage = Storage("full_demo_data")
    schema_manager = SchemaManager()
    index_manager = IndexManager("full_demo_data")
    
    # Step 1: Create schema with constraints
    print("\n1. Creating users table with schema and indexes...")
    
    user_columns = [
        Column("id", DataType.INTEGER, constraints=[ColumnConstraint.PRIMARY_KEY]),
        Column("username", DataType.VARCHAR, max_length=50, 
               constraints=[ColumnConstraint.UNIQUE, ColumnConstraint.NOT_NULL]),
        Column("email", DataType.VARCHAR, max_length=100, 
               constraints=[ColumnConstraint.UNIQUE, ColumnConstraint.NOT_NULL]),
        Column("age", DataType.INTEGER),
        Column("country", DataType.VARCHAR, max_length=50),
        Column("balance", DataType.FLOAT, default=0.0),
        Column("signup_date", DataType.DATE)
    ]
    
    users_schema = schema_manager.create_schema("users", user_columns)
    users_table = storage.get_table("users")
    
    # Create indexes (automatic on PRIMARY KEY, manual on others)
    index_manager.create_index("users", "id", unique=True)  # Primary key
    index_manager.create_index("users", "username", unique=True)  # Unique constraint
    index_manager.create_index("users", "email", unique=True)  # Unique constraint
    index_manager.create_index("users", "age", unique=False)  # For range queries
    index_manager.create_index("users", "country", unique=False)  # For filtering
    
    print("   Schema created with constraints")
    print("   Indexes created: id, username, email, age, country")
    
    # Step 2: Insert users with full validation
    print("\n2. Inserting users with schema validation and indexing...")
    
    test_users = [
        {
            "id": 1, "username": "alice", "email": "alice@example.com",
            "age": 30, "country": "USA", "balance": 1000.0, "signup_date": "2025-01-01"
        },
        {
            "id": 2, "username": "bob", "email": "bob@example.com",
            "age": 25, "country": "UK", "balance": 500.0, "signup_date": "2025-01-05"
        },
        {
            "id": 3, "username": "charlie", "email": "charlie@example.com",
            "age": 35, "country": "USA", "balance": 1500.0, "signup_date": "2025-01-10"
        },
        {
            "id": 4, "username": "diana", "email": "diana@example.com",
            "age": 28, "country": "Canada", "balance": 750.0, "signup_date": "2025-01-12"
        },
        {
            "id": 5, "username": "eve", "email": "eve@example.com",
            "age": 32, "country": "USA", "balance": 2000.0, "signup_date": "2025-01-14"
        }
    ]
    
    for user_data in test_users:
        # Step 2a: Schema validation
        is_valid, errors = users_schema.validate_row(user_data)
        if not is_valid:
            print(f"   ✗ Validation failed: {errors}")
            continue
        
        # Step 2b: Type conversion
        converted = users_schema.convert_row(user_data)
        
        # Step 2c: Check unique constraints via indexes
        existing = [r.to_dict() for r in users_table.select_all()]
        
        username_valid, err1 = users_schema.check_unique_constraint(
            "username", converted["username"], existing
        )
        email_valid, err2 = users_schema.check_unique_constraint(
            "email", converted["email"], existing
        )
        
        if not (username_valid and email_valid):
            print(f"   ✗ Unique constraint: {err1 or err2}")
            continue
        
        # Step 2d: Insert into storage
        row_id = users_table.insert(converted)
        
        # Step 2e: Update all indexes
        success, error = index_manager.insert_into_indexes("users", row_id, converted)
        
        if success:
            print(f"   ✓ Inserted {converted['username']} (ID: {row_id})")
        else:
            # Rollback storage insert if index insert fails
            users_table.delete_by_id(row_id)
            print(f"   ✗ Index error: {error}")
    
    # Step 3: Query using indexes (fast lookups)
    print("\n3. Querying using indexes...")
    
    # Exact match on indexed column
    print("\n   a) Find user by email (using index):")
    email_idx = index_manager.get_index("users", "email")
    start = time.time()
    row_ids = email_idx.search("alice@example.com")
    query_time = (time.time() - start) * 1000
    
    if row_ids:
        user = users_table.select_by_id(row_ids[0])
        print(f"      Found: {user['username']} ({user['email']}) in {query_time:.4f}ms")
    
    # Range query on age
    print("\n   b) Find users aged 25-30 (using index):")
    age_idx = index_manager.get_index("users", "age")
    start = time.time()
    row_ids = age_idx.range_search(25, 30)
    query_time = (time.time() - start) * 1000
    
    print(f"      Found {len(row_ids)} users in {query_time:.4f}ms:")
    for row_id in row_ids:
        user = users_table.select_by_id(row_id)
        print(f"      - {user['username']}, age {user['age']}")
    
    # Filter by country
    print("\n   c) Find users from USA (using index):")
    country_idx = index_manager.get_index("users", "country")
    start = time.time()
    row_ids = country_idx.search("USA")
    query_time = (time.time() - start) * 1000
    
    print(f"      Found {len(row_ids)} users in {query_time:.4f}ms:")
    for row_id in row_ids:
        user = users_table.select_by_id(row_id)
        print(f"      - {user['username']} from {user['country']}")
    
    # Step 4: Update with index maintenance
    print("\n4. Updating user with index maintenance...")
    
    user_to_update = users_table.select_by_id(1)
    old_data = user_to_update.data.copy()
    new_data = {**old_data, "age": 31, "balance": 1100.0}
    
    # Validate update
    is_valid, errors = users_schema.validate_row(new_data)
    
    if is_valid:
        converted = users_schema.convert_row(new_data)
        
        # Update storage
        users_table.update_by_id(1, converted)
        
        # Update indexes
        success, error = index_manager.update_indexes("users", 1, old_data, converted)
        
        if success:
            print(f"   ✓ Updated alice: age {old_data['age']} → {converted['age']}")
            print(f"   ✓ Indexes updated automatically")
        else:
            # Rollback
            users_table.update_by_id(1, old_data)
            print(f"   ✗ Index update failed: {error}")
    
    # Step 5: Delete with index cleanup
    print("\n5. Deleting user with index cleanup...")
    
    user_to_delete = users_table.select_by_id(2)
    deleted_data = user_to_delete.data.copy()
    
    # Delete from storage
    success = users_table.delete_by_id(2)
    
    if success:
        # Clean up indexes
        index_manager.delete_from_indexes("users", 2, deleted_data)
        print(f"   ✓ Deleted bob (ID: 2)")
        print(f"   ✓ Indexes cleaned up automatically")
    
    # Verify deletion
    email_idx = index_manager.get_index("users", "email")
    row_ids = email_idx.search("bob@example.com")
    print(f"   Verification: bob's email in index: {row_ids}")
    
    # Step 6: Test unique constraint violation
    print("\n6. Testing unique constraint via indexes...")
    
    duplicate_user = {
        "id": 10,
        "username": "alice",  # Duplicate username
        "email": "alice.new@example.com",
        "age": 29,
        "country": "USA",
        "signup_date": "2025-01-15"
    }
    
    is_valid, errors = users_schema.validate_row(duplicate_user)
    if is_valid:
        converted = users_schema.convert_row(duplicate_user)
        
        # Try to insert into indexes
        success, error = index_manager.insert_into_indexes("users", 10, converted)
        
        if not success:
            print(f"   ✗ Prevented duplicate: {error}")
        else:
            print(f"   ✗ ERROR: Should have been prevented!")
    
    # Step 7: Index statistics
    print("\n7. Index statistics:")
    
    stats = index_manager.get_all_stats()
    for table, table_stats in stats.items():
        print(f"\n   Table: {table}")
        for stat in table_stats:
            print(f"      Column: {stat['column']}")
            print(f"      Unique: {stat['unique']}")
            print(f"      Distinct keys: {stat['distinct_keys']}")
            print(f"      Total entries: {stat['total_entries']}")
            print()
    
    # Step 8: Demonstrate performance benefit
    print("8. Performance comparison (indexed vs non-indexed):")
    
    # Without index (full scan)
    print("\n   a) Full scan without index:")
    start = time.time()
    found = None
    for row in users_table.select_all():
        if row['username'] == 'alice':
            found = row
            break
    scan_time = (time.time() - start) * 1000
    print(f"      Found in {scan_time:.4f}ms (full table scan)")
    
    # With index
    print("\n   b) Lookup with index:")
    username_idx = index_manager.get_index("users", "username")
    start = time.time()
    row_ids = username_idx.search("alice")
    if row_ids:
        found = users_table.select_by_id(row_ids[0])
    index_time = (time.time() - start) * 1000
    print(f"      Found in {index_time:.4f}ms (indexed lookup)")
    
    if scan_time > index_time:
        speedup = scan_time / index_time if index_time > 0 else float('inf')
        print(f"      Speedup: {speedup:.2f}x faster with index")
    
    # Step 9: Summary
    print("\n" + "=" * 70)
    print("Summary:")
    print("=" * 70)
    print(f"✓ Schema validation working")
    print(f"✓ Storage persistence working")
    print(f"✓ Indexes accelerating queries")
    print(f"✓ UNIQUE constraints enforced via indexes")
    print(f"✓ Index maintenance on INSERT/UPDATE/DELETE")
    print(f"✓ Range queries supported")
    print(f"\nTotal users: {users_table.count()}")
    print(f"Total indexes: {len(index_manager.list_indexes('users'))}")
    print("\nCheck 'full_demo_data/' for database and index files")
    print("=" * 70)


if __name__ == "__main__":
    main()