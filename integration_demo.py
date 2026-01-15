"""
Integration Demo: Storage Layer + Schema Manager
This demonstrates how both components work together
"""

import sys
import os
from pathlib import Path
import shutil

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from simpldb.storage import Storage
from simpldb.schema import (
    Column, Schema, SchemaManager,
    DataType, ColumnConstraint
)


def cleanup():
    """Clean up demo data"""
    demo_dir = Path("demo_data")
    if demo_dir.exists():
        shutil.rmtree(demo_dir)


def main():
    print("=" * 60)
    print("SimplDB Integration Demo: Storage + Schema")
    print("=" * 60)
    
    cleanup()
    
    # Initialize components
    storage = Storage("demo_data")
    schema_manager = SchemaManager()
    
    # Step 1: Define a schema for users table
    print("\n1. Creating users table schema...")
    user_columns = [
        Column("id", DataType.INTEGER, constraints=[ColumnConstraint.PRIMARY_KEY]),
        Column("username", DataType.VARCHAR, max_length=50, 
               constraints=[ColumnConstraint.UNIQUE, ColumnConstraint.NOT_NULL]),
        Column("email", DataType.VARCHAR, max_length=100, 
               constraints=[ColumnConstraint.UNIQUE, ColumnConstraint.NOT_NULL]),
        Column("age", DataType.INTEGER),
        Column("balance", DataType.FLOAT, default=0.0),
        Column("is_active", DataType.BOOLEAN, default=True),
        Column("signup_date", DataType.DATE)
    ]
    
    users_schema = schema_manager.create_schema("users", user_columns)
    print(users_schema)
    
    # Step 2: Insert valid data with schema validation
    print("\n2. Inserting users with schema validation...")
    
    users_table = storage.get_table("users")
    
    new_users = [
        {
            "id": 1,
            "username": "alice",
            "email": "alice@example.com",
            "age": 30,
            "balance": 100.50,
            "is_active": True,
            "signup_date": "2025-01-15"
        },
        {
            "id": 2,
            "username": "bob",
            "email": "bob@example.com",
            "age": "25",  # String will be converted
            "balance": "250.75",  # String will be converted
            "is_active": "true",  # String will be converted
            "signup_date": "2025-01-14"
        },
        {
            "id": 3,
            "username": "charlie",
            "email": "charlie@example.com",
            "age": 35
            # Missing optional fields will use defaults
        }
    ]
    
    for user_data in new_users:
        # Validate against schema
        is_valid, errors = users_schema.validate_row(user_data)
        
        if is_valid:
            # Convert types
            converted = users_schema.convert_row(user_data)
            
            # Check unique constraints
            existing = users_table.select_all()
            
            # Check username uniqueness
            username_valid, error = users_schema.check_unique_constraint(
                "username", converted["username"], 
                [r.to_dict() for r in existing]
            )
            
            # Check email uniqueness
            email_valid, error2 = users_schema.check_unique_constraint(
                "email", converted["email"], 
                [r.to_dict() for r in existing]
            )
            
            if username_valid and email_valid:
                row_id = users_table.insert(converted)
                print(f"   ✓ Inserted user '{converted['username']}' with ID {row_id}")
            else:
                print(f"   ✗ Failed: {error or error2}")
        else:
            print(f"   ✗ Validation failed: {errors}")
    
    # Step 3: Try to insert invalid data
    print("\n3. Testing validation - inserting invalid data...")
    
    invalid_users = [
        {
            "id": 4,
            "username": "alice",  # Duplicate username
            "email": "alice2@example.com",
            "age": 28
        },
        {
            "id": 5,
            # Missing required field 'username'
            "email": "noname@example.com",
            "age": 22
        },
        {
            "id": 6,
            "username": "dave",
            "email": "dave@example.com",
            "age": "not a number"  # Invalid type
        }
    ]
    
    for user_data in invalid_users:
        is_valid, errors = users_schema.validate_row(user_data)
        
        if not is_valid:
            print(f"   ✗ Validation failed for {user_data.get('username', 'unknown')}:")
            for error in errors:
                print(f"      - {error}")
        else:
            converted = users_schema.convert_row(user_data)
            existing = users_table.select_all()
            
            username_valid, error = users_schema.check_unique_constraint(
                "username", converted["username"], 
                [r.to_dict() for r in existing]
            )
            
            if not username_valid:
                print(f"   ✗ Unique constraint violation: {error}")
    
    # Step 4: Read and display data
    print("\n4. Reading all users from storage...")
    all_users = users_table.select_all()
    
    print(f"\n   Total users: {len(all_users)}\n")
    for user in all_users:
        print(f"   ID: {user['id']}")
        print(f"   Username: {user['username']}")
        print(f"   Email: {user['email']}")
        print(f"   Age: {user['age']}")
        print(f"   Balance: ${user['balance']}")
        print(f"   Active: {user['is_active']}")
        print(f"   Signup Date: {user['signup_date']}")
        print()
    
    # Step 5: Update with validation
    print("5. Updating user with schema validation...")
    update_data = {
        "age": 31,
        "balance": 150.75
    }
    
    # Validate update
    user_to_update = users_table.select_by_id(1)
    updated_user = {**user_to_update.data, **update_data}
    
    is_valid, errors = users_schema.validate_row(updated_user)
    
    if is_valid:
        converted = users_schema.convert_row(update_data)
        users_table.update_by_id(1, converted)
        print(f"   ✓ Updated user ID 1")
        
        # Verify update
        updated = users_table.select_by_id(1)
        print(f"   New age: {updated['age']}, New balance: ${updated['balance']}")
    else:
        print(f"   ✗ Update validation failed: {errors}")
    
    # Step 6: Create a posts table with foreign key-like reference
    print("\n6. Creating posts table with author reference...")
    
    post_columns = [
        Column("id", DataType.INTEGER, constraints=[ColumnConstraint.PRIMARY_KEY]),
        Column("title", DataType.VARCHAR, max_length=200, 
               constraints=[ColumnConstraint.NOT_NULL]),
        Column("content", DataType.TEXT),
        Column("author_id", DataType.INTEGER, constraints=[ColumnConstraint.NOT_NULL]),
        Column("published", DataType.BOOLEAN, default=False),
        Column("created_at", DataType.DATE)
    ]
    
    posts_schema = schema_manager.create_schema("posts", post_columns)
    posts_table = storage.get_table("posts")
    
    posts_data = [
        {
            "id": 1,
            "title": "My First Post",
            "content": "This is Alice's first blog post!",
            "author_id": 1,
            "published": True,
            "created_at": "2025-01-15"
        },
        {
            "id": 2,
            "title": "Hello World",
            "content": "Bob says hello to the world!",
            "author_id": 2,
            "created_at": "2025-01-15"
        }
    ]
    
    for post_data in posts_data:
        is_valid, errors = posts_schema.validate_row(post_data)
        if is_valid:
            converted = posts_schema.convert_row(post_data)
            post_id = posts_table.insert(converted)
            print(f"   ✓ Created post: '{converted['title']}' by user {converted['author_id']}")
    
    # Step 7: Summary
    print("\n7. Database Summary:")
    print(f"   Tables: {schema_manager.list_tables()}")
    print(f"   Users count: {users_table.count()}")
    print(f"   Posts count: {posts_table.count()}")
    
    print("\n" + "=" * 60)
    print("✅ Integration demo complete!")
    print("=" * 60)
    print("\nCheck 'demo_data/' directory for the database files.")
    print("Both schema validation and storage are working together!")


if __name__ == "__main__":
    main()