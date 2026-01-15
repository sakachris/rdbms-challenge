"""
Unit tests for the Schema Manager
Run with: python tests/test_schema.py
"""

import sys
import os
from datetime import date, datetime

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from simpldb.schema import (
    Column, Schema, SchemaManager,
    DataType, ColumnConstraint
)


def test_column_creation():
    """Test Column class"""
    print("Testing Column creation...")
    
    # Basic column
    col = Column("name", DataType.VARCHAR, max_length=50)
    assert col.name == "name"
    assert col.data_type == DataType.VARCHAR
    assert col.max_length == 50
    
    # Column with constraints
    pk_col = Column("id", DataType.INTEGER, constraints=[ColumnConstraint.PRIMARY_KEY])
    assert pk_col.is_primary_key()
    assert pk_col.is_not_null()  # PRIMARY KEY implies NOT NULL
    assert pk_col.is_unique()  # PRIMARY KEY implies UNIQUE
    
    # VARCHAR without max_length should raise error
    try:
        bad_col = Column("bad", DataType.VARCHAR)
        assert False, "Should have raised ValueError"
    except ValueError as e:
        assert "max_length" in str(e)
    
    print("✅ Column creation test passed")


def test_data_type_validation():
    """Test data type validation and conversion"""
    print("\nTesting data type validation...")
    
    # INTEGER
    int_col = Column("age", DataType.INTEGER)
    assert int_col.convert_value(25) == 25
    assert int_col.convert_value("30") == 30
    
    try:
        int_col.convert_value("not a number")
        assert False, "Should raise ValueError"
    except ValueError:
        pass
    
    # VARCHAR
    str_col = Column("name", DataType.VARCHAR, max_length=10)
    is_valid, error = str_col.validate_value("Alice")
    assert is_valid
    
    is_valid, error = str_col.validate_value("VeryLongNameThatExceedsLimit")
    assert not is_valid
    assert "max length" in error.lower()
    
    # FLOAT
    float_col = Column("price", DataType.FLOAT)
    assert float_col.convert_value(10.5) == 10.5
    assert float_col.convert_value("20.5") == 20.5
    assert float_col.convert_value(30) == 30.0
    
    # BOOLEAN
    bool_col = Column("active", DataType.BOOLEAN)
    assert bool_col.convert_value(True) is True
    assert bool_col.convert_value("true") is True
    assert bool_col.convert_value("1") is True
    assert bool_col.convert_value("false") is False
    assert bool_col.convert_value(0) is False
    
    # DATE
    date_col = Column("created", DataType.DATE)
    assert date_col.convert_value("2025-01-15") == "2025-01-15"
    assert date_col.convert_value(date(2025, 1, 15)) == "2025-01-15"
    assert date_col.convert_value(datetime(2025, 1, 15, 10, 30)) == "2025-01-15"
    
    print("✅ Data type validation test passed")


def test_not_null_constraint():
    """Test NOT NULL constraint"""
    print("\nTesting NOT NULL constraint...")
    
    # Column with NOT NULL
    col = Column("email", DataType.VARCHAR, max_length=100, 
                 constraints=[ColumnConstraint.NOT_NULL])
    
    is_valid, error = col.validate_value("alice@example.com")
    assert is_valid
    
    is_valid, error = col.validate_value(None)
    assert not is_valid
    assert "cannot be NULL" in error
    
    # Column without NOT NULL (nullable)
    nullable_col = Column("middle_name", DataType.VARCHAR, max_length=50)
    is_valid, error = nullable_col.validate_value(None)
    assert is_valid
    
    print("✅ NOT NULL constraint test passed")


def test_default_values():
    """Test default values"""
    print("\nTesting default values...")
    
    columns = [
        Column("id", DataType.INTEGER, constraints=[ColumnConstraint.PRIMARY_KEY]),
        Column("status", DataType.VARCHAR, max_length=20, default="pending"),
        Column("count", DataType.INTEGER, default=0),
        Column("active", DataType.BOOLEAN, default=True)
    ]
    
    schema = Schema("tasks", columns)
    
    # Row without defaults specified
    row_data = {"id": 1}
    converted = schema.convert_row(row_data)
    
    assert converted["id"] == 1
    assert converted["status"] == "pending"
    assert converted["count"] == 0
    assert converted["active"] is True
    
    print("✅ Default values test passed")


def test_schema_validation():
    """Test schema validation"""
    print("\nTesting schema validation...")
    
    columns = [
        Column("id", DataType.INTEGER, constraints=[ColumnConstraint.PRIMARY_KEY]),
        Column("username", DataType.VARCHAR, max_length=50, 
               constraints=[ColumnConstraint.NOT_NULL]),
        Column("email", DataType.VARCHAR, max_length=100)
    ]
    
    schema = Schema("users", columns)
    
    # Valid row
    valid_row = {"id": 1, "username": "alice", "email": "alice@example.com"}
    is_valid, errors = schema.validate_row(valid_row)
    assert is_valid
    assert len(errors) == 0
    
    # Missing NOT NULL field
    invalid_row = {"id": 2, "email": "bob@example.com"}
    is_valid, errors = schema.validate_row(invalid_row)
    assert not is_valid
    assert any("username" in error for error in errors)
    
    # Unknown column
    unknown_col_row = {"id": 3, "username": "charlie", "unknown": "value"}
    is_valid, errors = schema.validate_row(unknown_col_row)
    assert not is_valid
    assert any("unknown" in error.lower() for error in errors)
    
    print("✅ Schema validation test passed")


def test_primary_key():
    """Test primary key constraints"""
    print("\nTesting primary key constraints...")
    
    # Can only have one primary key
    try:
        columns = [
            Column("id1", DataType.INTEGER, constraints=[ColumnConstraint.PRIMARY_KEY]),
            Column("id2", DataType.INTEGER, constraints=[ColumnConstraint.PRIMARY_KEY])
        ]
        schema = Schema("bad_table", columns)
        assert False, "Should have raised ValueError"
    except ValueError as e:
        assert "PRIMARY KEY" in str(e)
    
    # Primary key should be retrievable
    columns = [
        Column("id", DataType.INTEGER, constraints=[ColumnConstraint.PRIMARY_KEY]),
        Column("name", DataType.VARCHAR, max_length=50)
    ]
    schema = Schema("users", columns)
    
    pk = schema.get_primary_key()
    assert pk is not None
    assert pk.name == "id"
    assert pk.is_primary_key()
    
    print("✅ Primary key test passed")


def test_unique_constraint():
    """Test unique constraint checking"""
    print("\nTesting unique constraint...")
    
    columns = [
        Column("id", DataType.INTEGER, constraints=[ColumnConstraint.PRIMARY_KEY]),
        Column("username", DataType.VARCHAR, max_length=50, 
               constraints=[ColumnConstraint.UNIQUE]),
        Column("email", DataType.VARCHAR, max_length=100)
    ]
    
    schema = Schema("users", columns)
    
    # Get unique columns
    unique_cols = schema.get_unique_columns()
    assert len(unique_cols) == 2  # id (PK) and username
    assert any(col.name == "id" for col in unique_cols)
    assert any(col.name == "username" for col in unique_cols)
    
    # Simulate existing rows
    existing_rows = [
        {"row_id": 1, "data": {"id": 1, "username": "alice"}},
        {"row_id": 2, "data": {"id": 2, "username": "bob"}}
    ]
    
    # Check duplicate username
    is_valid, error = schema.check_unique_constraint("username", "alice", existing_rows)
    assert not is_valid
    assert "UNIQUE" in error or "already exists" in error
    
    # Check new username
    is_valid, error = schema.check_unique_constraint("username", "charlie", existing_rows)
    assert is_valid
    
    # Check duplicate ID (primary key)
    is_valid, error = schema.check_unique_constraint("id", 1, existing_rows)
    assert not is_valid
    assert "PRIMARY KEY" in error or "already exists" in error
    
    # Exclude row when updating
    is_valid, error = schema.check_unique_constraint("username", "alice", existing_rows, 
                                                      exclude_row_id=1)
    assert is_valid  # Should be valid because we're excluding row 1
    
    print("✅ Unique constraint test passed")


def test_schema_serialization():
    """Test schema serialization and deserialization"""
    print("\nTesting schema serialization...")
    
    columns = [
        Column("id", DataType.INTEGER, constraints=[ColumnConstraint.PRIMARY_KEY]),
        Column("name", DataType.VARCHAR, max_length=100, constraints=[ColumnConstraint.NOT_NULL]),
        Column("score", DataType.FLOAT, default=0.0)
    ]
    
    schema = Schema("players", columns)
    
    # Serialize
    schema_dict = schema.to_dict()
    assert schema_dict["table_name"] == "players"
    assert len(schema_dict["columns"]) == 3
    
    # Deserialize
    restored = Schema.from_dict(schema_dict)
    assert restored.table_name == "players"
    assert len(restored.columns) == 3
    assert restored.get_column("id").is_primary_key()
    assert restored.get_column("name").is_not_null()
    assert restored.get_column("score").default == 0.0
    
    print("✅ Schema serialization test passed")


def test_schema_manager():
    """Test SchemaManager"""
    print("\nTesting Schema Manager...")
    
    manager = SchemaManager()
    
    # Create schema
    user_columns = [
        Column("id", DataType.INTEGER, constraints=[ColumnConstraint.PRIMARY_KEY]),
        Column("name", DataType.VARCHAR, max_length=50)
    ]
    
    manager.create_schema("users", user_columns)
    
    assert manager.table_exists("users")
    assert not manager.table_exists("posts")
    assert "users" in manager.list_tables()
    
    # Get schema
    schema = manager.get_schema("users")
    assert schema is not None
    assert schema.table_name == "users"
    
    # Try to create duplicate
    try:
        manager.create_schema("users", user_columns)
        assert False, "Should have raised ValueError"
    except ValueError as e:
        assert "already exists" in str(e)
    
    # Drop schema
    manager.drop_schema("users")
    assert not manager.table_exists("users")
    
    print("✅ Schema Manager test passed")


def test_type_conversion():
    """Test comprehensive type conversion"""
    print("\nTesting type conversion...")
    
    columns = [
        Column("id", DataType.INTEGER, constraints=[ColumnConstraint.PRIMARY_KEY]),
        Column("name", DataType.VARCHAR, max_length=100),
        Column("score", DataType.FLOAT),
        Column("active", DataType.BOOLEAN),
        Column("signup_date", DataType.DATE),
        Column("bio", DataType.TEXT)
    ]
    
    schema = Schema("users", columns)
    
    # Test conversion from strings
    data = {
        "id": "123",
        "name": "Alice",
        "score": "95.5",
        "active": "true",
        "signup_date": "2025-01-15",
        "bio": "A very long biography text..."
    }
    
    converted = schema.convert_row(data)
    
    assert isinstance(converted["id"], int)
    assert converted["id"] == 123
    assert isinstance(converted["score"], float)
    assert converted["score"] == 95.5
    assert isinstance(converted["active"], bool)
    assert converted["active"] is True
    assert isinstance(converted["signup_date"], str)
    assert converted["signup_date"] == "2025-01-15"
    
    print("✅ Type conversion test passed")


def run_all_tests():
    """Run all schema tests"""
    print("=" * 60)
    print("Running Schema Manager Tests")
    print("=" * 60)
    
    test_column_creation()
    test_data_type_validation()
    test_not_null_constraint()
    test_default_values()
    test_schema_validation()
    test_primary_key()
    test_unique_constraint()
    test_schema_serialization()
    test_schema_manager()
    test_type_conversion()
    
    print("\n" + "=" * 60)
    print("✅ All schema tests passed!")
    print("=" * 60)


if __name__ == "__main__":
    run_all_tests()