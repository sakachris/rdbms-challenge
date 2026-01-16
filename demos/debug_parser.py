#!/usr/bin/env python3
"""
Debug script to test parser column parsing
"""

import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from simpldb.parser import SQLParser

def test_column_parsing():
    """Test individual column parsing"""
    parser = SQLParser()
    
    print("Testing Column Parsing")
    print("=" * 70)
    
    # Test SQL
    sql = "CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(100) NOT NULL, age INTEGER)"
    
    print(f"\nSQL: {sql}\n")
    
    try:
        query = parser.parse(sql)
        
        print(f"✅ Parsing succeeded")
        print(f"Table name: {query.table_name}")
        print(f"Number of columns: {len(query.columns)}\n")
        
        for i, col in enumerate(query.columns, 1):
            print(f"Column {i}:")
            print(f"  Name: {col.name}")
            print(f"  Data Type: {col.data_type}")
            print(f"  Max Length: {col.max_length}")
            print(f"  Constraints: {col.constraints}")
            print(f"  Default: {col.default}")
            print()
        
        # Verify specific columns
        assert len(query.columns) == 3, f"Expected 3 columns, got {len(query.columns)}"
        
        # Check column 1 (id)
        assert query.columns[0].name == "id", f"Column 0 name should be 'id', got '{query.columns[0].name}'"
        assert query.columns[0].data_type == "INTEGER", f"Column 0 type should be 'INTEGER', got '{query.columns[0].data_type}'"
        assert "PRIMARY_KEY" in query.columns[0].constraints, "Column 0 should have PRIMARY_KEY constraint"
        
        # Check column 2 (name)
        assert query.columns[1].name == "name", f"Column 1 name should be 'name', got '{query.columns[1].name}'"
        assert query.columns[1].data_type == "VARCHAR", f"Column 1 type should be 'VARCHAR', got '{query.columns[1].data_type}'"
        assert query.columns[1].max_length == 100, f"Column 1 max_length should be 100, got {query.columns[1].max_length}"
        assert "NOT_NULL" in query.columns[1].constraints, "Column 1 should have NOT_NULL constraint"
        
        # Check column 3 (age)
        assert query.columns[2].name == "age", f"Column 2 name should be 'age', got '{query.columns[2].name}'"
        assert query.columns[2].data_type == "INTEGER", f"Column 2 type should be 'INTEGER', got '{query.columns[2].data_type}'"
        
        print("=" * 70)
        print("✅ All assertions passed!")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    return True

def test_column_splitting():
    """Test the column splitting function"""
    parser = SQLParser()
    
    print("\n\nTesting Column Splitting")
    print("=" * 70)
    
    test_cases = [
        "id INTEGER PRIMARY KEY, name VARCHAR(100) NOT NULL, age INTEGER",
        "id INTEGER PRIMARY KEY, email VARCHAR(100) UNIQUE",
        "id INTEGER, name VARCHAR(50), balance FLOAT DEFAULT 0.0",
    ]
    
    for i, columns_str in enumerate(test_cases, 1):
        print(f"\nTest {i}: {columns_str}")
        columns = parser._split_columns(columns_str)
        print(f"Split into {len(columns)} columns:")
        for j, col in enumerate(columns, 1):
            print(f"  {j}. '{col}'")

def test_multiple_queries():
    """Test parsing multiple types of queries"""
    parser = SQLParser()
    
    print("\n\nTesting Multiple Query Types")
    print("=" * 70)
    
    test_queries = [
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(100) NOT NULL, age INTEGER)",
        "CREATE TABLE products (id INTEGER PRIMARY KEY, name VARCHAR(200), price FLOAT, stock INTEGER DEFAULT 0)",
        "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)",
        "SELECT * FROM users WHERE age > 25",
        "UPDATE users SET age = 31 WHERE id = 1",
    ]
    
    for i, sql in enumerate(test_queries, 1):
        print(f"\n{i}. {sql}")
        try:
            query = parser.parse(sql)
            print(f"   ✅ Type: {query.query_type.value}")
            if hasattr(query, 'columns') and isinstance(query.columns, list):
                if query.query_type.value == 'CREATE_TABLE':
                    print(f"   Columns: {len(query.columns)}")
                    for col in query.columns:
                        print(f"      - {col.name} {col.data_type}" + 
                              (f"({col.max_length})" if col.max_length else ""))
        except Exception as e:
            print(f"   ❌ Error: {e}")

if __name__ == "__main__":
    success = test_column_parsing()
    test_column_splitting()
    test_multiple_queries()
    
    if success:
        print("\n" + "=" * 70)
        print("✅ Parser is working correctly!")
        print("=" * 70)
        sys.exit(0)
    else:
        sys.exit(1)