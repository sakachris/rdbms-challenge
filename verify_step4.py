"""
Quick verification script for Step 4 fixes
Run from project root: python verify_step4.py
"""

import sys
import shutil
from pathlib import Path

# Add current directory to path
sys.path.insert(0, '.')

from simpldb.parser import SQLParser
from simpldb.executor import QueryExecutor


def cleanup():
    test_dir = Path("verify_data")
    if test_dir.exists():
        shutil.rmtree(test_dir)


def main():
    print("=" * 70)
    print("Step 4 Verification Script")
    print("=" * 70)
    
    cleanup()
    
    parser = SQLParser()
    executor = QueryExecutor("verify_data")
    
    print("\n1. Testing Parser...")
    
    # Test problematic SQL
    sql = "CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(100) NOT NULL, age INTEGER)"
    print(f"   SQL: {sql}")
    
    try:
        query = parser.parse(sql)
        print(f"   ✓ Parsed successfully")
        print(f"   ✓ Table: {query.table_name}")
        print(f"   ✓ Columns: {len(query.columns)}")
        
        for col in query.columns:
            print(f"      - {col.name}: {col.data_type}" + 
                  (f"({col.max_length})" if col.max_length else ""))
    except Exception as e:
        print(f"   ✗ Error: {e}")
        cleanup()
        return False
    
    print("\n2. Testing Executor...")
    
    try:
        result = executor.execute(query)
        print(f"   Success: {result.success}")
        print(f"   Message: {result.message}")
        
        if not result.success:
            print(f"   ✗ Failed to create table")
            cleanup()
            return False
        
        print(f"   ✓ Table created successfully")
    except Exception as e:
        print(f"   ✗ Error: {e}")
        cleanup()
        return False
    
    print("\n3. Testing INSERT...")
    
    sql = "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)"
    query = parser.parse(sql)
    result = executor.execute(query)
    
    print(f"   Success: {result.success}")
    print(f"   Message: {result.message}")
    
    if not result.success:
        print(f"   ✗ Failed to insert")
        cleanup()
        return False
    
    print(f"   ✓ Row inserted")
    
    print("\n4. Testing SELECT...")
    
    sql = "SELECT * FROM users"
    query = parser.parse(sql)
    result = executor.execute(query)
    
    print(f"   Success: {result.success}")
    print(f"   Rows: {len(result.rows)}")
    
    if result.rows:
        print(f"   Data: {result.rows[0]}")
        print(f"   ✓ Query executed successfully")
    else:
        print(f"   ✗ No rows returned")
        cleanup()
        return False
    
    print("\n5. Checking generated files...")
    
    verify_dir = Path("verify_data")
    if not verify_dir.exists():
        print(f"   ✗ verify_data directory not created")
        return False
    
    json_files = list(verify_dir.glob("*.json"))
    print(f"   ✓ Found {len(json_files)} JSON files:")
    for f in json_files:
        print(f"      - {f.name}")
    
    print("\n" + "=" * 70)
    print("✅ All verifications passed!")
    print("=" * 70)
    print("\nStep 4 is working correctly. You can now:")
    print("  1. Run full tests: python tests/test_query_engine.py")
    print("  2. Proceed to Step 5: Database Core")
    
    cleanup()
    return True


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)