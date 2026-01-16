#!/usr/bin/env python3
"""
Setup script to verify SimplDB structure and run tests
"""

import os
import sys
from pathlib import Path

def check_structure():
    """Check if the project structure is correct"""
    print("Checking project structure...\n")
    
    required_files = [
        'simpldb/__init__.py',
        'simpldb/parser.py',
        'simpldb/executor.py',
        'simpldb/storage.py',
        'simpldb/schema.py',
        'simpldb/indexes.py',
        'tests/__init__.py',
        'tests/test_query_engine.py',
    ]
    
    missing = []
    for file_path in required_files:
        if not Path(file_path).exists():
            missing.append(file_path)
            print(f"❌ Missing: {file_path}")
        else:
            print(f"✅ Found: {file_path}")
    
    if missing:
        print(f"\n⚠️  Missing {len(missing)} files")
        print("\nCreating missing __init__.py files...")
        
        # Create __init__.py files
        for init_file in ['simpldb/__init__.py', 'tests/__init__.py']:
            if init_file in missing:
                Path(init_file).parent.mkdir(parents=True, exist_ok=True)
                Path(init_file).touch()
                print(f"   Created: {init_file}")
        
        return False
    else:
        print("\n✅ All required files present")
        return True

def run_parser_test():
    """Run parser test"""
    print("\n" + "="*70)
    print("Testing Parser...")
    print("="*70)
    
    try:
        from simpldb.parser import SQLParser
        
        parser = SQLParser()
        sql = "CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(100) NOT NULL, age INTEGER)"
        query = parser.parse(sql)
        
        print(f"✅ Parser test passed")
        print(f"   Table: {query.table_name}")
        print(f"   Columns: {len(query.columns)}")
        for col in query.columns:
            print(f"      - {col}")
        
        return True
    except Exception as e:
        print(f"❌ Parser test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def run_executor_test():
    """Run basic executor test"""
    print("\n" + "="*70)
    print("Testing Executor...")
    print("="*70)
    
    try:
        from simpldb.parser import SQLParser
        from simpldb.executor import QueryExecutor
        import shutil
        
        # Clean up
        test_dir = Path("test_setup_data")
        if test_dir.exists():
            shutil.rmtree(test_dir)
        
        executor = QueryExecutor("test_setup_data")
        parser = SQLParser()
        
        # Create table
        sql = "CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(100) NOT NULL, age INTEGER)"
        query = parser.parse(sql)
        result = executor.execute(query)
        
        if not result.success:
            print(f"❌ CREATE TABLE failed: {result.message}")
            return False
        
        print(f"✅ CREATE TABLE succeeded")
        
        # Insert data
        sql = "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)"
        query = parser.parse(sql)
        result = executor.execute(query)
        
        if not result.success:
            print(f"❌ INSERT failed: {result.message}")
            return False
        
        print(f"✅ INSERT succeeded")
        
        # Select data
        sql = "SELECT * FROM users"
        query = parser.parse(sql)
        result = executor.execute(query)
        
        if not result.success:
            print(f"❌ SELECT failed: {result.message}")
            return False
        
        print(f"✅ SELECT succeeded - {len(result.rows)} rows")
        for row in result.rows:
            print(f"   {row}")
        
        # Clean up
        shutil.rmtree(test_dir)
        
        return True
        
    except Exception as e:
        print(f"❌ Executor test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def run_full_tests():
    """Run the full test suite"""
    print("\n" + "="*70)
    print("Running Full Test Suite...")
    print("="*70 + "\n")
    
    try:
        # Add project root to path
        sys.path.insert(0, str(Path.cwd()))
        
        # Import and run tests
        from tests import test_query_engine
        test_query_engine.run_all_tests()
        return True
    except Exception as e:
        print(f"❌ Full test suite failed: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Main setup and test function"""
    print("SimplDB Setup and Test")
    print("="*70)
    
    # Check structure
    if not check_structure():
        print("\n⚠️  Please ensure all files are in place before running tests")
        return
    
    # Run parser test
    if not run_parser_test():
        print("\n❌ Parser test failed. Fix parser issues before proceeding.")
        return
    
    # Run executor test
    if not run_executor_test():
        print("\n❌ Executor test failed. Fix executor issues before proceeding.")
        return
    
    # Run full test suite
    run_full_tests()
    
    print("\n" + "="*70)
    print("Setup and testing complete!")
    print("="*70)

if __name__ == "__main__":
    main()