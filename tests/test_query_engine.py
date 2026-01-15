"""
Unit tests for SQL Parser and Query Executor
Run with: python tests/test_query_engine.py
"""

import sys
import os
import shutil
from pathlib import Path

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from simpldb.parser import SQLParser, QueryType
from simpldb.executor import QueryExecutor


def cleanup():
    """Clean up test data"""
    test_dir = Path("test_query_data")
    if test_dir.exists():
        shutil.rmtree(test_dir)


def test_parse_create_table():
    """Test CREATE TABLE parsing"""
    print("Testing CREATE TABLE parsing...")
    
    parser = SQLParser()
    
    sql = "CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(100) NOT NULL, age INTEGER)"
    query = parser.parse(sql)
    
    assert query.query_type == QueryType.CREATE_TABLE
    assert query.table_name == "users"
    assert len(query.columns) == 3
    
    assert query.columns[0].name == "id"
    assert query.columns[0].data_type == "INTEGER"
    assert "PRIMARY_KEY" in query.columns[0].constraints
    
    assert query.columns[1].name == "name"
    assert query.columns[1].data_type == "VARCHAR"
    assert query.columns[1].max_length == 100
    assert "NOT_NULL" in query.columns[1].constraints
    
    print("✅ CREATE TABLE parsing test passed")


def test_parse_insert():
    """Test INSERT parsing"""
    print("\nTesting INSERT parsing...")
    
    parser = SQLParser()
    
    sql = "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)"
    query = parser.parse(sql)
    
    assert query.query_type == QueryType.INSERT
    assert query.table_name == "users"
    assert query.columns == ['id', 'name', 'age']
    assert query.values == [1, 'Alice', 30]
    
    print("✅ INSERT parsing test passed")


def test_parse_select():
    """Test SELECT parsing"""
    print("\nTesting SELECT parsing...")
    
    parser = SQLParser()
    
    # Simple SELECT
    sql = "SELECT * FROM users"
    query = parser.parse(sql)
    assert query.query_type == QueryType.SELECT
    assert query.table_name == "users"
    assert query.columns == ['*']
    
    # SELECT with WHERE
    sql = "SELECT name, age FROM users WHERE age > 25"
    query = parser.parse(sql)
    assert query.columns == ['name', 'age']
    assert len(query.where) == 1
    assert query.where[0].column == "age"
    assert query.where[0].value == 25
    
    # SELECT with ORDER BY and LIMIT
    sql = "SELECT * FROM users ORDER BY age DESC LIMIT 10"
    query = parser.parse(sql)
    assert query.order_by == [('age', 'DESC')]
    assert query.limit == 10
    
    print("✅ SELECT parsing test passed")


def test_parse_update():
    """Test UPDATE parsing"""
    print("\nTesting UPDATE parsing...")
    
    parser = SQLParser()
    
    sql = "UPDATE users SET age = 31, name = 'Bob' WHERE id = 1"
    query = parser.parse(sql)
    
    assert query.query_type == QueryType.UPDATE
    assert query.table_name == "users"
    assert query.updates == {'age': 31, 'name': 'Bob'}
    assert len(query.where) == 1
    assert query.where[0].column == "id"
    
    print("✅ UPDATE parsing test passed")


def test_parse_delete():
    """Test DELETE parsing"""
    print("\nTesting DELETE parsing...")
    
    parser = SQLParser()
    
    sql = "DELETE FROM users WHERE id = 1"
    query = parser.parse(sql)
    
    assert query.query_type == QueryType.DELETE
    assert query.table_name == "users"
    assert len(query.where) == 1
    
    print("✅ DELETE parsing test passed")


def test_parse_create_index():
    """Test CREATE INDEX parsing"""
    print("\nTesting CREATE INDEX parsing...")
    
    parser = SQLParser()
    
    # Regular index
    sql = "CREATE INDEX idx_email ON users(email)"
    query = parser.parse(sql)
    assert query.query_type == QueryType.CREATE_INDEX
    assert query.index_name == "idx_email"
    assert query.table_name == "users"
    assert query.column_name == "email"
    assert query.unique == False
    
    # Unique index
    sql = "CREATE UNIQUE INDEX idx_username ON users(username)"
    query = parser.parse(sql)
    assert query.unique == True
    
    print("✅ CREATE INDEX parsing test passed")


def test_parse_join():
    """Test JOIN parsing"""
    print("\nTesting JOIN parsing...")
    
    parser = SQLParser()
    
    sql = "SELECT u.name, p.title FROM users u INNER JOIN posts p ON u.id = p.author_id"
    query = parser.parse(sql)
    
    assert query.query_type == QueryType.SELECT
    assert len(query.joins) == 1
    assert query.joins[0].table == "p"
    assert query.joins[0].on_left == "u.id"
    assert query.joins[0].on_right == "p.author_id"
    
    print("✅ JOIN parsing test passed")


def test_execute_create_table():
    """Test CREATE TABLE execution"""
    print("\nTesting CREATE TABLE execution...")
    
    cleanup()
    executor = QueryExecutor("test_query_data")
    parser = SQLParser()
    
    sql = "CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(100) NOT NULL, age INTEGER)"
    query = parser.parse(sql)
    result = executor.execute(query)
    
    assert result.success
    assert executor.schema_manager.table_exists("users")
    
    cleanup()
    print("✅ CREATE TABLE execution test passed")


def test_execute_insert():
    """Test INSERT execution"""
    print("\nTesting INSERT execution...")
    
    cleanup()
    executor = QueryExecutor("test_query_data")
    parser = SQLParser()
    
    # Create table
    sql = "CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(100), age INTEGER)"
    executor.execute(parser.parse(sql))
    
    # Insert data
    sql = "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)"
    result = executor.execute(parser.parse(sql))
    
    assert result.success
    assert result.rows_affected == 1
    
    cleanup()
    print("✅ INSERT execution test passed")


def test_execute_select():
    """Test SELECT execution"""
    print("\nTesting SELECT execution...")
    
    cleanup()
    executor = QueryExecutor("test_query_data")
    parser = SQLParser()
    
    # Setup
    executor.execute(parser.parse("CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(100), age INTEGER)"))
    executor.execute(parser.parse("INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)"))
    executor.execute(parser.parse("INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25)"))
    executor.execute(parser.parse("INSERT INTO users (id, name, age) VALUES (3, 'Charlie', 35)"))
    
    # Select all
    result = executor.execute(parser.parse("SELECT * FROM users"))
    assert result.success
    assert len(result.rows) == 3
    
    # Select with WHERE
    result = executor.execute(parser.parse("SELECT * FROM users WHERE age > 25"))
    assert len(result.rows) == 2
    
    # Select with ORDER BY
    result = executor.execute(parser.parse("SELECT * FROM users ORDER BY age ASC"))
    assert result.rows[0]['age'] == 25
    assert result.rows[2]['age'] == 35
    
    # Select with LIMIT
    result = executor.execute(parser.parse("SELECT * FROM users LIMIT 2"))
    assert len(result.rows) == 2
    
    cleanup()
    print("✅ SELECT execution test passed")


def test_execute_update():
    """Test UPDATE execution"""
    print("\nTesting UPDATE execution...")
    
    cleanup()
    executor = QueryExecutor("test_query_data")
    parser = SQLParser()
    
    # Setup
    executor.execute(parser.parse("CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(100), age INTEGER)"))
    executor.execute(parser.parse("INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)"))
    
    # Update
    result = executor.execute(parser.parse("UPDATE users SET age = 31 WHERE id = 1"))
    assert result.success
    assert result.rows_affected == 1
    
    # Verify
    result = executor.execute(parser.parse("SELECT * FROM users WHERE id = 1"))
    assert result.rows[0]['age'] == 31
    
    cleanup()
    print("✅ UPDATE execution test passed")


def test_execute_delete():
    """Test DELETE execution"""
    print("\nTesting DELETE execution...")
    
    cleanup()
    executor = QueryExecutor("test_query_data")
    parser = SQLParser()
    
    # Setup
    executor.execute(parser.parse("CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(100), age INTEGER)"))
    executor.execute(parser.parse("INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)"))
    executor.execute(parser.parse("INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25)"))
    
    # Delete
    result = executor.execute(parser.parse("DELETE FROM users WHERE id = 1"))
    assert result.success
    assert result.rows_affected == 1
    
    # Verify
    result = executor.execute(parser.parse("SELECT * FROM users"))
    assert len(result.rows) == 1
    assert result.rows[0]['id'] == 2
    
    cleanup()
    print("✅ DELETE execution test passed")


def test_execute_with_indexes():
    """Test queries with indexes"""
    print("\nTesting queries with indexes...")
    
    cleanup()
    executor = QueryExecutor("test_query_data")
    parser = SQLParser()
    
    # Setup
    executor.execute(parser.parse("CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(100), age INTEGER)"))
    executor.execute(parser.parse("INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)"))
    executor.execute(parser.parse("INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25)"))
    executor.execute(parser.parse("INSERT INTO users (id, name, age) VALUES (3, 'Charlie', 35)"))
    
    # Create index
    result = executor.execute(parser.parse("CREATE INDEX idx_age ON users(age)"))
    assert result.success
    
    # Query using index
    result = executor.execute(parser.parse("SELECT * FROM users WHERE age = 30"))
    assert result.success
    assert len(result.rows) == 1
    assert result.rows[0]['name'] == 'Alice'
    
    cleanup()
    print("✅ Indexed query test passed")


def test_unique_constraint():
    """Test UNIQUE constraint enforcement"""
    print("\nTesting UNIQUE constraint...")
    
    cleanup()
    executor = QueryExecutor("test_query_data")
    parser = SQLParser()
    
    # Create table with unique constraint
    executor.execute(parser.parse("CREATE TABLE users (id INTEGER PRIMARY KEY, email VARCHAR(100) UNIQUE)"))
    executor.execute(parser.parse("INSERT INTO users (id, email) VALUES (1, 'alice@example.com')"))
    
    # Try to insert duplicate
    result = executor.execute(parser.parse("INSERT INTO users (id, email) VALUES (2, 'alice@example.com')"))
    assert not result.success
    assert "constraint" in result.message.lower() or "exists" in result.message.lower()
    
    cleanup()
    print("✅ UNIQUE constraint test passed")


def test_complex_where():
    """Test complex WHERE clauses"""
    print("\nTesting complex WHERE clauses...")
    
    cleanup()
    executor = QueryExecutor("test_query_data")
    parser = SQLParser()
    
    # Setup
    executor.execute(parser.parse("CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(100), age INTEGER)"))
    executor.execute(parser.parse("INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)"))
    executor.execute(parser.parse("INSERT INTO users (id, name, age) VALUES (2, 'Bob', 25)"))
    executor.execute(parser.parse("INSERT INTO users (id, name, age) VALUES (3, 'Charlie', 35)"))
    
    # Multiple conditions
    result = executor.execute(parser.parse("SELECT * FROM users WHERE age >= 30 AND age <= 35"))
    assert len(result.rows) == 2
    
    cleanup()
    print("✅ Complex WHERE test passed")


def test_data_types():
    """Test various data types"""
    print("\nTesting data types...")
    
    cleanup()
    executor = QueryExecutor("test_query_data")
    parser = SQLParser()
    
    sql = """CREATE TABLE products (
        id INTEGER PRIMARY KEY,
        name VARCHAR(100),
        price FLOAT,
        in_stock BOOLEAN,
        created_date DATE
    )"""
    executor.execute(parser.parse(sql))
    
    sql = "INSERT INTO products (id, name, price, in_stock, created_date) VALUES (1, 'Widget', 19.99, TRUE, '2025-01-15')"
    result = executor.execute(parser.parse(sql))
    assert result.success
    
    result = executor.execute(parser.parse("SELECT * FROM products"))
    assert result.rows[0]['price'] == 19.99
    assert result.rows[0]['in_stock'] == True
    
    cleanup()
    print("✅ Data types test passed")


def run_all_tests():
    """Run all query engine tests"""
    print("=" * 70)
    print("Running Query Engine Tests (Parser + Executor)")
    print("=" * 70)
    
    # Parser tests
    test_parse_create_table()
    test_parse_insert()
    test_parse_select()
    test_parse_update()
    test_parse_delete()
    test_parse_create_index()
    test_parse_join()
    
    # Executor tests
    test_execute_create_table()
    test_execute_insert()
    test_execute_select()
    test_execute_update()
    test_execute_delete()
    test_execute_with_indexes()
    test_unique_constraint()
    test_complex_where()
    test_data_types()
    
    print("\n" + "=" * 70)
    print("✅ All query engine tests passed!")
    print("=" * 70)


if __name__ == "__main__":
    run_all_tests()