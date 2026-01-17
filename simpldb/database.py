"""
SimplDB Database Core
Main database interface that orchestrates all components
"""

import json
from datetime import datetime
from pathlib import Path
from threading import RLock
from typing import Any, Dict, List, Optional

from simpldb.executor import QueryExecutor, QueryResult
from simpldb.indexes import IndexManager
from simpldb.parser import SQLParser
from simpldb.schema import SchemaManager
from simpldb.storage import Storage


class Transaction:
    """Represents a database transaction"""

    def __init__(self, tx_id: int):
        self.tx_id = tx_id
        self.started_at = datetime.now()
        self.operations = []
        self.is_active = True

    def add_operation(self, operation: str, table: str, data: Any):
        """Record an operation in this transaction"""
        self.operations.append(
            {
                "operation": operation,
                "table": table,
                "data": data,
                "timestamp": datetime.now().isoformat(),
            }
        )

    def commit(self):
        """Mark transaction as committed"""
        self.is_active = False

    def rollback(self):
        """Mark transaction as rolled back"""
        self.is_active = False


class DatabaseCatalog:
    """
    Database catalog - stores metadata about the database
    Includes information about tables, indexes, statistics, etc.
    """

    def __init__(self, data_dir: Path):
        self.data_dir = data_dir
        self.catalog_file = data_dir / "catalog.json"
        self.lock = RLock()
        self._load_catalog()

    def _load_catalog(self):
        """Load catalog from disk"""
        if self.catalog_file.exists():
            try:
                with open(self.catalog_file, "r") as f:
                    self.catalog = json.load(f)
            except Exception:
                self._init_catalog()
        else:
            self._init_catalog()

    def _init_catalog(self):
        """Initialize empty catalog"""
        self.catalog = {
            "version": "0.1.0",
            "created_at": datetime.now().isoformat(),
            "tables": {},
            "indexes": {},
            "statistics": {},
            "metadata": {},
        }
        self._save_catalog()

    def _save_catalog(self):
        """Save catalog to disk"""
        with self.lock:
            with open(self.catalog_file, "w") as f:
                json.dump(self.catalog, f, indent=2)

    def register_table(self, table_name: str, schema_dict: Dict):
        """Register a table in the catalog"""
        with self.lock:
            self.catalog["tables"][table_name] = {
                "created_at": datetime.now().isoformat(),
                "schema": schema_dict,
                "row_count": 0,
                "last_modified": datetime.now().isoformat(),
            }
            self._save_catalog()

    def unregister_table(self, table_name: str):
        """Remove a table from the catalog"""
        with self.lock:
            if table_name in self.catalog["tables"]:
                del self.catalog["tables"][table_name]
            # Also remove associated indexes
            self.catalog["indexes"] = {
                k: v
                for k, v in self.catalog["indexes"].items()
                if not k.startswith(f"{table_name}.")
            }
            self._save_catalog()

    def register_index(
        self, table_name: str, column_name: str, index_name: str, unique: bool
    ):
        """Register an index in the catalog"""
        with self.lock:
            key = f"{table_name}.{column_name}"
            self.catalog["indexes"][key] = {
                "name": index_name,
                "table": table_name,
                "column": column_name,
                "unique": unique,
                "created_at": datetime.now().isoformat(),
            }
            self._save_catalog()

    def unregister_index(self, table_name: str, column_name: str):
        """Remove an index from the catalog"""
        with self.lock:
            key = f"{table_name}.{column_name}"
            if key in self.catalog["indexes"]:
                del self.catalog["indexes"][key]
                self._save_catalog()

    def update_table_stats(self, table_name: str, row_count: int):
        """Update table statistics"""
        with self.lock:
            if table_name in self.catalog["tables"]:
                self.catalog["tables"][table_name]["row_count"] = row_count
                self.catalog["tables"][table_name]["last_modified"] = (
                    datetime.now().isoformat()
                )
                self._save_catalog()

    def get_table_info(self, table_name: str) -> Optional[Dict]:
        """Get information about a table"""
        return self.catalog["tables"].get(table_name)

    def get_all_tables(self) -> List[str]:
        """Get list of all tables"""
        return list(self.catalog["tables"].keys())

    def get_all_indexes(self) -> Dict[str, Dict]:
        """Get all indexes"""
        return self.catalog["indexes"].copy()

    def set_metadata(self, key: str, value: Any):
        """Set database metadata"""
        with self.lock:
            self.catalog["metadata"][key] = value
            self._save_catalog()

    def get_metadata(self, key: str, default=None) -> Any:
        """Get database metadata"""
        return self.catalog["metadata"].get(key, default)


class Database:
    """
    Main database class - the primary interface for SimplDB
    Provides high-level API wrapping all components
    """

    def __init__(self, name: str = "simpldb", data_dir: str = "data"):
        self.name = name
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)

        # Initialize components
        self.storage = Storage(data_dir)
        self.schema_manager = SchemaManager()
        self.index_manager = IndexManager(data_dir)
        self.parser = SQLParser()
        self.executor = QueryExecutor(data_dir)

        # 🔥 CRITICAL: share core managers
        self.executor.schema_manager = self.schema_manager
        self.executor.index_manager = self.index_manager
        self.executor.storage = self.storage

        self.catalog = DatabaseCatalog(self.data_dir)

        # Transaction management
        self.transactions: Dict[int, Transaction] = {}
        self.next_tx_id = 1
        self.tx_lock = RLock()

        # Load existing schemas from catalog
        self._load_schemas_from_catalog()

    def _load_schemas_from_catalog(self):
        """Load existing table schemas from catalog"""
        from simpldb.schema import Schema

        for table_name, table_info in self.catalog.catalog["tables"].items():
            if "schema" in table_info:
                try:
                    schema = Schema.from_dict(table_info["schema"])
                    self.schema_manager.schemas[table_name] = schema
                except Exception as e:
                    print(
                        f"Warning: Could not load schema for {table_name}: {e}"
                    )

    def execute(self, sql: str) -> QueryResult:
        """
        Execute a SQL query
        Main entry point for SQL execution
        """
        try:
            # Parse SQL
            query = self.parser.parse(sql)

            # Execute query
            result = self.executor.execute(query)

            # Update catalog based on operation
            if result.success:
                self._update_catalog_after_query(query, result)

            return result

        except Exception as e:
            return QueryResult(False, f"Error: {str(e)}")

    def _update_catalog_after_query(self, query, result: QueryResult):
        """Update catalog after successful query execution"""
        from simpldb.parser import QueryType

        if query.query_type == QueryType.CREATE_TABLE:
            # schema = self.schema_manager.get_schema(query.table_name)
            schema = self.executor.schema_manager.get_schema(query.table_name)
            if schema:
                self.catalog.register_table(query.table_name, schema.to_dict())

        elif query.query_type == QueryType.DROP_TABLE:
            self.catalog.unregister_table(query.table_name)

        elif query.query_type == QueryType.CREATE_INDEX:
            self.catalog.register_index(
                query.table_name,
                query.column_name,
                query.index_name,
                query.unique,
            )

        elif query.query_type == QueryType.DROP_INDEX:
            self.catalog.unregister_index(query.table_name, query.index_name)

        elif query.query_type in (
            QueryType.INSERT,
            QueryType.UPDATE,
            QueryType.DELETE,
        ):
            # Update table statistics
            table = self.storage.get_table(query.table_name)
            self.catalog.update_table_stats(query.table_name, table.count())

    def execute_many(self, sql_queries: List[str]) -> List[QueryResult]:
        """Execute multiple SQL queries"""
        results = []
        for sql in sql_queries:
            result = self.execute(sql)
            results.append(result)
            # Stop on first error
            if not result.success:
                break
        return results

    def begin_transaction(self) -> int:
        """Start a new transaction (basic implementation)"""
        with self.tx_lock:
            tx_id = self.next_tx_id
            self.next_tx_id += 1
            self.transactions[tx_id] = Transaction(tx_id)
            return tx_id

    def commit_transaction(self, tx_id: int) -> bool:
        """Commit a transaction"""
        with self.tx_lock:
            if tx_id in self.transactions:
                self.transactions[tx_id].commit()
                # In a full implementation, we'd flush changes here
                return True
            return False

    def rollback_transaction(self, tx_id: int) -> bool:
        """Rollback a transaction"""
        with self.tx_lock:
            if tx_id in self.transactions:
                self.transactions[tx_id].rollback()
                # In a full implementation, we'd undo changes here
                return True
            return False

    def list_tables(self) -> List[str]:
        """List all tables in the database"""
        return self.catalog.get_all_tables()

    def describe_table(self, table_name: str) -> Optional[Dict]:
        """Get detailed information about a table"""
        table_info = self.catalog.get_table_info(table_name)
        if not table_info:
            return None

        schema = self.executor.schema_manager.get_schema(table_name)
        if not schema:
            return table_info

        # schema = self.schema_manager.get_schema(table_name)
        # if not schema:
        #     return table_info

        # Add column details
        columns_info = []
        for col_name, col in schema.columns.items():
            col_info = {
                "name": col.name,
                "type": col.data_type.value,
                "max_length": col.max_length,
                "constraints": [c.value for c in col.constraints],
                "default": col.default,
            }
            columns_info.append(col_info)

        table_info["columns"] = columns_info

        # Add index information
        indexes = []
        for idx_key, idx_info in self.catalog.get_all_indexes().items():
            if idx_info["table"] == table_name:
                indexes.append(idx_info)
        table_info["indexes"] = indexes

        return table_info

    def get_table_stats(self, table_name: str) -> Optional[Dict]:
        """Get statistics for a table"""
        table_info = self.catalog.get_table_info(table_name)
        if not table_info:
            return None

        table = self.storage.get_table(table_name)

        stats = {
            "table_name": table_name,
            "row_count": table.count(),
            "created_at": table_info.get("created_at"),
            "last_modified": table_info.get("last_modified"),
            "indexes": [],
        }

        # Add index stats
        for col_name in self.index_manager.list_indexes(table_name):
            idx = self.index_manager.get_index(table_name, col_name)
            if idx:
                stats["indexes"].append(idx.get_stats())

        return stats

    def get_database_info(self) -> Dict:
        """Get overall database information"""
        tables = self.list_tables()

        total_rows = 0
        for table_name in tables:
            table = self.storage.get_table(table_name)
            total_rows += table.count()

        return {
            "name": self.name,
            "version": self.catalog.catalog.get("version"),
            "created_at": self.catalog.catalog.get("created_at"),
            "data_directory": str(self.data_dir),
            "total_tables": len(tables),
            "total_rows": total_rows,
            "tables": tables,
            "total_indexes": len(self.catalog.get_all_indexes()),
        }

    def export_schema(self, output_file: str):
        """Export database schema to a file"""
        schema_data = {
            "database": self.name,
            "version": self.catalog.catalog.get("version"),
            "exported_at": datetime.now().isoformat(),
            "tables": [],
        }

        for table_name in self.list_tables():
            table_info = self.describe_table(table_name)
            if table_info:
                schema_data["tables"].append(table_info)

        with open(output_file, "w") as f:
            json.dump(schema_data, f, indent=2)

    def import_schema(self, input_file: str) -> List[QueryResult]:
        """Import database schema from a file"""
        with open(input_file, "r") as f:
            schema_data = json.load(f)

        results = []

        # Create tables
        for table_info in schema_data.get("tables", []):
            if "schema" not in table_info:
                continue

            schema_dict = table_info["schema"]
            table_name = schema_dict["table_name"]

            # Build CREATE TABLE SQL
            columns_sql = []
            for col_dict in schema_dict["columns"]:
                col_sql = f"{col_dict['name']} {col_dict['data_type']}"

                if col_dict.get("max_length"):
                    col_sql += f"({col_dict['max_length']})"

                for constraint in col_dict.get("constraints", []):
                    if constraint == "PRIMARY_KEY":
                        col_sql += " PRIMARY KEY"
                    elif constraint == "UNIQUE":
                        col_sql += " UNIQUE"
                    elif constraint == "NOT_NULL":
                        col_sql += " NOT NULL"

                if col_dict.get("default") is not None:
                    col_sql += f" DEFAULT {col_dict['default']}"

                columns_sql.append(col_sql)

            sql = f"CREATE TABLE {table_name} ({', '.join(columns_sql)})"
            result = self.execute(sql)
            results.append(result)

        return results

    def close(self):
        """Close the database and cleanup resources"""
        # Commit any active transactions
        with self.tx_lock:
            for tx in self.transactions.values():
                if tx.is_active:
                    tx.commit()

        # Save final catalog state
        self.catalog._save_catalog()


if __name__ == "__main__":
    print("Testing SimplDB Database Core\n")
    print("=" * 70)

    import shutil

    # Cleanup
    test_dir = Path("db_test_data")
    if test_dir.exists():
        shutil.rmtree(test_dir)

    # Create database
    print("\n1. Creating database...")
    db = Database(name="testdb", data_dir="db_test_data")
    print(f"   ✓ Database '{db.name}' created")

    # Create table
    print("\n2. Creating table...")
    result = db.execute("""
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            username VARCHAR(50) UNIQUE NOT NULL,
            email VARCHAR(100) UNIQUE NOT NULL,
            age INTEGER,
            balance FLOAT DEFAULT 0.0
        )
    """)
    print(f"   Success: {result.success}")
    print(f"   Message: {result.message}")

    # Insert data
    print("\n3. Inserting data...")
    inserts = [
        (
            "INSERT INTO users (id, username, email, age, balance) "
            "VALUES (1, 'alice', 'alice@example.com', 30, 1000.0)"
        ),
        (
            "INSERT INTO users (id, username, email, age, balance) "
            "VALUES (2, 'bob', 'bob@example.com', 25, 500.0)"
        ),
        (
            "INSERT INTO users (id, username, email, age, balance) "
            "VALUES (3, 'charlie', 'charlie@example.com', 35, 1500.0)"
        ),
    ]

    for sql in inserts:
        result = db.execute(sql)
        print(f"   {result.message}")

    # Query data
    print("\n4. Querying data...")
    result = db.execute("SELECT * FROM users WHERE age > 25")
    print(f"   Found {len(result.rows)} users:")
    for row in result.rows:
        print(f"      {row}")

    # Create index
    print("\n5. Creating index...")
    result = db.execute("CREATE INDEX idx_age ON users(age)")
    print(f"   {result.message}")

    # Update data
    print("\n6. Updating data...")
    result = db.execute(
        "UPDATE users SET balance = 1100.0 WHERE username = 'alice'"
    )
    print(f"   {result.message}")

    # List tables
    print("\n7. Listing tables...")
    tables = db.list_tables()
    print(f"   Tables: {tables}")

    # Describe table
    print("\n8. Describing table 'users'...")
    info = db.describe_table("users")
    if info:
        print(f"   Created: {info['created_at']}")
        print(f"   Columns: {len(info['columns'])}")
        for col in info["columns"]:
            print(f"      - {col['name']}: {col['type']}")
        print(f"   Indexes: {len(info['indexes'])}")

    # Get stats
    print("\n9. Getting table statistics...")
    stats = db.get_table_stats("users")
    if stats:
        print(f"   Table: {stats['table_name']}")
        print(f"   Rows: {stats['row_count']}")
        print(f"   Indexes: {len(stats['indexes'])}")

    # Database info
    print("\n10. Getting database info...")
    db_info = db.get_database_info()
    print(f"   Database: {db_info['name']}")
    print(f"   Total tables: {db_info['total_tables']}")
    print(f"   Total rows: {db_info['total_rows']}")
    print(f"   Total indexes: {db_info['total_indexes']}")

    # Export schema
    print("\n11. Exporting schema...")
    db.export_schema("db_test_data/schema_export.json")
    print("   ✓ Schema exported to schema_export.json")

    # Transaction test
    print("\n12. Testing transactions...")
    tx_id = db.begin_transaction()
    print(f"   Started transaction: {tx_id}")
    db.commit_transaction(tx_id)
    print(f"   Committed transaction: {tx_id}")

    # Close database
    print("\n13. Closing database...")
    db.close()
    print("   ✓ Database closed")

    print("\n" + "=" * 70)
    print("✅ Database core test complete!")
    print("Check 'db_test_data/' for generated files")
