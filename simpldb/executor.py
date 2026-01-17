"""
SimplDB Query Executor
Executes parsed SQL queries using Storage, Schema, and Index managers
"""

from typing import Dict, List, Optional

from simpldb.indexes import IndexManager
from simpldb.parser import (
    CreateIndexQuery,
    CreateTableQuery,
    DeleteQuery,
    DropIndexQuery,
    DropTableQuery,
    InsertQuery,
    Operator,
    Query,
    QueryType,
    SelectQuery,
    UpdateQuery,
)
from simpldb.schema import (
    Column,
    ColumnConstraint,
    DataType,
    Schema,
    SchemaManager,
)
from simpldb.storage import Storage


class QueryResult:
    """Result of a query execution"""

    def __init__(
        self,
        success: bool,
        message: str = "",
        rows: Optional[List[Dict]] = None,
        rows_affected: int = 0,
    ):
        self.success = success
        self.message = message
        self.rows = rows or []
        self.rows_affected = rows_affected

    def __repr__(self):
        if self.rows:
            return (
                f"QueryResult(success={self.success}, rows={len(self.rows)}, "
                f"affected={self.rows_affected})"
            )
        return (
            f"QueryResult(success={self.success}, message='{self.message}', "
            f"affected={self.rows_affected})"
        )


class QueryExecutor:
    """Executes SQL queries"""

    def __init__(self, data_dir: str = "data"):
        self.storage = Storage(data_dir)
        self.schema_manager = SchemaManager()
        self.index_manager = IndexManager(data_dir)

    def execute(self, query: Query) -> QueryResult:
        """Execute a parsed query"""
        try:
            if query.query_type == QueryType.CREATE_TABLE:
                return self._execute_create_table(query)
            elif query.query_type == QueryType.DROP_TABLE:
                return self._execute_drop_table(query)
            elif query.query_type == QueryType.CREATE_INDEX:
                return self._execute_create_index(query)
            elif query.query_type == QueryType.DROP_INDEX:
                return self._execute_drop_index(query)
            elif query.query_type == QueryType.INSERT:
                return self._execute_insert(query)
            elif query.query_type == QueryType.SELECT:
                return self._execute_select(query)
            elif query.query_type == QueryType.UPDATE:
                return self._execute_update(query)
            elif query.query_type == QueryType.DELETE:
                return self._execute_delete(query)
            else:
                return QueryResult(
                    False, f"Unsupported query type: {query.query_type}"
                )
        except Exception as e:
            return QueryResult(False, f"Execution error: {str(e)}")

    def _execute_create_table(self, query: CreateTableQuery) -> QueryResult:
        """Execute CREATE TABLE"""
        if self.schema_manager.table_exists(query.table_name):
            return QueryResult(
                False, f"Table '{query.table_name}' already exists"
            )

        # Convert parsed columns to schema columns
        columns = []
        for col_def in query.columns:
            # Map data type string to DataType enum
            # Extract just the base type (e.g., VARCHAR from VARCHAR(100))
            base_type = col_def.data_type.upper()
            if "(" in base_type:
                base_type = base_type.split("(")[0]

            try:
                data_type = DataType[base_type]
            except KeyError:
                return QueryResult(
                    False, f"Unknown data type: {col_def.data_type}"
                )

            # Map constraint strings to ColumnConstraint enums
            constraints = []
            for constraint_str in col_def.constraints:
                try:
                    constraints.append(ColumnConstraint[constraint_str])
                except KeyError:
                    return QueryResult(
                        False, f"Unknown constraint: {constraint_str}"
                    )

            column = Column(
                name=col_def.name,
                data_type=data_type,
                max_length=col_def.max_length,
                constraints=constraints,
                default=col_def.default,
            )
            columns.append(column)

        # Create schema
        # schema = self.schema_manager.create_schema(query.table_name, columns)
        self.schema_manager.create_schema(query.table_name, columns)

        # Create storage table
        self.storage.get_table(query.table_name)

        # Create indexes for PRIMARY KEY and UNIQUE constraints
        for column in columns:
            if column.is_primary_key() or column.is_unique():
                self.index_manager.create_index(
                    query.table_name, column.name, unique=True
                )

        return QueryResult(
            True, f"Table '{query.table_name}' created successfully"
        )

    def _execute_drop_table(self, query: DropTableQuery) -> QueryResult:
        """Execute DROP TABLE"""
        if not self.schema_manager.table_exists(query.table_name):
            return QueryResult(
                False, f"Table '{query.table_name}' does not exist"
            )

        # Drop indexes
        for column_name in self.index_manager.list_indexes(query.table_name):
            self.index_manager.drop_index(query.table_name, column_name)

        # Drop schema
        self.schema_manager.drop_schema(query.table_name)

        # Drop storage
        self.storage.drop_table(query.table_name)

        return QueryResult(
            True, f"Table '{query.table_name}' dropped successfully"
        )

    def _execute_create_index(self, query: CreateIndexQuery) -> QueryResult:
        """Execute CREATE INDEX"""
        if not self.schema_manager.table_exists(query.table_name):
            return QueryResult(
                False, f"Table '{query.table_name}' does not exist"
            )

        schema = self.schema_manager.get_schema(query.table_name)
        if not schema.get_column(query.column_name):
            return QueryResult(
                False, f"Column '{query.column_name}' does not exist"
            )

        if self.index_manager.has_index(query.table_name, query.column_name):
            return QueryResult(
                False,
                (
                    f"Index on '{query.table_name}.{query.column_name}' "
                    f"already exists"
                ),
            )

        # Create index
        # index = self.index_manager.create_index(
        #     query.table_name, query.column_name, unique=query.unique
        # )
        self.index_manager.create_index(
            query.table_name, query.column_name, unique=query.unique
        )

        # Rebuild index from existing data
        table = self.storage.get_table(query.table_name)
        rows = [row.to_dict() for row in table.select_all()]
        self.index_manager.rebuild_index(
            query.table_name, query.column_name, rows
        )

        return QueryResult(
            True, f"Index '{query.index_name}' created successfully"
        )

    def _execute_drop_index(self, query: DropIndexQuery) -> QueryResult:
        """Execute DROP INDEX"""
        if not self.index_manager.has_index(
            query.table_name, query.index_name
        ):
            return QueryResult(
                False, f"Index '{query.index_name}' does not exist"
            )

        self.index_manager.drop_index(query.table_name, query.index_name)
        return QueryResult(
            True, f"Index '{query.index_name}' dropped successfully"
        )

    def _execute_insert(self, query: InsertQuery) -> QueryResult:
        """Execute INSERT"""
        schema = self.schema_manager.get_schema(query.table_name)
        if not schema:
            return QueryResult(
                False, f"Table '{query.table_name}' does not exist"
            )

        # Build row data
        row_data = dict(zip(query.columns, query.values))

        # Validate against schema
        is_valid, errors = schema.validate_row(row_data)
        if not is_valid:
            return QueryResult(
                False, f"Validation failed: {', '.join(errors)}"
            )

        # Convert types
        converted = schema.convert_row(row_data)

        # Check unique constraints via indexes
        existing = [
            r.to_dict()
            for r in self.storage.get_table(query.table_name).select_all()
        ]

        for column in schema.get_unique_columns():
            is_valid, error = schema.check_unique_constraint(
                column.name, converted.get(column.name), existing
            )
            if not is_valid:
                return QueryResult(False, error)

        # Insert into storage
        table = self.storage.get_table(query.table_name)
        row_id = table.insert(converted)

        # Update indexes
        success, error = self.index_manager.insert_into_indexes(
            query.table_name, row_id, converted
        )

        if not success:
            # Rollback storage insert
            table.delete_by_id(row_id)
            return QueryResult(False, f"Index error: {error}")

        return QueryResult(
            True, f"1 row inserted (ID: {row_id})", rows_affected=1
        )

    def _execute_select(self, query: SelectQuery) -> QueryResult:
        """Execute SELECT"""
        schema = self.schema_manager.get_schema(query.table_name)
        if not schema:
            return QueryResult(
                False, f"Table '{query.table_name}' does not exist"
            )

        table = self.storage.get_table(query.table_name)

        # Get candidate rows (use index if possible)
        if query.where and len(query.where) > 0:
            candidate_row_ids = self._get_candidate_rows_with_index(
                query.table_name, query.where
            )
            if candidate_row_ids is not None:
                # Use indexed lookup
                rows = [
                    table.select_by_id(row_id) for row_id in candidate_row_ids
                ]
                rows = [r for r in rows if r is not None]
            else:
                # Fall back to full scan
                rows = table.select_all()
        else:
            rows = table.select_all()

        # Handle JOINs
        if query.joins:
            rows = self._execute_joins(query, rows, schema)

        # Apply WHERE filter
        if query.where:
            rows = [
                row
                for row in rows
                if self._evaluate_conditions(row, query.where)
            ]

        # Select columns
        result_rows = []
        for row in rows:
            if query.columns == ["*"]:
                result_rows.append(row.data if hasattr(row, "data") else row)
            else:
                selected = {}
                for col in query.columns:
                    if hasattr(row, "data"):
                        selected[col] = row.data.get(col)
                    else:
                        selected[col] = row.get(col)
                result_rows.append(selected)

        # Apply ORDER BY
        if query.order_by:
            for col, direction in reversed(query.order_by):
                reverse = direction == "DESC"
                result_rows.sort(
                    key=lambda r: r.get(col) if r.get(col) is not None else "",
                    reverse=reverse,
                )

        # Apply LIMIT and OFFSET
        if query.offset:
            result_rows = result_rows[query.offset :]
        if query.limit:
            result_rows = result_rows[: query.limit]

        return QueryResult(
            True, f"{len(result_rows)} rows selected", rows=result_rows
        )

    def _execute_update(self, query: UpdateQuery) -> QueryResult:
        """Execute UPDATE"""
        schema = self.schema_manager.get_schema(query.table_name)
        if not schema:
            return QueryResult(
                False, f"Table '{query.table_name}' does not exist"
            )

        table = self.storage.get_table(query.table_name)

        # Find rows to update
        rows_to_update = table.select_all()
        if query.where:
            rows_to_update = [
                row
                for row in rows_to_update
                if self._evaluate_conditions(row, query.where)
            ]

        if not rows_to_update:
            return QueryResult(True, "0 rows updated", rows_affected=0)

        updated_count = 0

        for row in rows_to_update:
            old_data = row.data.copy()
            new_data = {**old_data, **query.updates}

            # Validate update
            is_valid, errors = schema.validate_row(new_data)
            if not is_valid:
                return QueryResult(
                    False, f"Validation failed: {', '.join(errors)}"
                )

            # Convert types
            converted = schema.convert_row(new_data)

            # Check unique constraints (excluding current row)
            existing = [r.to_dict() for r in table.select_all()]
            for column in schema.get_unique_columns():
                if column.name in query.updates:
                    is_valid, error = schema.check_unique_constraint(
                        column.name,
                        converted.get(column.name),
                        existing,
                        exclude_row_id=row.row_id,
                    )
                    if not is_valid:
                        return QueryResult(False, error)

            # Update storage
            table.update_by_id(row.row_id, converted)

            # Update indexes
            success, error = self.index_manager.update_indexes(
                query.table_name, row.row_id, old_data, converted
            )

            if not success:
                # Rollback
                table.update_by_id(row.row_id, old_data)
                return QueryResult(False, f"Index error: {error}")

            updated_count += 1

        return QueryResult(
            True, f"{updated_count} rows updated", rows_affected=updated_count
        )

    def _execute_delete(self, query: DeleteQuery) -> QueryResult:
        """Execute DELETE"""
        schema = self.schema_manager.get_schema(query.table_name)
        if not schema:
            return QueryResult(
                False, f"Table '{query.table_name}' does not exist"
            )

        table = self.storage.get_table(query.table_name)

        # Find rows to delete
        rows_to_delete = table.select_all()
        if query.where:
            rows_to_delete = [
                row
                for row in rows_to_delete
                if self._evaluate_conditions(row, query.where)
            ]

        deleted_count = 0

        for row in rows_to_delete:
            # Delete from indexes
            self.index_manager.delete_from_indexes(
                query.table_name, row.row_id, row.data
            )

            # Delete from storage
            table.delete_by_id(row.row_id)
            deleted_count += 1

        return QueryResult(
            True, f"{deleted_count} rows deleted", rows_affected=deleted_count
        )

    def _get_candidate_rows_with_index(
        self, table_name: str, conditions: List
    ) -> Optional[List[int]]:
        """Try to use an index to get candidate rows"""
        for condition in conditions:
            if condition.operator == Operator.EQ:
                # Check if we have an index on this column
                index = self.index_manager.get_index(
                    table_name, condition.column
                )
                if index:
                    return index.search(condition.value)
            elif condition.operator in (
                Operator.GT,
                Operator.GE,
                Operator.LT,
                Operator.LE,
            ):
                # Range query
                index = self.index_manager.get_index(
                    table_name, condition.column
                )
                if index:
                    if condition.operator == Operator.GT:
                        return index.range_search(
                            condition.value, None, include_start=False
                        )
                    elif condition.operator == Operator.GE:
                        return index.range_search(
                            condition.value, None, include_start=True
                        )
                    elif condition.operator == Operator.LT:
                        return index.range_search(
                            None, condition.value, include_end=False
                        )
                    elif condition.operator == Operator.LE:
                        return index.range_search(
                            None, condition.value, include_end=True
                        )

        return None  # No usable index found

    def _evaluate_conditions(self, row, conditions: List) -> bool:
        """Evaluate WHERE conditions for a row"""
        for condition in conditions:
            value = (
                row.data.get(condition.column)
                if hasattr(row, "data")
                else row.get(condition.column)
            )

            if condition.operator == Operator.EQ:
                if value != condition.value:
                    return False
            elif condition.operator == Operator.NE:
                if value == condition.value:
                    return False
            elif condition.operator == Operator.LT:
                if value is None or value >= condition.value:
                    return False
            elif condition.operator == Operator.LE:
                if value is None or value > condition.value:
                    return False
            elif condition.operator == Operator.GT:
                if value is None or value <= condition.value:
                    return False
            elif condition.operator == Operator.GE:
                if value is None or value < condition.value:
                    return False
            elif condition.operator == Operator.IS_NULL:
                if value is not None:
                    return False
            elif condition.operator == Operator.IS_NOT_NULL:
                if value is None:
                    return False
            elif condition.operator == Operator.LIKE:
                if value is None:
                    return False
                # Simple LIKE implementation (% as wildcard)
                pattern = condition.value.replace("%", ".*")
                import re

                if not re.match(pattern, str(value)):
                    return False

        return True

    def _execute_joins(
        self, query: SelectQuery, left_rows: List, left_schema: Schema
    ) -> List:
        """Execute JOIN operations"""
        if not query.joins:
            return left_rows

        for join in query.joins:
            # Get right table
            right_schema = self.schema_manager.get_schema(join.table)
            if not right_schema:
                raise ValueError(f"Table '{join.table}' does not exist")

            right_table = self.storage.get_table(join.table)
            right_rows = right_table.select_all()

            # Parse join condition (table.column format)
            left_col = join.on_left.split(".")[1]
            right_col = join.on_right.split(".")[1]

            # Perform join
            joined_rows = []
            for left_row in left_rows:
                left_val = (
                    left_row.data.get(left_col)
                    if hasattr(left_row, "data")
                    else left_row.get(left_col)
                )
                matched = False

                for right_row in right_rows:
                    right_val = right_row.data.get(right_col)

                    if left_val == right_val:
                        # Combine rows
                        combined = {}
                        if hasattr(left_row, "data"):
                            combined.update(
                                {
                                    f"{query.table_name}.{k}": v
                                    for k, v in left_row.data.items()
                                }
                            )
                        else:
                            combined.update(
                                {
                                    f"{query.table_name}.{k}": v
                                    for k, v in left_row.items()
                                }
                            )
                        combined.update(
                            {
                                f"{join.table}.{k}": v
                                for k, v in right_row.data.items()
                            }
                        )

                        # Create a pseudo-row object
                        from simpldb.storage import Row

                        joined_row = Row(combined)
                        joined_rows.append(joined_row)
                        matched = True

                # For LEFT JOIN, include left row even if no match
                if not matched and join.join_type.value == "LEFT":
                    combined = {}
                    if hasattr(left_row, "data"):
                        combined.update(
                            {
                                f"{query.table_name}.{k}": v
                                for k, v in left_row.data.items()
                            }
                        )
                    else:
                        combined.update(
                            {
                                f"{query.table_name}.{k}": v
                                for k, v in left_row.items()
                            }
                        )

                    from simpldb.storage import Row

                    joined_row = Row(combined)
                    joined_rows.append(joined_row)

            left_rows = joined_rows

        return left_rows


if __name__ == "__main__":
    print("Testing SimplDB Query Executor\n")
    print("=" * 70)

    import shutil
    from pathlib import Path

    from simpldb.parser import SQLParser

    # Clean up
    test_dir = Path("executor_test_data")
    if test_dir.exists():
        shutil.rmtree(test_dir)

    # Initialize
    executor = QueryExecutor("executor_test_data")
    parser = SQLParser()

    # Test queries
    test_sqls = [
        (
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(100) "
            "NOT NULL, age INTEGER, email VARCHAR(100) UNIQUE)"
        ),
        (
            "INSERT INTO users (id, name, age, email) "
            "VALUES (1, 'Alice', 30, 'alice@example.com')"
        ),
        (
            "INSERT INTO users (id, name, age, email) "
            "VALUES (2, 'Bob', 25, 'bob@example.com')"
        ),
        (
            "INSERT INTO users (id, name, age, email) "
            "VALUES (3, 'Charlie', 35, 'charlie@example.com')"
        ),
        "SELECT * FROM users",
        "SELECT name, age FROM users WHERE age > 25",
        "SELECT * FROM users WHERE age >= 30 ORDER BY age DESC",
        "UPDATE users SET age = 31 WHERE id = 1",
        "SELECT * FROM users WHERE id = 1",
        "DELETE FROM users WHERE id = 2",
        "SELECT * FROM users",
        "CREATE INDEX idx_age ON users(age)",
        "SELECT * FROM users WHERE age > 30",
    ]

    for i, sql in enumerate(test_sqls, 1):
        print(f"\n{i}. SQL: {sql}")
        try:
            query = parser.parse(sql)
            result = executor.execute(query)

            print(f"   Success: {result.success}")
            print(f"   Message: {result.message}")

            if result.rows:
                print(f"   Rows returned: {len(result.rows)}")
                for row in result.rows[:3]:  # Show first 3 rows
                    print(f"      {row}")
                if len(result.rows) > 3:
                    print(f"      ... and {len(result.rows) - 3} more")
        except Exception as e:
            print(f"   Error: {e}")

    print("\n" + "=" * 70)
    print("✅ Query executor test complete!")
    print("Check 'executor_test_data/' for generated files")
