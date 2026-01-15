"""
SimplDB Schema Manager
Handles table schemas, column definitions, data types, and constraints
"""

from enum import Enum
from typing import Any, Dict, List, Optional, Set
from datetime import datetime, date
import re


class DataType(Enum):
    """Supported data types"""
    INTEGER = "INTEGER"
    VARCHAR = "VARCHAR"
    FLOAT = "FLOAT"
    BOOLEAN = "BOOLEAN"
    DATE = "DATE"
    TEXT = "TEXT"  # Unlimited length text


class ColumnConstraint(Enum):
    """Column constraints"""
    PRIMARY_KEY = "PRIMARY_KEY"
    UNIQUE = "UNIQUE"
    NOT_NULL = "NOT_NULL"


class Column:
    """Represents a column definition in a table"""
    
    def __init__(
        self,
        name: str,
        data_type: DataType,
        max_length: Optional[int] = None,
        constraints: Optional[List[ColumnConstraint]] = None,
        default: Any = None
    ):
        self.name = name
        self.data_type = data_type
        self.max_length = max_length
        self.constraints = constraints or []
        self.default = default
        
        # Validate max_length for VARCHAR
        if data_type == DataType.VARCHAR and max_length is None:
            raise ValueError(f"VARCHAR column '{name}' must specify max_length")
        
        # Validate constraints
        if ColumnConstraint.PRIMARY_KEY in self.constraints:
            # Primary key implies NOT NULL and UNIQUE
            if ColumnConstraint.NOT_NULL not in self.constraints:
                self.constraints.append(ColumnConstraint.NOT_NULL)
    
    def is_primary_key(self) -> bool:
        return ColumnConstraint.PRIMARY_KEY in self.constraints
    
    def is_unique(self) -> bool:
        return ColumnConstraint.UNIQUE in self.constraints or self.is_primary_key()
    
    def is_not_null(self) -> bool:
        return ColumnConstraint.NOT_NULL in self.constraints
    
    def validate_value(self, value: Any) -> tuple[bool, Optional[str]]:
        """
        Validate a value against this column's type and constraints
        Returns: (is_valid, error_message)
        """
        # Check NOT NULL constraint
        if value is None:
            if self.is_not_null():
                return False, f"Column '{self.name}' cannot be NULL"
            return True, None  # NULL is valid for nullable columns
        
        # Type validation
        try:
            converted_value = self.convert_value(value)
        except (ValueError, TypeError) as e:
            return False, f"Invalid type for column '{self.name}': {str(e)}"
        
        # VARCHAR length validation
        if self.data_type == DataType.VARCHAR:
            if len(str(converted_value)) > self.max_length:
                return False, f"Value for '{self.name}' exceeds max length of {self.max_length}"
        
        return True, None
    
    def convert_value(self, value: Any) -> Any:
        """Convert and validate value to the correct type"""
        if value is None:
            return None
        
        if self.data_type == DataType.INTEGER:
            if isinstance(value, bool):  # bool is subclass of int, handle separately
                raise TypeError("Boolean cannot be converted to INTEGER")
            return int(value)
        
        elif self.data_type == DataType.VARCHAR or self.data_type == DataType.TEXT:
            return str(value)
        
        elif self.data_type == DataType.FLOAT:
            return float(value)
        
        elif self.data_type == DataType.BOOLEAN:
            if isinstance(value, bool):
                return value
            if isinstance(value, str):
                if value.lower() in ('true', '1', 'yes', 't', 'y'):
                    return True
                elif value.lower() in ('false', '0', 'no', 'f', 'n'):
                    return False
            if isinstance(value, int):
                return bool(value)
            raise TypeError(f"Cannot convert {value} to BOOLEAN")
        
        elif self.data_type == DataType.DATE:
            # IMPORTANT: datetime must be checked before date
            if isinstance(value, datetime):
                return value.date().isoformat()

            if isinstance(value, date):
                return value.isoformat()

            if isinstance(value, str):
                try:
                    parsed = datetime.fromisoformat(value).date()
                    return parsed.isoformat()
                except ValueError:
                    raise ValueError(
                        f"Invalid date format: {value}. Use YYYY-MM-DD"
                    )

            raise TypeError(f"Cannot convert {type(value)} to DATE")
        
        return value
    
    def to_dict(self) -> Dict[str, Any]:
        """Serialize column definition"""
        return {
            'name': self.name,
            'data_type': self.data_type.value,
            'max_length': self.max_length,
            'constraints': [c.value for c in self.constraints],
            'default': self.default
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Column':
        """Deserialize column definition"""
        return cls(
            name=data['name'],
            data_type=DataType(data['data_type']),
            max_length=data.get('max_length'),
            constraints=[ColumnConstraint(c) for c in data.get('constraints', [])],
            default=data.get('default')
        )
    
    def __repr__(self):
        constraints_str = ', '.join(c.value for c in self.constraints)
        length_str = f"({self.max_length})" if self.max_length else ""
        return f"{self.name} {self.data_type.value}{length_str} {constraints_str}".strip()


class Schema:
    """Represents a table schema with columns and constraints"""
    
    def __init__(self, table_name: str, columns: List[Column]):
        self.table_name = table_name
        self.columns = {col.name: col for col in columns}
        self._validate_schema()
    
    def _validate_schema(self):
        """Validate the schema definition"""
        # Check for duplicate column names
        if len(self.columns) != len(set(self.columns.keys())):
            raise ValueError("Duplicate column names found")
        
        # Check that there's at most one primary key
        primary_keys = [col for col in self.columns.values() if col.is_primary_key()]
        if len(primary_keys) > 1:
            raise ValueError("Table can have at most one PRIMARY KEY column")
    
    def get_column(self, name: str) -> Optional[Column]:
        """Get column by name"""
        return self.columns.get(name)
    
    def get_primary_key(self) -> Optional[Column]:
        """Get the primary key column if it exists"""
        for col in self.columns.values():
            if col.is_primary_key():
                return col
        return None
    
    def get_unique_columns(self) -> List[Column]:
        """Get all columns with UNIQUE constraint"""
        return [col for col in self.columns.values() if col.is_unique()]
    
    def validate_row(self, row_data: Dict[str, Any]) -> tuple[bool, List[str]]:
        """
        Validate a row of data against this schema
        Returns: (is_valid, list_of_errors)
        """
        errors = []
        
        # Check for unknown columns
        for col_name in row_data.keys():
            if col_name not in self.columns:
                errors.append(f"Unknown column: '{col_name}'")
        
        # Validate each column
        for col_name, column in self.columns.items():
            value = row_data.get(col_name)
            
            # Use default if value not provided
            if value is None and column.default is not None:
                row_data[col_name] = column.default
                value = column.default
            
            is_valid, error = column.validate_value(value)
            if not is_valid:
                errors.append(error)
        
        return len(errors) == 0, errors
    
    def convert_row(self, row_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Convert row data to correct types according to schema
        Also applies defaults for missing values
        """
        converted = {}
        
        for col_name, column in self.columns.items():
            value = row_data.get(col_name)
            
            # Apply default if missing
            if value is None and column.default is not None:
                value = column.default
            
            # Convert to correct type
            if value is not None:
                converted[col_name] = column.convert_value(value)
            else:
                converted[col_name] = None
        
        return converted
    
    def check_unique_constraint(
        self, 
        column_name: str, 
        value: Any, 
        existing_rows: List[Dict[str, Any]],
        exclude_row_id: Optional[int] = None
    ) -> tuple[bool, Optional[str]]:
        """
        Check if a value violates unique constraint
        exclude_row_id: Row ID to exclude from check (for updates)
        Returns: (is_valid, error_message)
        """
        column = self.get_column(column_name)
        if not column or not column.is_unique():
            return True, None
        
        if value is None:
            return True, None  # NULL values don't violate unique constraint
        
        # Check existing rows
        for row in existing_rows:
            # Skip the row being updated
            if exclude_row_id and row.get('row_id') == exclude_row_id:
                continue
            
            row_value = row.get('data', {}).get(column_name)
            if row_value == value:
                constraint_type = "PRIMARY KEY" if column.is_primary_key() else "UNIQUE"
                return False, f"{constraint_type} violation: '{column_name}' value '{value}' already exists"
        
        return True, None
    
    def to_dict(self) -> Dict[str, Any]:
        """Serialize schema"""
        return {
            'table_name': self.table_name,
            'columns': [col.to_dict() for col in self.columns.values()]
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Schema':
        """Deserialize schema"""
        columns = [Column.from_dict(col_data) for col_data in data['columns']]
        return cls(data['table_name'], columns)
    
    def __repr__(self):
        cols = '\n  '.join(str(col) for col in self.columns.values())
        return f"Table: {self.table_name}\n  {cols}"


class SchemaManager:
    """Manages schemas for all tables in the database"""
    
    def __init__(self):
        self.schemas: Dict[str, Schema] = {}
    
    def create_schema(self, table_name: str, columns: List[Column]) -> Schema:
        """Create and register a new schema"""
        if table_name in self.schemas:
            raise ValueError(f"Schema for table '{table_name}' already exists")
        
        schema = Schema(table_name, columns)
        self.schemas[table_name] = schema
        return schema
    
    def get_schema(self, table_name: str) -> Optional[Schema]:
        """Get schema for a table"""
        return self.schemas.get(table_name)
    
    def drop_schema(self, table_name: str):
        """Remove a schema"""
        if table_name in self.schemas:
            del self.schemas[table_name]
    
    def list_tables(self) -> List[str]:
        """List all tables with schemas"""
        return list(self.schemas.keys())
    
    def table_exists(self, table_name: str) -> bool:
        """Check if table schema exists"""
        return table_name in self.schemas


if __name__ == "__main__":
    print("Testing SimplDB Schema Manager\n")
    print("=" * 60)
    
    # Test 1: Create a schema
    print("\n1. Creating a user table schema...")
    columns = [
        Column("id", DataType.INTEGER, constraints=[ColumnConstraint.PRIMARY_KEY]),
        Column("username", DataType.VARCHAR, max_length=50, 
               constraints=[ColumnConstraint.UNIQUE, ColumnConstraint.NOT_NULL]),
        Column("email", DataType.VARCHAR, max_length=100, 
               constraints=[ColumnConstraint.UNIQUE, ColumnConstraint.NOT_NULL]),
        Column("age", DataType.INTEGER),
        Column("balance", DataType.FLOAT, default=0.0),
        Column("is_active", DataType.BOOLEAN, default=True),
        Column("created_at", DataType.DATE)
    ]
    
    schema = Schema("users", columns)
    print(schema)
    
    # Test 2: Validate valid data
    print("\n2. Validating valid data...")
    valid_data = {
        "id": 1,
        "username": "alice",
        "email": "alice@example.com",
        "age": 30,
        "balance": 100.50,
        "is_active": True,
        "created_at": "2025-01-15"
    }
    
    is_valid, errors = schema.validate_row(valid_data)
    print(f"   Valid: {is_valid}")
    if errors:
        for error in errors:
            print(f"   Error: {error}")
    
    # Test 3: Validate invalid data
    print("\n3. Validating invalid data (missing NOT NULL field)...")
    invalid_data = {
        "id": 2,
        "age": 25
    }
    
    is_valid, errors = schema.validate_row(invalid_data)
    print(f"   Valid: {is_valid}")
    for error in errors:
        print(f"   Error: {error}")
    
    # Test 4: Type conversion
    print("\n4. Testing type conversion...")
    data_to_convert = {
        "id": "5",
        "username": "bob",
        "email": "bob@example.com",
        "age": "25",
        "balance": "150.75",
        "is_active": "true",
        "created_at": "2025-01-15"
    }
    
    converted = schema.convert_row(data_to_convert)
    print(f"   Original: {data_to_convert}")
    print(f"   Converted: {converted}")
    print(f"   Types: id={type(converted['id'])}, balance={type(converted['balance'])}, is_active={type(converted['is_active'])}")
    
    # Test 5: Schema Manager
    print("\n5. Testing Schema Manager...")
    manager = SchemaManager()
    
    # Create users schema
    user_schema = manager.create_schema("users", columns)
    
    # Create posts schema
    post_columns = [
        Column("id", DataType.INTEGER, constraints=[ColumnConstraint.PRIMARY_KEY]),
        Column("title", DataType.VARCHAR, max_length=200, constraints=[ColumnConstraint.NOT_NULL]),
        Column("content", DataType.TEXT),
        Column("author_id", DataType.INTEGER, constraints=[ColumnConstraint.NOT_NULL]),
        Column("published", DataType.BOOLEAN, default=False)
    ]
    
    post_schema = manager.create_schema("posts", post_columns)
    
    print(f"   Tables: {manager.list_tables()}")
    print(f"   Users table exists: {manager.table_exists('users')}")
    print(f"   Comments table exists: {manager.table_exists('comments')}")
    
    # Test 6: Unique constraint check
    print("\n6. Testing UNIQUE constraint validation...")
    existing_rows = [
        {"row_id": 1, "data": {"id": 1, "username": "alice", "email": "alice@example.com"}},
        {"row_id": 2, "data": {"id": 2, "username": "bob", "email": "bob@example.com"}}
    ]
    
    # Check duplicate username
    is_valid, error = schema.check_unique_constraint("username", "alice", existing_rows)
    print(f"   Duplicate username 'alice': Valid={is_valid}, Error={error}")
    
    # Check new username
    is_valid, error = schema.check_unique_constraint("username", "charlie", existing_rows)
    print(f"   New username 'charlie': Valid={is_valid}, Error={error}")
    
    print("\n" + "=" * 60)
    print("✅ Schema manager test complete!")