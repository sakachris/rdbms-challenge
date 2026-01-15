"""
SimplDB SQL Parser
Parses SQL-like queries into structured query objects
"""

import re
from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass
from enum import Enum


class QueryType(Enum):
    """Types of SQL queries"""
    CREATE_TABLE = "CREATE_TABLE"
    DROP_TABLE = "DROP_TABLE"
    CREATE_INDEX = "CREATE_INDEX"
    DROP_INDEX = "DROP_INDEX"
    INSERT = "INSERT"
    SELECT = "SELECT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"


class JoinType(Enum):
    """Types of joins"""
    INNER = "INNER"
    LEFT = "LEFT"
    RIGHT = "RIGHT"


class Operator(Enum):
    """Comparison operators for WHERE clauses"""
    EQ = "="
    NE = "!="
    LT = "<"
    LE = "<="
    GT = ">"
    GE = ">="
    LIKE = "LIKE"
    IN = "IN"
    IS_NULL = "IS NULL"
    IS_NOT_NULL = "IS NOT NULL"


@dataclass
class Condition:
    """Represents a WHERE clause condition"""
    column: str
    operator: Operator
    value: Any
    
    def __repr__(self):
        return f"{self.column} {self.operator.value} {self.value}"


@dataclass
class JoinClause:
    """Represents a JOIN clause"""
    join_type: JoinType
    table: str
    on_left: str  # left.column
    on_right: str  # right.column
    
    def __repr__(self):
        return f"{self.join_type.value} JOIN {self.table} ON {self.on_left} = {self.on_right}"


@dataclass
class ColumnDef:
    """Column definition for CREATE TABLE"""
    name: str
    data_type: str
    constraints: List[str]
    max_length: Optional[int] = None
    default: Optional[Any] = None
    
    def __repr__(self):
        constraints_str = ' '.join(self.constraints)
        length_str = f"({self.max_length})" if self.max_length else ""
        default_str = f" DEFAULT {self.default}" if self.default is not None else ""
        return f"{self.name} {self.data_type}{length_str} {constraints_str}{default_str}".strip()


@dataclass
class Query:
    """Base query object"""
    query_type: QueryType
    raw_sql: str


@dataclass
class CreateTableQuery(Query):
    """CREATE TABLE query"""
    table_name: str
    columns: List[ColumnDef]


@dataclass
class DropTableQuery(Query):
    """DROP TABLE query"""
    table_name: str


@dataclass
class CreateIndexQuery(Query):
    """CREATE INDEX query"""
    index_name: str
    table_name: str
    column_name: str
    unique: bool = False


@dataclass
class DropIndexQuery(Query):
    """DROP INDEX query"""
    index_name: str
    table_name: str


@dataclass
class InsertQuery(Query):
    """INSERT query"""
    table_name: str
    columns: List[str]
    values: List[Any]


@dataclass
class SelectQuery(Query):
    """SELECT query"""
    table_name: str
    columns: List[str]  # ['*'] for all columns
    where: Optional[List[Condition]] = None
    joins: Optional[List[JoinClause]] = None
    order_by: Optional[List[tuple[str, str]]] = None  # [(column, 'ASC'/'DESC')]
    limit: Optional[int] = None
    offset: Optional[int] = None


@dataclass
class UpdateQuery(Query):
    """UPDATE query"""
    table_name: str
    updates: Dict[str, Any]
    where: Optional[List[Condition]] = None


@dataclass
class DeleteQuery(Query):
    """DELETE query"""
    table_name: str
    where: Optional[List[Condition]] = None


class SQLParser:
    """Parser for SQL-like queries"""
    
    def __init__(self):
        self.query_patterns = {
            QueryType.CREATE_TABLE: r'CREATE\s+TABLE\s+(\w+)\s*\((.*)\)\s*$',
            QueryType.DROP_TABLE: r'DROP\s+TABLE\s+(\w+)',
            QueryType.CREATE_INDEX: r'CREATE\s+(UNIQUE\s+)?INDEX\s+(\w+)\s+ON\s+(\w+)\s*\((\w+)\)',
            QueryType.DROP_INDEX: r'DROP\s+INDEX\s+(\w+)\s+ON\s+(\w+)',
            QueryType.INSERT: r'INSERT\s+INTO\s+(\w+)\s*\((.*?)\)\s+VALUES\s*\((.*?)\)',
            QueryType.SELECT: r'SELECT\s+(.*?)\s+FROM\s+(\w+)(.*)',
            QueryType.UPDATE: r'UPDATE\s+(\w+)\s+SET\s+(.*?)(?:\s+WHERE\s+(.*))?$',
            QueryType.DELETE: r'DELETE\s+FROM\s+(\w+)(?:\s+WHERE\s+(.*))?$',
        }
    
    def parse(self, sql: str) -> Query:
        """Parse SQL string into a Query object"""
        sql = sql.strip()
        if not sql:
            raise ValueError("Empty SQL query")
        
        # Remove trailing semicolon
        if sql.endswith(';'):
            sql = sql[:-1].strip()
        
        # Determine query type
        sql_upper = sql.upper()
        
        if sql_upper.startswith('CREATE TABLE'):
            return self._parse_create_table(sql)
        elif sql_upper.startswith('DROP TABLE'):
            return self._parse_drop_table(sql)
        elif sql_upper.startswith('CREATE'):
            return self._parse_create_index(sql)
        elif sql_upper.startswith('DROP INDEX'):
            return self._parse_drop_index(sql)
        elif sql_upper.startswith('INSERT'):
            return self._parse_insert(sql)
        elif sql_upper.startswith('SELECT'):
            return self._parse_select(sql)
        elif sql_upper.startswith('UPDATE'):
            return self._parse_update(sql)
        elif sql_upper.startswith('DELETE'):
            return self._parse_delete(sql)
        else:
            raise ValueError(f"Unsupported query type: {sql[:20]}...")
 
    def _parse_create_table(self, sql: str) -> CreateTableQuery:
        """Parse CREATE TABLE query"""
        match = re.match(self.query_patterns[QueryType.CREATE_TABLE], sql, re.IGNORECASE | re.DOTALL)
        if not match:
            raise ValueError(f"Invalid CREATE TABLE syntax: {sql}")
        
        table_name = match.group(1)
        columns_str = match.group(2)
        
        columns = []
        for col_def in self._split_columns(columns_str):
            columns.append(self._parse_column_def(col_def))
        
        return CreateTableQuery(
            query_type=QueryType.CREATE_TABLE,
            raw_sql=sql,
            table_name=table_name,
            columns=columns
        )
    
    def _parse_column_def(self, col_def: str) -> ColumnDef:
        """Parse column definition"""
        col_def = col_def.strip()
        parts = col_def.split()
        
        if len(parts) < 2:
            raise ValueError(f"Invalid column definition: {col_def}")
        
        name = parts[0]
        data_type_raw = parts[1]
        
        # Extract max_length from VARCHAR(100)
        max_length = None
        data_type = data_type_raw
        if '(' in data_type_raw:
            match = re.match(r'(\w+)\((\d+)\)', data_type_raw)
            if match:
                data_type = match.group(1)
                max_length = int(match.group(2))
            else:
                # Handle case where closing paren might be in next token
                # e.g., "VARCHAR(100" and ")" are separate
                match = re.match(r'(\w+)\((\d+)', data_type_raw)
                if match:
                    data_type = match.group(1)
                    max_length = int(match.group(2))
                    # Skip the next token if it's just ")"
                    if len(parts) > 2 and parts[2].strip() == ')':
                        parts.pop(2)
        
        # Parse constraints and default
        constraints = []
        default = None
        i = 2
        
        while i < len(parts):
            part_upper = parts[i].upper()
            
            if part_upper == 'PRIMARY' and i + 1 < len(parts) and parts[i + 1].upper() == 'KEY':
                constraints.append('PRIMARY_KEY')
                i += 2
            elif part_upper == 'UNIQUE':
                constraints.append('UNIQUE')
                i += 1
            elif part_upper == 'NOT' and i + 1 < len(parts) and parts[i + 1].upper() == 'NULL':
                constraints.append('NOT_NULL')
                i += 2
            elif part_upper == 'DEFAULT':
                if i + 1 < len(parts):
                    default = self._parse_value(parts[i + 1])
                i += 2
            else:
                i += 1
        
        return ColumnDef(
            name=name,
            data_type=data_type,
            constraints=constraints,
            max_length=max_length,
            default=default
        )
    
    def _parse_drop_table(self, sql: str) -> DropTableQuery:
        """Parse DROP TABLE query"""
        match = re.match(self.query_patterns[QueryType.DROP_TABLE], sql, re.IGNORECASE)
        if not match:
            raise ValueError(f"Invalid DROP TABLE syntax: {sql}")
        
        return DropTableQuery(
            query_type=QueryType.DROP_TABLE,
            raw_sql=sql,
            table_name=match.group(1)
        )
    
    def _parse_create_index(self, sql: str) -> CreateIndexQuery:
        """Parse CREATE INDEX query"""
        match = re.match(self.query_patterns[QueryType.CREATE_INDEX], sql, re.IGNORECASE)
        if not match:
            raise ValueError(f"Invalid CREATE INDEX syntax: {sql}")
        
        unique = match.group(1) is not None
        index_name = match.group(2)
        table_name = match.group(3)
        column_name = match.group(4)
        
        return CreateIndexQuery(
            query_type=QueryType.CREATE_INDEX,
            raw_sql=sql,
            index_name=index_name,
            table_name=table_name,
            column_name=column_name,
            unique=unique
        )
    
    def _parse_drop_index(self, sql: str) -> DropIndexQuery:
        """Parse DROP INDEX query"""
        match = re.match(self.query_patterns[QueryType.DROP_INDEX], sql, re.IGNORECASE)
        if not match:
            raise ValueError(f"Invalid DROP INDEX syntax: {sql}")
        
        return DropIndexQuery(
            query_type=QueryType.DROP_INDEX,
            raw_sql=sql,
            index_name=match.group(1),
            table_name=match.group(2)
        )
    
    def _parse_insert(self, sql: str) -> InsertQuery:
        """Parse INSERT query"""
        match = re.match(self.query_patterns[QueryType.INSERT], sql, re.IGNORECASE | re.DOTALL)
        if not match:
            raise ValueError(f"Invalid INSERT syntax: {sql}")
        
        table_name = match.group(1)
        columns_str = match.group(2)
        values_str = match.group(3)
        
        columns = [col.strip() for col in columns_str.split(',')]
        values = [self._parse_value(val.strip()) for val in self._split_values(values_str)]
        
        if len(columns) != len(values):
            raise ValueError(f"Column count ({len(columns)}) doesn't match value count ({len(values)})")
        
        return InsertQuery(
            query_type=QueryType.INSERT,
            raw_sql=sql,
            table_name=table_name,
            columns=columns,
            values=values
        )
    
    def _parse_select(self, sql: str) -> SelectQuery:
        """Parse SELECT query"""
        match = re.match(self.query_patterns[QueryType.SELECT], sql, re.IGNORECASE | re.DOTALL)
        if not match:
            raise ValueError(f"Invalid SELECT syntax: {sql}")
        
        columns_str = match.group(1).strip()
        table_name = match.group(2)
        rest = match.group(3).strip() if match.group(3) else ""
        
        # Parse columns
        if columns_str == '*':
            columns = ['*']
        else:
            columns = [col.strip() for col in columns_str.split(',')]

        # Parse optional clauses
        joins: List[JoinClause] = []
        where = None
        order_by = None
        limit = None
        offset = None

        if rest:
            # Parse JOIN clauses
            join_match = re.search(
                r'(INNER|LEFT|RIGHT)\s+JOIN\s+(\w+)\s+(\w+)\s+ON\s+(\w+\.\w+)\s*=\s*(\w+\.\w+)',
                rest,
                re.IGNORECASE
            )

            if join_match:
                join_type = JoinType[join_match.group(1).upper()]
                join_table_alias = join_match.group(3)   # <-- alias, not table name
                on_left = join_match.group(4)
                on_right = join_match.group(5)

                joins.append(
                    JoinClause(
                        join_type=join_type,
                        table=join_table_alias,
                        on_left=on_left,
                        on_right=on_right
                    )
                )
            
            # Parse WHERE clause
            where_match = re.search(r'WHERE\s+(.*?)(?:\s+ORDER BY|\s+LIMIT|\s*$)', rest, re.IGNORECASE | re.DOTALL)
            if where_match:
                where = self._parse_where(where_match.group(1))
            
            # Parse ORDER BY
            order_match = re.search(r'ORDER BY\s+(.*?)(?:\s+LIMIT|\s*$)', rest, re.IGNORECASE)
            if order_match:
                order_by = self._parse_order_by(order_match.group(1))
            
            # Parse LIMIT
            limit_match = re.search(r'LIMIT\s+(\d+)(?:\s+OFFSET\s+(\d+))?', rest, re.IGNORECASE)
            if limit_match:
                limit = int(limit_match.group(1))
                if limit_match.group(2):
                    offset = int(limit_match.group(2))
        
        return SelectQuery(
            query_type=QueryType.SELECT,
            raw_sql=sql,
            table_name=table_name,
            columns=columns,
            where=where,
            joins=joins,
            order_by=order_by,
            limit=limit,
            offset=offset
        )
    
    def _parse_update(self, sql: str) -> UpdateQuery:
        """Parse UPDATE query"""
        match = re.match(self.query_patterns[QueryType.UPDATE], sql, re.IGNORECASE | re.DOTALL)
        if not match:
            raise ValueError(f"Invalid UPDATE syntax: {sql}")
        
        table_name = match.group(1)
        set_clause = match.group(2)
        where_clause = match.group(3)
        
        # Parse SET clause
        updates = {}
        for assignment in set_clause.split(','):
            parts = assignment.strip().split('=')
            if len(parts) != 2:
                raise ValueError(f"Invalid SET clause: {assignment}")
            column = parts[0].strip()
            value = self._parse_value(parts[1].strip())
            updates[column] = value
        
        # Parse WHERE clause
        where = None
        if where_clause:
            where = self._parse_where(where_clause)
        
        return UpdateQuery(
            query_type=QueryType.UPDATE,
            raw_sql=sql,
            table_name=table_name,
            updates=updates,
            where=where
        )
    
    def _parse_delete(self, sql: str) -> DeleteQuery:
        """Parse DELETE query"""
        match = re.match(self.query_patterns[QueryType.DELETE], sql, re.IGNORECASE | re.DOTALL)
        if not match:
            raise ValueError(f"Invalid DELETE syntax: {sql}")
        
        table_name = match.group(1)
        where_clause = match.group(2)
        
        where = None
        if where_clause:
            where = self._parse_where(where_clause)
        
        return DeleteQuery(
            query_type=QueryType.DELETE,
            raw_sql=sql,
            table_name=table_name,
            where=where
        )
    
    def _parse_where(self, where_clause: str) -> List[Condition]:
        """Parse WHERE clause into conditions"""
        conditions = []
        
        # Split by AND (simple parsing, doesn't handle OR or complex expressions)
        parts = re.split(r'\s+AND\s+', where_clause, flags=re.IGNORECASE)
        
        for part in parts:
            part = part.strip()
            
            # Check for IS NULL / IS NOT NULL
            if re.search(r'IS\s+NOT\s+NULL', part, re.IGNORECASE):
                column = re.match(r'(\w+)\s+IS\s+NOT\s+NULL', part, re.IGNORECASE).group(1)
                conditions.append(Condition(column, Operator.IS_NOT_NULL, None))
            elif re.search(r'IS\s+NULL', part, re.IGNORECASE):
                column = re.match(r'(\w+)\s+IS\s+NULL', part, re.IGNORECASE).group(1)
                conditions.append(Condition(column, Operator.IS_NULL, None))
            else:
                # Parse comparison operators
                for op in ['<=', '>=', '!=', '<', '>', '=', 'LIKE']:
                    if op in part.upper() or op in part:
                        if op == 'LIKE':
                            match = re.search(r'(\w+)\s+LIKE\s+(.+)', part, re.IGNORECASE)
                        else:
                            parts_split = part.split(op)
                            if len(parts_split) == 2:
                                match = True
                                column = parts_split[0].strip()
                                value_str = parts_split[1].strip()
                        
                        if op == 'LIKE' and match:
                            column = match.group(1)
                            value_str = match.group(2).strip()
                            value = self._parse_value(value_str)
                            conditions.append(Condition(column, Operator.LIKE, value))
                        elif match and op != 'LIKE':
                            value = self._parse_value(value_str)
                            op_enum = {
                                '=': Operator.EQ, '!=': Operator.NE,
                                '<': Operator.LT, '<=': Operator.LE,
                                '>': Operator.GT, '>=': Operator.GE
                            }[op]
                            conditions.append(Condition(column, op_enum, value))
                        break
        
        return conditions
    
    def _parse_order_by(self, order_clause: str) -> List[tuple[str, str]]:
        """Parse ORDER BY clause"""
        order_by = []
        
        for part in order_clause.split(','):
            part = part.strip()
            if ' ' in part:
                column, direction = part.rsplit(None, 1)
                direction = direction.upper()
                if direction not in ('ASC', 'DESC'):
                    direction = 'ASC'
            else:
                column = part
                direction = 'ASC'
            
            order_by.append((column, direction))
        
        return order_by
    
    def _parse_value(self, value_str: str) -> Any:
        """Parse a value from string"""
        value_str = value_str.strip()
        
        # NULL
        if value_str.upper() == 'NULL':
            return None
        
        # String (quoted)
        if (value_str.startswith("'") and value_str.endswith("'")) or \
           (value_str.startswith('"') and value_str.endswith('"')):
            return value_str[1:-1]
        
        # Boolean
        if value_str.upper() in ('TRUE', 'FALSE'):
            return value_str.upper() == 'TRUE'
        
        # Number
        try:
            if '.' in value_str:
                return float(value_str)
            else:
                return int(value_str)
        except ValueError:
            pass
        
        # Default: treat as string
        return value_str
    
    def _split_columns(self, columns_str: str) -> List[str]:
        """Split column definitions, handling parentheses"""
        columns = []
        current = []
        depth = 0
        
        for char in columns_str:
            if char == '(':
                depth += 1
                current.append(char)
            elif char == ')':
                depth -= 1
                current.append(char)
            elif char == ',' and depth == 0:
                columns.append(''.join(current).strip())
                current = []
            else:
                current.append(char)
        
        if current:
            columns.append(''.join(current).strip())
        
        return columns
    
    def _split_values(self, values_str: str) -> List[str]:
        """Split values, handling quoted strings"""
        values = []
        current = []
        in_quote = False
        quote_char = None
        
        for char in values_str:
            if char in ('"', "'") and not in_quote:
                in_quote = True
                quote_char = char
                current.append(char)
            elif char == quote_char and in_quote:
                in_quote = False
                quote_char = None
                current.append(char)
            elif char == ',' and not in_quote:
                values.append(''.join(current).strip())
                current = []
            else:
                current.append(char)
        
        if current:
            values.append(''.join(current).strip())
        
        return values


if __name__ == "__main__":
    print("Testing SimplDB SQL Parser\n")
    print("=" * 70)
    
    parser = SQLParser()
    
    # Test queries
    test_queries = [
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(100) NOT NULL, age INTEGER, balance FLOAT DEFAULT 0.0)",
        "DROP TABLE users",
        "CREATE INDEX idx_email ON users(email)",
        "CREATE UNIQUE INDEX idx_username ON users(username)",
        "DROP INDEX idx_email ON users",
        "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30)",
        "SELECT * FROM users",
        "SELECT id, name FROM users WHERE age > 25",
        "SELECT * FROM users WHERE age >= 25 AND name = 'Alice'",
        "SELECT * FROM users ORDER BY age DESC LIMIT 10",
        "SELECT * FROM users WHERE email IS NOT NULL",
        "UPDATE users SET age = 31, balance = 100.0 WHERE id = 1",
        "DELETE FROM users WHERE id = 1",
        "SELECT u.id, u.name, p.title FROM users u INNER JOIN posts p ON u.id = p.author_id",
    ]
    
    for i, sql in enumerate(test_queries, 1):
        print(f"\n{i}. SQL: {sql}")
        try:
            query = parser.parse(sql)
            print(f"   Type: {query.query_type.value}")
            print(f"   Parsed: {query}")
        except Exception as e:
            print(f"   Error: {e}")
    
    print("\n" + "=" * 70)
    print("✅ SQL parser test complete!")
