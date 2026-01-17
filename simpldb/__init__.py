"""
SimplDB - A Simple Relational Database Management System
"""

from .database import Database, DatabaseCatalog, Transaction
from .executor import QueryExecutor, QueryResult
from .indexes import BTreeNode, Index, IndexManager
from .parser import Operator, Query, QueryType, SQLParser
from .schema import Column, ColumnConstraint, DataType, Schema, SchemaManager
from .storage import Row, Storage, TableStorage

__version__ = "0.1.0"

__all__ = [
    # Storage
    "Storage",
    "TableStorage",
    "Row",
    # Schema
    "Schema",
    "SchemaManager",
    "Column",
    "DataType",
    "ColumnConstraint",
    # Indexes
    "Index",
    "IndexManager",
    "BTreeNode",
    # Parser
    "SQLParser",
    "Query",
    "QueryType",
    "Operator",
    # Executor
    "QueryExecutor",
    "QueryResult",
    # Database
    "Database",
    "DatabaseCatalog",
    "Transaction",
]
