"""
SimplDB - A Simple Relational Database Management System
"""

from .storage import Storage, TableStorage, Row
from .schema import Schema, SchemaManager, Column, DataType, ColumnConstraint
from .indexes import Index, IndexManager, BTreeNode
from .parser import SQLParser, Query, QueryType, Operator
from .executor import QueryExecutor, QueryResult
from .database import Database, DatabaseCatalog, Transaction
from .repl import SimplDBREPL

__version__ = '0.1.0'

__all__ = [
    # Storage
    'Storage', 'TableStorage', 'Row',
    
    # Schema
    'Schema', 'SchemaManager', 'Column', 'DataType', 'ColumnConstraint',
    
    # Indexes
    'Index', 'IndexManager', 'BTreeNode',
    
    # Parser
    'SQLParser', 'Query', 'QueryType', 'Operator',
    
    # Executor
    'QueryExecutor', 'QueryResult',
    
    # Database
    'Database', 'DatabaseCatalog', 'Transaction',
    
    # REPL
    'SimplDBREPL',
]



# """
# SimplDB - A Simple Relational Database Management System
# """

# from .storage import Storage, TableStorage, Row
# from .schema import Schema, SchemaManager, Column, DataType, ColumnConstraint
# from .indexes import Index, IndexManager, BTreeNode
# from .parser import SQLParser, Query, QueryType, Operator
# from .executor import QueryExecutor, QueryResult
# from .database import Database, DatabaseCatalog, Transaction

# __version__ = '0.1.0'

# __all__ = [
#     # Storage
#     'Storage', 'TableStorage', 'Row',
    
#     # Schema
#     'Schema', 'SchemaManager', 'Column', 'DataType', 'ColumnConstraint',
    
#     # Indexes
#     'Index', 'IndexManager', 'BTreeNode',
    
#     # Parser
#     'SQLParser', 'Query', 'QueryType', 'Operator',
    
#     # Executor
#     'QueryExecutor', 'QueryResult',
    
#     # Database
#     'Database', 'DatabaseCatalog', 'Transaction',
# ]