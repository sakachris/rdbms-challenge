"""
SimplDB Storage Layer
Handles data persistence to disk using JSON files
"""

import json
import os
from pathlib import Path
from threading import Lock
from typing import Dict, List, Any, Optional
from datetime import datetime


class Row:
    """Represents a single row/record in a table"""
    
    def __init__(self, data: Dict[str, Any], row_id: Optional[int] = None):
        self.data = data
        self.row_id = row_id
        self.created_at = datetime.now().isoformat()
        self.updated_at = datetime.now().isoformat()
    
    def __getitem__(self, key: str) -> Any:
        """Allow dict-like access: row['column_name']"""
        return self.data.get(key)
    
    def __setitem__(self, key: str, value: Any):
        """Allow dict-like assignment: row['column_name'] = value"""
        self.data[key] = value
        self.updated_at = datetime.now().isoformat()
    
    def get(self, key: str, default=None) -> Any:
        """Get value with default"""
        return self.data.get(key, default)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert row to dictionary for serialization"""
        return {
            'row_id': self.row_id,
            'data': self.data,
            'created_at': self.created_at,
            'updated_at': self.updated_at
        }
    
    @classmethod
    def from_dict(cls, row_dict: Dict[str, Any]) -> 'Row':
        """Create Row from dictionary"""
        row = cls(row_dict['data'], row_dict['row_id'])
        row.created_at = row_dict.get('created_at', row.created_at)
        row.updated_at = row_dict.get('updated_at', row.updated_at)
        return row
    
    def __repr__(self):
        return f"Row(id={self.row_id}, data={self.data})"


class TableStorage:
    """Manages storage for a single table"""
    
    def __init__(self, table_name: str, data_dir: Path):
        self.table_name = table_name
        self.data_dir = Path(data_dir)
        self.file_path = self.data_dir / f"{table_name}.json"
        self.lock = Lock()  # Thread-safe operations
        
        # Ensure data directory exists
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize file if it doesn't exist
        if not self.file_path.exists():
            self._write_data({'rows': [], 'next_id': 1, 'metadata': {}})
    
    def _read_data(self) -> Dict[str, Any]:
        """Read data from disk"""
        try:
            with open(self.file_path, 'r') as f:
                return json.load(f)
        except (json.JSONDecodeError, FileNotFoundError):
            # If file is corrupted or missing, return empty structure
            return {'rows': [], 'next_id': 1, 'metadata': {}}
    
    def _write_data(self, data: Dict[str, Any]):
        """Write data to disk"""
        with open(self.file_path, 'w') as f:
            json.dump(data, f, indent=2)
    
    def insert(self, row_data: Dict[str, Any]) -> int:
        """Insert a new row and return its ID"""
        with self.lock:
            data = self._read_data()
            row_id = data['next_id']
            
            row = Row(row_data, row_id)
            data['rows'].append(row.to_dict())
            data['next_id'] += 1
            
            self._write_data(data)
            return row_id
    
    def insert_many(self, rows_data: List[Dict[str, Any]]) -> List[int]:
        """Insert multiple rows efficiently"""
        with self.lock:
            data = self._read_data()
            inserted_ids = []
            
            for row_data in rows_data:
                row_id = data['next_id']
                row = Row(row_data, row_id)
                data['rows'].append(row.to_dict())
                data['next_id'] += 1
                inserted_ids.append(row_id)
            
            self._write_data(data)
            return inserted_ids
    
    def select_all(self) -> List[Row]:
        """Retrieve all rows"""
        with self.lock:
            data = self._read_data()
            return [Row.from_dict(row_dict) for row_dict in data['rows']]
    
    def select_by_id(self, row_id: int) -> Optional[Row]:
        """Retrieve a specific row by ID"""
        with self.lock:
            data = self._read_data()
            for row_dict in data['rows']:
                if row_dict['row_id'] == row_id:
                    return Row.from_dict(row_dict)
            return None
    
    def update_by_id(self, row_id: int, updates: Dict[str, Any]) -> bool:
        """Update a row by ID, returns True if successful"""
        with self.lock:
            data = self._read_data()
            for i, row_dict in enumerate(data['rows']):
                if row_dict['row_id'] == row_id:
                    # Update the data
                    row = Row.from_dict(row_dict)
                    for key, value in updates.items():
                        row[key] = value
                    data['rows'][i] = row.to_dict()
                    self._write_data(data)
                    return True
            return False
    
    def delete_by_id(self, row_id: int) -> bool:
        """Delete a row by ID, returns True if successful"""
        with self.lock:
            data = self._read_data()
            original_count = len(data['rows'])
            data['rows'] = [r for r in data['rows'] if r['row_id'] != row_id]
            
            if len(data['rows']) < original_count:
                self._write_data(data)
                return True
            return False
    
    def delete_all(self) -> int:
        """Delete all rows, returns count of deleted rows"""
        with self.lock:
            data = self._read_data()
            count = len(data['rows'])
            data['rows'] = []
            self._write_data(data)
            return count
    
    def count(self) -> int:
        """Count total rows"""
        with self.lock:
            data = self._read_data()
            return len(data['rows'])
    
    def set_metadata(self, key: str, value: Any):
        """Store metadata about the table"""
        with self.lock:
            data = self._read_data()
            data['metadata'][key] = value
            self._write_data(data)
    
    def get_metadata(self, key: str, default=None) -> Any:
        """Retrieve metadata"""
        with self.lock:
            data = self._read_data()
            return data.get('metadata', {}).get(key, default)
    
    def drop(self):
        """Delete the table file"""
        with self.lock:
            if self.file_path.exists():
                self.file_path.unlink()


class Storage:
    """Main storage manager for all tables"""
    
    def __init__(self, data_dir: str = "data"):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self._tables: Dict[str, TableStorage] = {}
    
    def get_table(self, table_name: str) -> TableStorage:
        """Get or create a table storage"""
        if table_name not in self._tables:
            self._tables[table_name] = TableStorage(table_name, self.data_dir)
        return self._tables[table_name]
    
    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists"""
        file_path = self.data_dir / f"{table_name}.json"
        return file_path.exists()
    
    def list_tables(self) -> List[str]:
        """List all table names"""
        return [f.stem for f in self.data_dir.glob("*.json")]
    
    def drop_table(self, table_name: str):
        """Drop a table"""
        if table_name in self._tables:
            self._tables[table_name].drop()
            del self._tables[table_name]
        else:
            table = TableStorage(table_name, self.data_dir)
            table.drop()
    
    def clear_all(self):
        """Clear all tables (use with caution!)"""
        for table_name in list(self._tables.keys()):
            self.drop_table(table_name)


if __name__ == "__main__":
    # Quick test of the storage layer
    print("Testing SimplDB Storage Layer\n")
    
    # Initialize storage
    storage = Storage("data")
    
    # Get a table
    users = storage.get_table("users")
    
    # Insert some data
    print("1. Inserting users...")
    id1 = users.insert({"name": "Alice", "age": 30, "email": "alice@example.com"})
    id2 = users.insert({"name": "Bob", "age": 25, "email": "bob@example.com"})
    id3 = users.insert({"name": "Charlie", "age": 35, "email": "charlie@example.com"})
    print(f"   Inserted IDs: {id1}, {id2}, {id3}")
    
    # Read all
    print("\n2. Reading all users...")
    all_users = users.select_all()
    for user in all_users:
        print(f"   {user}")
    
    # Update
    print("\n3. Updating user...")
    users.update_by_id(id1, {"age": 31, "email": "alice.updated@example.com"})
    updated = users.select_by_id(id1)
    print(f"   Updated: {updated}")
    
    # Delete
    print("\n4. Deleting user...")
    users.delete_by_id(id2)
    remaining = users.select_all()
    print(f"   Remaining users: {len(remaining)}")
    
    # Count
    print(f"\n5. Total users: {users.count()}")
    
    # List tables
    print(f"\n6. Tables in database: {storage.list_tables()}")
    
    print("\n✅ Storage layer test complete!")
    print("Check the 'data' directory for the generated JSON files.")
