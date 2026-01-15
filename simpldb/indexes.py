"""
SimplDB Index Manager
Implements B-Tree-like indexes for fast data retrieval
"""

import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from threading import Lock
from bisect import bisect_left, insort


class BTreeNode:
    """
    Simplified B-Tree node for indexing
    In a full implementation, this would have splitting/merging logic
    For simplicity, we use sorted lists with binary search
    """
    
    def __init__(self, is_leaf: bool = True):
        self.is_leaf = is_leaf
        self.keys: List[Any] = []
        self.values: List[List[int]] = []  # Each key can map to multiple row IDs
    
    def insert(self, key: Any, row_id: int):
        """Insert a key-value pair"""
        # Find position using binary search
        pos = bisect_left(self.keys, key)
        
        if pos < len(self.keys) and self.keys[pos] == key:
            # Key exists, append row_id if not already present
            if row_id not in self.values[pos]:
                self.values[pos].append(row_id)
        else:
            # New key, insert at position
            self.keys.insert(pos, key)
            self.values.insert(pos, [row_id])
    
    def search(self, key: Any) -> List[int]:
        """Search for a key, returns list of row IDs"""
        pos = bisect_left(self.keys, key)
        if pos < len(self.keys) and self.keys[pos] == key:
            return self.values[pos].copy()
        return []
    
    def range_search(self, start_key: Any, end_key: Any, 
                     include_start: bool = True, 
                     include_end: bool = True) -> List[int]:
        """Search for keys in range [start_key, end_key]"""
        result = []
        
        # Find start position
        start_pos = bisect_left(self.keys, start_key)
        
        # Collect all matching keys
        for i in range(start_pos, len(self.keys)):
            key = self.keys[i]
            
            # Check if we've passed the end
            if key > end_key:
                break
            if key == end_key and not include_end:
                break
            
            # Check if we should include start
            if key == start_key and not include_start:
                continue
            
            result.extend(self.values[i])
        
        return result
    
    def delete(self, key: Any, row_id: Optional[int] = None):
        """
        Delete a key or a specific row_id from a key
        If row_id is None, delete the entire key
        """
        pos = bisect_left(self.keys, key)
        
        if pos < len(self.keys) and self.keys[pos] == key:
            if row_id is None:
                # Delete entire key
                del self.keys[pos]
                del self.values[pos]
            else:
                # Delete specific row_id
                if row_id in self.values[pos]:
                    self.values[pos].remove(row_id)
                    
                    # If no more values, remove the key
                    if not self.values[pos]:
                        del self.keys[pos]
                        del self.values[pos]
    
    def get_all_keys(self) -> List[Tuple[Any, List[int]]]:
        """Get all key-value pairs"""
        return list(zip(self.keys, self.values))
    
    def to_dict(self) -> Dict:
        """Serialize node"""
        return {
            'is_leaf': self.is_leaf,
            'keys': self.keys,
            'values': self.values
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'BTreeNode':
        """Deserialize node"""
        node = cls(is_leaf=data['is_leaf'])
        node.keys = data['keys']
        node.values = data['values']
        return node


class Index:
    """
    Index structure for fast lookups
    Uses a simplified B-Tree implementation
    """
    
    def __init__(self, table_name: str, column_name: str, unique: bool = False):
        self.table_name = table_name
        self.column_name = column_name
        self.unique = unique
        self.root = BTreeNode()
        self.lock = Lock()
    
    def insert(self, key: Any, row_id: int) -> Tuple[bool, Optional[str]]:
        """
        Insert a key-value pair into the index
        Returns: (success, error_message)
        """
        with self.lock:
            # Check uniqueness constraint
            if self.unique:
                existing = self.root.search(key)
                if existing and row_id not in existing:
                    return False, f"Unique constraint violation on {self.column_name}: '{key}' already exists"
            
            self.root.insert(key, row_id)
            return True, None
    
    def search(self, key: Any) -> List[int]:
        """Search for exact key match"""
        with self.lock:
            return self.root.search(key)
    
    def range_search(self, start_key: Any = None, end_key: Any = None,
                     include_start: bool = True, include_end: bool = True) -> List[int]:
        """
        Search for keys in a range
        If start_key is None, starts from beginning
        If end_key is None, goes to end
        """
        with self.lock:
            if start_key is None and end_key is None:
                # Return all
                result = []
                for _, row_ids in self.root.get_all_keys():
                    result.extend(row_ids)
                return result
            
            # Get all keys and filter
            all_keys = self.root.get_all_keys()
            result = []
            
            for key, row_ids in all_keys:
                include = True
                
                if start_key is not None:
                    if key < start_key:
                        include = False
                    elif key == start_key and not include_start:
                        include = False
                
                if end_key is not None:
                    if key > end_key:
                        include = False
                    elif key == end_key and not include_end:
                        include = False
                
                if include:
                    result.extend(row_ids)
            
            return result
    
    def delete(self, key: Any, row_id: Optional[int] = None):
        """Delete a key or specific row_id from index"""
        with self.lock:
            self.root.delete(key, row_id)
    
    def update(self, old_key: Any, new_key: Any, row_id: int) -> Tuple[bool, Optional[str]]:
        """Update an indexed value"""
        with self.lock:
            # Delete old entry
            self.root.delete(old_key, row_id)
            
            # Check uniqueness for new key
            if self.unique:
                existing = self.root.search(new_key)
                if existing and row_id not in existing:
                    # Rollback - reinsert old key
                    self.root.insert(old_key, row_id)
                    return False, f"Unique constraint violation on {self.column_name}: '{new_key}' already exists"
            
            # Insert new entry
            self.root.insert(new_key, row_id)
            return True, None
    
    def get_stats(self) -> Dict[str, Any]:
        """Get index statistics"""
        with self.lock:
            all_keys = self.root.get_all_keys()
            total_entries = sum(len(row_ids) for _, row_ids in all_keys)
            
            return {
                'table': self.table_name,
                'column': self.column_name,
                'unique': self.unique,
                'distinct_keys': len(all_keys),
                'total_entries': total_entries
            }
    
    def to_dict(self) -> Dict:
        """Serialize index"""
        return {
            'table_name': self.table_name,
            'column_name': self.column_name,
            'unique': self.unique,
            'root': self.root.to_dict()
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'Index':
        """Deserialize index"""
        index = cls(data['table_name'], data['column_name'], data['unique'])
        index.root = BTreeNode.from_dict(data['root'])
        return index


class IndexManager:
    """Manages all indexes for a database"""
    
    def __init__(self, data_dir: str = "data"):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.indexes: Dict[str, Dict[str, Index]] = {}  # table_name -> {column_name -> Index}
        self.lock = Lock()
        
        # Load existing indexes
        self._load_indexes()
    
    def _get_index_file(self, table_name: str) -> Path:
        """Get the file path for table indexes"""
        return self.data_dir / f"{table_name}_indexes.json"
    
    def _load_indexes(self):
        """Load all indexes from disk"""
        for index_file in self.data_dir.glob("*_indexes.json"):
            try:
                with open(index_file, 'r') as f:
                    data = json.load(f)
                    table_name = data['table_name']
                    
                    if table_name not in self.indexes:
                        self.indexes[table_name] = {}
                    
                    for idx_data in data['indexes']:
                        index = Index.from_dict(idx_data)
                        self.indexes[table_name][index.column_name] = index
            except Exception as e:
                print(f"Warning: Could not load index file {index_file}: {e}")
    
    def _save_indexes(self, table_name: str):
        """Save indexes for a table to disk"""
        if table_name not in self.indexes or not self.indexes[table_name]:
            return
        
        index_file = self._get_index_file(table_name)
        
        data = {
            'table_name': table_name,
            'indexes': [idx.to_dict() for idx in self.indexes[table_name].values()]
        }
        
        with open(index_file, 'w') as f:
            json.dump(data, f, indent=2)
    
    def create_index(self, table_name: str, column_name: str, unique: bool = False) -> Index:
        """Create a new index"""
        with self.lock:
            if table_name not in self.indexes:
                self.indexes[table_name] = {}
            
            if column_name in self.indexes[table_name]:
                raise ValueError(f"Index on {table_name}.{column_name} already exists")
            
            index = Index(table_name, column_name, unique)
            self.indexes[table_name][column_name] = index
            self._save_indexes(table_name)
            
            return index
    
    def get_index(self, table_name: str, column_name: str) -> Optional[Index]:
        """Get an index if it exists"""
        return self.indexes.get(table_name, {}).get(column_name)
    
    def has_index(self, table_name: str, column_name: str) -> bool:
        """Check if an index exists"""
        return self.get_index(table_name, column_name) is not None
    
    def drop_index(self, table_name: str, column_name: str):
        """Drop an index"""
        with self.lock:
            if table_name in self.indexes and column_name in self.indexes[table_name]:
                del self.indexes[table_name][column_name]
                
                if not self.indexes[table_name]:
                    del self.indexes[table_name]
                    # Delete index file
                    index_file = self._get_index_file(table_name)
                    if index_file.exists():
                        index_file.unlink()
                else:
                    self._save_indexes(table_name)
    
    def list_indexes(self, table_name: str) -> List[str]:
        """List all indexes for a table"""
        return list(self.indexes.get(table_name, {}).keys())
    
    def rebuild_index(self, table_name: str, column_name: str, rows: List[Dict]) -> bool:
        """
        Rebuild an index from scratch using provided rows
        Useful when creating index on existing data
        """
        index = self.get_index(table_name, column_name)
        if not index:
            return False
        
        with self.lock:
            # Clear existing index
            index.root = BTreeNode()
            
            # Rebuild from rows
            for row in rows:
                row_id = row.get('row_id')
                value = row.get('data', {}).get(column_name)
                
                if value is not None and row_id is not None:
                    success, error = index.insert(value, row_id)
                    if not success:
                        print(f"Warning: Could not index row {row_id}: {error}")
            
            self._save_indexes(table_name)
            return True
    
    def insert_into_indexes(self, table_name: str, row_id: int, 
                           row_data: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """Insert a row into all relevant indexes"""
        if table_name not in self.indexes:
            return True, None
        
        # Track which indexes we've updated for rollback
        updated_indexes = []
        
        try:
            for column_name, index in self.indexes[table_name].items():
                value = row_data.get(column_name)
                if value is not None:
                    success, error = index.insert(value, row_id)
                    if not success:
                        # Rollback previous inserts
                        for col, val in updated_indexes:
                            idx = self.indexes[table_name][col]
                            idx.delete(val, row_id)
                        return False, error
                    updated_indexes.append((column_name, value))
            
            # Persist changes
            self._save_indexes(table_name)
            return True, None
            
        except Exception as e:
            # Rollback on exception
            for col, val in updated_indexes:
                idx = self.indexes[table_name][col]
                idx.delete(val, row_id)
            return False, str(e)
    
    def delete_from_indexes(self, table_name: str, row_id: int, row_data: Dict[str, Any]):
        """Delete a row from all relevant indexes"""
        if table_name not in self.indexes:
            return
        
        for column_name, index in self.indexes[table_name].items():
            value = row_data.get(column_name)
            if value is not None:
                index.delete(value, row_id)
        
        self._save_indexes(table_name)
    
    def update_indexes(self, table_name: str, row_id: int, 
                      old_data: Dict[str, Any], 
                      new_data: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """Update indexes when a row is modified"""
        if table_name not in self.indexes:
            return True, None
        
        updated_indexes = []
        
        try:
            for column_name, index in self.indexes[table_name].items():
                old_value = old_data.get(column_name)
                new_value = new_data.get(column_name)
                
                # Only update if value changed
                if old_value != new_value:
                    if new_value is not None:
                        success, error = index.update(old_value, new_value, row_id)
                        if not success:
                            # Rollback
                            for col, old_val, new_val in updated_indexes:
                                idx = self.indexes[table_name][col]
                                idx.update(new_val, old_val, row_id)
                            return False, error
                        updated_indexes.append((column_name, old_value, new_value))
            
            self._save_indexes(table_name)
            return True, None
            
        except Exception as e:
            # Rollback
            for col, old_val, new_val in updated_indexes:
                idx = self.indexes[table_name][col]
                idx.update(new_val, old_val, row_id)
            return False, str(e)
    
    def get_all_stats(self) -> Dict[str, List[Dict]]:
        """Get statistics for all indexes"""
        stats = {}
        for table_name, table_indexes in self.indexes.items():
            stats[table_name] = [idx.get_stats() for idx in table_indexes.values()]
        return stats


if __name__ == "__main__":
    print("Testing SimplDB Index Manager\n")
    print("=" * 60)
    
    # Test 1: Create indexes
    print("\n1. Creating indexes...")
    manager = IndexManager("demo_data")
    
    users_id_idx = manager.create_index("users", "id", unique=True)
    users_email_idx = manager.create_index("users", "email", unique=True)
    users_age_idx = manager.create_index("users", "age", unique=False)
    
    print(f"   Created indexes: {manager.list_indexes('users')}")
    
    # Test 2: Insert data into indexes
    print("\n2. Inserting data into indexes...")
    test_users = [
        (1, {"id": 1, "email": "alice@example.com", "age": 30}),
        (2, {"id": 2, "email": "bob@example.com", "age": 25}),
        (3, {"id": 3, "email": "charlie@example.com", "age": 30}),
        (4, {"id": 4, "email": "dave@example.com", "age": 35})
    ]
    
    for row_id, data in test_users:
        success, error = manager.insert_into_indexes("users", row_id, data)
        if success:
            print(f"   ✓ Inserted row {row_id}")
        else:
            print(f"   ✗ Failed: {error}")
    
    # Test 3: Search by exact value
    print("\n3. Searching by exact value...")
    email_idx = manager.get_index("users", "email")
    results = email_idx.search("alice@example.com")
    print(f"   Email 'alice@example.com': Row IDs = {results}")
    
    # Test 4: Range search
    print("\n4. Range search on age...")
    age_idx = manager.get_index("users", "age")
    results = age_idx.range_search(25, 30)
    print(f"   Ages 25-30: Row IDs = {results}")
    
    # Test 5: Test unique constraint
    print("\n5. Testing unique constraint...")
    success, error = manager.insert_into_indexes("users", 5, 
                                                  {"id": 5, "email": "alice@example.com", "age": 28})
    print(f"   Duplicate email insert: Success={success}, Error={error}")
    
    # Test 6: Update index
    print("\n6. Updating indexed value...")
    old_data = {"id": 1, "email": "alice@example.com", "age": 30}
    new_data = {"id": 1, "email": "alice.new@example.com", "age": 31}
    
    success, error = manager.update_indexes("users", 1, old_data, new_data)
    print(f"   Update success: {success}")
    
    results = email_idx.search("alice.new@example.com")
    print(f"   New email search: Row IDs = {results}")
    
    # Test 7: Delete from index
    print("\n7. Deleting from indexes...")
    manager.delete_from_indexes("users", 2, {"id": 2, "email": "bob@example.com", "age": 25})
    results = email_idx.search("bob@example.com")
    print(f"   Deleted Bob, search result: {results}")
    
    # Test 8: Index statistics
    print("\n8. Index statistics...")
    stats = manager.get_all_stats()
    for table, table_stats in stats.items():
        print(f"   Table: {table}")
        for stat in table_stats:
            print(f"      {stat}")
    
    print("\n" + "=" * 60)
    print("✅ Index manager test complete!")
    print("Check 'demo_data/' for index files (*_indexes.json)")