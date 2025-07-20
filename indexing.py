from typing import Dict, Any, List, Set, Optional
from collections import defaultdict
import json

class Index:
    def __init__(self, table_name: str, field_name: str, kv_store):
        self.table_name = table_name
        self.field_name = field_name
        self.kv_store = kv_store
        self.index_key = f"idx:{table_name}:{field_name}"
        self._load_index()

    def _load_index(self):
        """Load or initialize the index from storage."""
        index_data = self.kv_store.get(self.index_key)
        if isinstance(index_data, str) and index_data.startswith("Key "):
            self.index: Dict[Any, Set[str]] = defaultdict(set)
        else:
            self.index = defaultdict(set, json.loads(index_data))
            # Convert list values back to sets
            for key in self.index:
                self.index[key] = set(self.index[key])

    def _save_index(self):
        """Save the index to storage."""
        # Convert sets to lists for JSON serialization
        serializable_index = {k: list(v) for k, v in self.index.items()}
        self.kv_store.set(self.index_key, json.dumps(serializable_index))

    def add_entry(self, row_key: str, value: Any):
        """Add an entry to the index."""
        if value is not None:  # Don't index None values
            # Convert value to string for consistent storage
            str_value = str(value)
            self.index[str_value].add(row_key)
            self._save_index()

    def remove_entry(self, row_key: str, value: Any):
        """Remove an entry from the index."""
        if value is not None:
            str_value = str(value)
            if str_value in self.index:
                self.index[str_value].discard(row_key)
                if not self.index[str_value]:  # Remove empty sets
                    del self.index[str_value]
                self._save_index()

    def update_entry(self, row_key: str, old_value: Any, new_value: Any):
        """Update an indexed entry."""
        self.remove_entry(row_key, old_value)
        self.add_entry(row_key, new_value)

    def find_rows(self, value: Any) -> Set[str]:
        """Find all row keys that match the given value."""
        str_value = str(value)
        return self.index.get(str_value, set())

    def find_range(self, start_value: Any, end_value: Any) -> Set[str]:
        """Find all row keys within a range of values."""
        result = set()
        str_start = str(start_value)
        str_end = str(end_value)
        
        for value in self.index:
            if str_start <= value <= str_end:
                result.update(self.index[value])
        
        return result

class IndexManager:
    def __init__(self, kv_store):
        self.kv_store = kv_store
        self.indexes: Dict[str, Index] = {}

    def create_index(self, table_name: str, field_name: str) -> Index:
        """Create a new index for a table field."""
        index_key = f"{table_name}:{field_name}"
        if index_key not in self.indexes:
            self.indexes[index_key] = Index(table_name, field_name, self.kv_store)
        return self.indexes[index_key]

    def get_index(self, table_name: str, field_name: str) -> Optional[Index]:
        """Get an existing index."""
        index_key = f"{table_name}:{field_name}"
        return self.indexes.get(index_key)

    def drop_index(self, table_name: str, field_name: str):
        """Drop an existing index."""
        index_key = f"{table_name}:{field_name}"
        if index_key in self.indexes:
            # Remove the index data from storage
            self.kv_store.delete(f"idx:{table_name}:{field_name}")
            del self.indexes[index_key]