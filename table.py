import json
import uuid
import logging
from typing import Dict, Any, List, Optional, Set
from kv_store import KVStore
from schema import Schema
from indexing import IndexManager
from exceptions import SchemaValidationError

logger = logging.getLogger(__name__)

class Row:
    def __init__(self, **columns):
        """Initialize a row with given columns."""
        self.columns = columns

    def to_dict(self) -> Dict[str, Any]:
        """Convert row to dictionary."""
        return self.columns

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Row':
        """Create a row from dictionary."""
        return cls(**data)

class Table:
    def __init__(self, name: str, kv_store: KVStore, schema: Optional[Schema] = None):
        """Initialize a table with a name, KV store, and optional schema."""
        self.name = name
        self.kv_store = kv_store
        self.schema = schema
        self.index_manager = IndexManager(kv_store)
        logger.info(f"Initialized table: {name}")

    def _validate_row(self, row: Row) -> Dict[str, Any]:
        """Validate row data against schema."""
        if self.schema is None:
            return row.columns
        try:
            return self.schema.validate(row.columns)
        except SchemaValidationError as e:
            logger.error(f"Schema validation error in table {self.name}: {e}")
            raise

    def create_index(self, field_name: str) -> None:
        """Create an index for a field."""
        try:
            self.index_manager.create_index(self.name, field_name)
            logger.info(f"Created index on {self.name}.{field_name}")
        except Exception as e:
            logger.error(f"Error creating index on {self.name}.{field_name}: {e}")
            raise

    def insert(self, row: Row) -> str:
        """Insert a row into the table."""
        try:
            # Validate row against schema
            validated_data = self._validate_row(row)
            
            # Generate unique key and store row
            key = str(uuid.uuid4())
            full_key = f"{self.name}:{key}"
            self.kv_store.set(full_key, json.dumps(validated_data))
            
            # Update indexes
            for field_name in validated_data:
                index = self.index_manager.get_index(self.name, field_name)
                if index:
                    index.add_entry(key, validated_data[field_name])
            
            logger.debug(f"Inserted row with key {key} in table {self.name}")
            return key
        except Exception as e:
            logger.error(f"Error inserting row in table {self.name}: {e}")
            raise

    def query(self, key: str) -> Optional[Row]:
        """Query a row by key."""
        try:
            data = self.kv_store.get(f"{self.name}:{key}")
            if data.startswith("Key "):
                return None
            return Row.from_dict(json.loads(data))
        except Exception as e:
            logger.error(f"Error querying key {key} in table {self.name}: {e}")
            raise

    def query_by_field(self, field_name: str, value: Any) -> List[Row]:
        """Query rows by field value using index."""
        try:
            index = self.index_manager.get_index(self.name, field_name)
            if not index:
                logger.warning(f"No index found for {field_name}, performing full table scan")
                return [row for row in self.list_rows() 
                        if row.columns.get(field_name) == value]
            
            matching_keys = index.find_rows(value)
            return [self.query(key) for key in matching_keys if key is not None]
        except Exception as e:
            logger.error(f"Error querying by field {field_name} in table {self.name}: {e}")
            raise

    def update(self, key: str, **updates) -> bool:
        """Update a row by key."""
        try:
            row = self.query(key)
            if not row:
                return False

            # Update row data
            new_data = row.columns.copy()
            new_data.update(updates)
            
            # Validate updated data
            validated_data = self._validate_row(Row(**new_data))
            
            # Update indexes
            for field_name, new_value in updates.items():
                index = self.index_manager.get_index(self.name, field_name)
                if index:
                    index.update_entry(key, row.columns.get(field_name), new_value)
            
            # Save updated row
            self.kv_store.set(f"{self.name}:{key}", json.dumps(validated_data))
            logger.debug(f"Updated row {key} in table {self.name}")
            return True
        except Exception as e:
            logger.error(f"Error updating row {key} in table {self.name}: {e}")
            raise

    def delete(self, key: str) -> bool:
        """Delete a row by key."""
        try:
            row = self.query(key)
            if not row:
                return False

            # Update indexes
            for field_name, value in row.columns.items():
                index = self.index_manager.get_index(self.name, field_name)
                if index:
                    index.remove_entry(key, value)

            # Delete row
            self.kv_store.delete(f"{self.name}:{key}")
            logger.debug(f"Deleted row {key} from table {self.name}")
            return True
        except Exception as e:
            logger.error(f"Error deleting row {key} from table {self.name}: {e}")
            raise

    def list_rows(self) -> List[Row]:
        """List all rows in the table."""
        try:
            rows = []
            prefix = f"{self.name}:"
            for key in self.kv_store.keys():
                if key.startswith(prefix):
                    data = self.kv_store.get(key)
                    if not data.startswith("Key "):
                        rows.append(Row.from_dict(json.loads(data)))
            return rows
        except Exception as e:
            logger.error(f"Error listing rows in table {self.name}: {e}")
            raise

    def count_rows(self) -> int:
        """Count the number of rows in the table."""
        return len(self.list_rows())
