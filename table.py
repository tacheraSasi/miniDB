import json
import uuid
from kv_store import KVStore

class Row:
    def __init__(self, **columns):
        """Initialize a row with given columns."""
        self.columns = columns

class Table:
    def __init__(self, name, kv_store):
        """Initialize a table with a name and KV store."""
        self.name = name
        self.kv_store = kv_store

    def insert(self, row):
        """Insert a row into the table."""
        key = str(uuid.uuid4())  # Generate a unique key for the row
        self.kv_store.set(f"{self.name}:{key}", json.dumps(row.columns))
        return key

    def query(self, key):
        """Query a row by key."""
        data = self.kv_store.get(f"{self.name}:{key}")
        if data.startswith("Key "):  # Handle case where key is not found
            return None
        return Row(**json.loads(data))

    def delete(self, key):
        """Delete a row by key."""
        self.kv_store.delete(f"{self.name}:{key}")

    def list_rows(self):
        """List all rows in the table."""
        rows = []
        prefix = f"{self.name}:"
        for key in self.kv_store.keys():
            if key.startswith(prefix):
                data = self.kv_store.get(key)
                if not data.startswith("Key "):
                    rows.append(Row(**json.loads(data)))
        return rows

    def update(self, key, **updates):
        """Update a row by key."""
        row = self.query(key)
        if row:
            row.columns.update(updates)
            self.kv_store.set(f"{self.name}:{key}", json.dumps(row.columns))

    def count_rows(self):
        """Count the number of rows in the table."""
        return len(self.list_rows())
