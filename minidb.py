from table import Table
from kv_store import KVStore

class MiniDB:
    def __init__(self, db_name):
        """Initialize MiniDB with a KV store."""
        self.kv_store = KVStore(db_name)
        self.tables = {}
   
    def create_table(self, table_name):
        """Create a new table."""
        if table_name not in self.tables:
            self.tables[table_name] = Table(table_name, self.kv_store)

    def get_table(self, table_name):
        """Get a table by name."""
        return self.tables.get(table_name)

    def list_all_tables(self):
        """List all tables in the database."""
        return list(self.tables.keys())

    def close(self):
        """Close the database."""
        self.kv_store.close()
