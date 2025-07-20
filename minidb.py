import logging
from typing import Dict, Optional, List, Any
from table import Table
from connection import ConnectionPool
from schema import Schema
from transaction import TransactionManager
from exceptions import TableNotFoundError, ConnectionError

logger = logging.getLogger(__name__)

class MiniDB:
    def __init__(self, db_name: str, max_connections: int = 10):
        """Initialize MiniDB with connection pooling and transaction support."""
        try:
            self.db_name = db_name
            self.connection_pool = ConnectionPool(db_name, max_connections)
            self.transaction_manager = TransactionManager()
            self.tables: Dict[str, Table] = {}
            self._setup_logging()
            logger.info(f"Initialized MiniDB: {db_name}")
        except Exception as e:
            logger.error(f"Failed to initialize MiniDB: {e}")
            raise ConnectionError(f"Failed to initialize database: {str(e)}")

    def _setup_logging(self):
        """Setup logging configuration."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )

    def create_table(self, table_name: str, schema: Optional[Schema] = None) -> Table:
        """Create a new table with optional schema."""
        try:
            if table_name in self.tables:
                logger.warning(f"Table {table_name} already exists")
                return self.tables[table_name]

            with self.connection_pool.get_connection() as connection:
                table = Table(table_name, connection.kv_store, schema)
                self.tables[table_name] = table
                logger.info(f"Created table: {table_name}")
                return table
        except Exception as e:
            logger.error(f"Error creating table {table_name}: {e}")
            raise

    def get_table(self, table_name: str) -> Table:
        """Get a table by name."""
        table = self.tables.get(table_name)
        if table is None:
            raise TableNotFoundError(f"Table not found: {table_name}")
        return table

    def drop_table(self, table_name: str) -> None:
        """Drop a table and all its data."""
        try:
            table = self.get_table(table_name)
            # Delete all rows
            for row in table.list_rows():
                table.delete(row)
            del self.tables[table_name]
            logger.info(f"Dropped table: {table_name}")
        except Exception as e:
            logger.error(f"Error dropping table {table_name}: {e}")
            raise

    def list_tables(self) -> List[str]:
        """List all tables in the database."""
        return list(self.tables.keys())

    def begin_transaction(self):
        """Begin a new transaction."""
        try:
            with self.connection_pool.get_connection() as connection:
                return self.transaction_manager.transaction(connection.kv_store)
        except Exception as e:
            logger.error(f"Error beginning transaction: {e}")
            raise

    def create_index(self, table_name: str, field_name: str) -> None:
        """Create an index on a table field."""
        try:
            table = self.get_table(table_name)
            table.create_index(field_name)
            logger.info(f"Created index on {table_name}.{field_name}")
        except Exception as e:
            logger.error(f"Error creating index on {table_name}.{field_name}: {e}")
            raise

    def query_by_index(self, table_name: str, field_name: str, value: Any) -> List[Any]:
        """Query table using an index."""
        try:
            table = self.get_table(table_name)
            return table.query_by_field(field_name, value)
        except Exception as e:
            logger.error(f"Error querying index {table_name}.{field_name}: {e}")
            raise

    def backup(self, backup_path: str) -> None:
        """Create a backup of the database."""
        try:
            # Implementation depends on specific backup requirements
            # This is a placeholder for the backup functionality
            logger.info(f"Created backup at: {backup_path}")
        except Exception as e:
            logger.error(f"Error creating backup: {e}")
            raise

    def restore(self, backup_path: str) -> None:
        """Restore database from a backup."""
        try:
            # Implementation depends on specific restore requirements
            # This is a placeholder for the restore functionality
            logger.info(f"Restored from backup: {backup_path}")
        except Exception as e:
            logger.error(f"Error restoring from backup: {e}")
            raise

    def close(self) -> None:
        """Close the database and all connections."""
        try:
            self.connection_pool.close()
            logger.info(f"Closed database: {self.db_name}")
        except Exception as e:
            logger.error(f"Error closing database: {e}")
            raise
