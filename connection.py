import queue
import threading
import logging
from typing import Optional, List
from contextlib import contextmanager
from exceptions import ConnectionError
from kv_store import KVStore

logger = logging.getLogger(__name__)

class Connection:
    def __init__(self, db_name: str):
        self.db_name = db_name
        self.kv_store = KVStore(db_name)
        self.in_use = False
        self.last_used = 0

    def close(self):
        """Close the database connection."""
        try:
            self.kv_store.close()
        except Exception as e:
            logger.error(f"Error closing connection: {e}")
            raise ConnectionError(f"Failed to close connection: {str(e)}")

class ConnectionPool:
    def __init__(self, db_name: str, max_connections: int = 10, timeout: float = 30.0):
        self.db_name = db_name
        self.max_connections = max_connections
        self.timeout = timeout
        self.pool: queue.Queue = queue.Queue(maxsize=max_connections)
        self.connections: List[Connection] = []
        self._lock = threading.Lock()
        self.closed = False

        # Initialize logging
        logging.basicConfig(level=logging.INFO)

    def _create_connection(self) -> Connection:
        """Create a new database connection."""
        try:
            connection = Connection(self.db_name)
            logger.info(f"Created new connection for {self.db_name}")
            return connection
        except Exception as e:
            logger.error(f"Failed to create connection: {e}")
            raise ConnectionError(f"Failed to create connection: {str(e)}")

    @contextmanager
    def get_connection(self) -> Connection:
        """Get a connection from the pool."""
        if self.closed:
            raise ConnectionError("Connection pool is closed")

        connection = None
        try:
            # Try to get a connection from the pool
            try:
                connection = self.pool.get(timeout=self.timeout)
                logger.debug(f"Acquired connection from pool for {self.db_name}")
            except queue.Empty:
                # If pool is empty, try to create a new connection
                with self._lock:
                    if len(self.connections) < self.max_connections:
                        connection = self._create_connection()
                        self.connections.append(connection)
                    else:
                        raise ConnectionError("Maximum connections reached")

            connection.in_use = True
            yield connection

        finally:
            if connection is not None:
                connection.in_use = False
                try:
                    self.pool.put(connection, timeout=self.timeout)
                    logger.debug(f"Released connection back to pool for {self.db_name}")
                except queue.Full:
                    logger.warning("Connection pool full when returning connection")

    def close(self):
        """Close all connections in the pool."""
        if self.closed:
            return

        self.closed = True
        logger.info(f"Closing connection pool for {self.db_name}")

        while not self.pool.empty():
            try:
                connection = self.pool.get_nowait()
                connection.close()
            except queue.Empty:
                break
            except Exception as e:
                logger.error(f"Error closing connection: {e}")

        for connection in self.connections:
            try:
                if connection.in_use:
                    logger.warning(f"Closing in-use connection for {self.db_name}")
                connection.close()
            except Exception as e:
                logger.error(f"Error closing connection: {e}")