import shelve
import logging
from typing import Any, Optional, List, Dict, Tuple
from exceptions import ConnectionError

logger = logging.getLogger(__name__)

class KVStore:
    def __init__(self, db_name: str):
        """Constructor for the KVStore class."""
        self.db_name = db_name
        try:
            self.db = shelve.open(db_name)
            logger.info(f"Opened KV store: {db_name}")
        except Exception as e:
            logger.error(f"Failed to open KV store {db_name}: {e}")
            raise ConnectionError(f"Failed to open database: {str(e)}")
    
    def get(self, key: str) -> Any:
        """Get a value by key."""
        try:
            return self.db.get(key, f"Key {key} not found in {self.db_name}")
        except Exception as e:
            logger.error(f"Error getting key {key}: {e}")
            raise

    def get_many(self, keys: List[str]) -> Dict[str, Any]:
        """Get multiple values by keys."""
        return {key: self.get(key) for key in keys}

    def set(self, key: str, value: Any) -> None:
        """Set a value by key."""
        try:
            self.db[key] = value
            self.db.sync()
            logger.debug(f"Set key {key} in {self.db_name}")
        except Exception as e:
            logger.error(f"Error setting key {key}: {e}")
            raise
        
    def set_many(self, items: Dict[str, Any]) -> None:
        """Set multiple key-value pairs."""
        try:
            for key, value in items.items():
                self.db[key] = value
            self.db.sync()
            logger.debug(f"Set {len(items)} items in {self.db_name}")
        except Exception as e:
            logger.error(f"Error in bulk set operation: {e}")
            raise

    def keys(self) -> List[str]:
        """Return all keys."""
        try:
            return list(self.db.keys())
        except Exception as e:
            logger.error(f"Error getting keys: {e}")
            raise
    
    def values(self) -> List[Any]:
        """Return all values."""
        try:
            return list(self.db.values())
        except Exception as e:
            logger.error(f"Error getting values: {e}")
            raise

    def items(self) -> List[Tuple[str, Any]]:
        """Return all key-value pairs."""
        try:
            return list(self.db.items())
        except Exception as e:
            logger.error(f"Error getting items: {e}")
            raise
    
    def clear(self) -> None:
        """Erase everything from the database."""
        try:
            self.db.clear()
            self.db.sync()
            logger.info(f"Cleared all data from {self.db_name}")
        except Exception as e:
            logger.error(f"Error clearing database: {e}")
            raise

    def delete(self, key: str) -> None:
        """Delete a key-value pair by key."""
        try:
            if key in self.db:
                del self.db[key]
                self.db.sync()
                logger.debug(f"Deleted key {key} from {self.db_name}")
        except Exception as e:
            logger.error(f"Error deleting key {key}: {e}")
            raise

    def delete_many(self, keys: List[str]) -> None:
        """Delete multiple keys."""
        try:
            for key in keys:
                if key in self.db:
                    del self.db[key]
            self.db.sync()
            logger.debug(f"Deleted {len(keys)} keys from {self.db_name}")
        except Exception as e:
            logger.error(f"Error in bulk delete operation: {e}")
            raise

    def close(self) -> None:
        """Close the database."""
        try:
            self.db.close()
            logger.info(f"Closed KV store: {self.db_name}")
        except Exception as e:
            logger.error(f"Error closing database: {e}")
            raise
