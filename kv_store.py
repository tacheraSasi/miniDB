import shelve

class KVStore:
    def __init__(self, db_name):
        """Constructor for the KVStore class."""
        self.db_name = db_name
        self.db = shelve.open(db_name)
    
    def get(self, key):
        """Get a value by key."""
        return self.db.get(key, f"Key {key} not found in {self.db_name}")

    def set(self, key, value):
        """Set a value by key."""
        self.db[key] = value
        self.db.sync()
        
    def keys(self):
        """Return all keys."""
        return list(self.db.keys())
    
    def values(self):
        """Return all values."""
        return list(self.db.values())

    def items(self):
        """Return all key-value pairs."""
        return list(self.db.items())
    
    def clear(self):
        """Erase everything from the database."""
        self.db.clear()
        self.db.sync() 
        

    def delete(self, key):
        """Delete a key-value pair by key."""
        if key in self.db:
            del self.db[key]
            self.db.sync()

    def close(self):
        """Close the database."""
        self.db.close()
