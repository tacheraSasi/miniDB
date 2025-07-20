class MiniDBError(Exception):
    """Base exception class for MiniDB."""
    pass

class TableNotFoundError(MiniDBError):
    """Raised when attempting to access a non-existent table."""
    pass

class SchemaValidationError(MiniDBError):
    """Raised when data doesn't match the defined schema."""
    pass

class TransactionError(MiniDBError):
    """Raised when a transaction operation fails."""
    pass

class ConnectionError(MiniDBError):
    """Raised when database connection issues occur."""
    pass