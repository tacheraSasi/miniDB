import threading
from typing import Dict, Any, Optional, List
from contextlib import contextmanager
from exceptions import TransactionError

class Transaction:
    def __init__(self, kv_store):
        self.kv_store = kv_store
        self.changes: Dict[str, Any] = {}
        self.deletions: List[str] = []
        self.committed = False
        self.rolled_back = False

    def set(self, key: str, value: Any) -> None:
        """Stage a set operation."""
        if self.committed or self.rolled_back:
            raise TransactionError("Transaction already completed")
        self.changes[key] = value

    def delete(self, key: str) -> None:
        """Stage a delete operation."""
        if self.committed or self.rolled_back:
            raise TransactionError("Transaction already completed")
        if key in self.changes:
            del self.changes[key]
        self.deletions.append(key)

    def get(self, key: str) -> Any:
        """Get a value, considering staged changes."""
        if key in self.changes:
            return self.changes[key]
        if key in self.deletions:
            return None
        return self.kv_store.get(key)

    def commit(self) -> None:
        """Commit all staged changes."""
        if self.committed or self.rolled_back:
            raise TransactionError("Transaction already completed")

        try:
            # Apply all changes
            for key, value in self.changes.items():
                self.kv_store.set(key, value)

            # Apply all deletions
            for key in self.deletions:
                self.kv_store.delete(key)

            self.committed = True
        except Exception as e:
            self.rollback()
            raise TransactionError(f"Failed to commit transaction: {str(e)}")

    def rollback(self) -> None:
        """Roll back all staged changes."""
        if self.committed:
            raise TransactionError("Transaction already committed")
        self.changes.clear()
        self.deletions.clear()
        self.rolled_back = True

class TransactionManager:
    def __init__(self):
        self._local = threading.local()

    @property
    def current_transaction(self) -> Optional[Transaction]:
        return getattr(self._local, 'transaction', None)

    @contextmanager
    def transaction(self, kv_store):
        """Create a new transaction context."""
        if self.current_transaction is not None:
            raise TransactionError("Transaction already in progress")

        transaction = Transaction(kv_store)
        self._local.transaction = transaction

        try:
            yield transaction
            if not transaction.committed and not transaction.rolled_back:
                transaction.commit()
        except Exception as e:
            if not transaction.rolled_back:
                transaction.rollback()
            raise e
        finally:
            self._local.transaction = None