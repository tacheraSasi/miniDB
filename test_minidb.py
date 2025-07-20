import pytest
from minidb import MiniDB
from schema import Schema, StringField, IntegerField, DateTimeField
from exceptions import TableNotFoundError, SchemaValidationError
from table import Row

@pytest.fixture
def db():
    """Create a test database instance."""
    db = MiniDB('test_db')
    yield db
    db.close()

def test_create_table(db):
    """Test table creation."""
    table = db.create_table('test_table')
    assert table is not None
    assert 'test_table' in db.list_tables()

def test_schema_validation():
    """Test schema validation."""
    db = MiniDB('test_schema_db')
    
    # Create schema
    user_schema = Schema(
        name=StringField(required=True, max_length=50),
        age=IntegerField(required=True, min_value=0, max_value=150),
        created_at=DateTimeField(auto_now_add=True)
    )
    
    # Create table with schema
    users = db.create_table('users', schema=user_schema)
    
    # Valid data
    valid_user = Row(name='John Doe', age=30)
    key = users.insert(valid_user)
    assert key is not None
    
    # Invalid data - missing required field
    with pytest.raises(SchemaValidationError):
        users.insert(Row(name='Invalid'))
    
    # Invalid data - age out of range
    with pytest.raises(SchemaValidationError):
        users.insert(Row(name='Invalid', age=200))
    
    db.close()

def test_indexing(db):
    """Test indexing functionality."""
    # Create table and index
    users = db.create_table('indexed_users')
    db.create_index('indexed_users', 'name')
    
    # Insert test data
    user1 = Row(name='Alice', age=25)
    user2 = Row(name='Bob', age=30)
    users.insert(user1)
    users.insert(user2)
    
    # Query by index
    results = db.query_by_index('indexed_users', 'name', 'Alice')
    assert len(results) == 1
    assert results[0].columns['name'] == 'Alice'

def test_transactions(db):
    """Test transaction functionality."""
    users = db.create_table('transaction_test')
    
    # Successful transaction
    with db.begin_transaction() as transaction:
        users.insert(Row(name='Transaction1', age=25))
        users.insert(Row(name='Transaction2', age=30))
    
    # Verify data was committed
    all_users = users.list_rows()
    assert len(all_users) == 2
    
    # Failed transaction
    try:
        with db.begin_transaction() as transaction:
            users.insert(Row(name='Transaction3', age=35))
            raise Exception('Rollback test')
    except:
        pass
    
    # Verify data was rolled back
    all_users = users.list_rows()
    assert len(all_users) == 2  # Still 2 users

def test_crud_operations(db):
    """Test basic CRUD operations."""
    users = db.create_table('crud_test')
    
    # Create
    user = Row(name='Test User', age=25)
    key = users.insert(user)
    assert key is not None
    
    # Read
    retrieved_user = users.query(key)
    assert retrieved_user is not None
    assert retrieved_user.columns['name'] == 'Test User'
    
    # Update
    users.update(key, age=26)
    updated_user = users.query(key)
    assert updated_user.columns['age'] == 26
    
    # Delete
    users.delete(key)
    deleted_user = users.query(key)
    assert deleted_user is None

def test_error_handling(db):
    """Test error handling."""
    # Test non-existent table
    with pytest.raises(TableNotFoundError):
        db.get_table('non_existent')
    
    # Test invalid schema
    schema = Schema(age=IntegerField(required=True, min_value=0))
    users = db.create_table('validation_test', schema=schema)
    
    with pytest.raises(SchemaValidationError):
        users.insert(Row(age=-1))  # Invalid age

def test_connection_pool(db):
    """Test connection pooling."""
    # Test multiple connections
    with db.connection_pool.get_connection() as conn1:
        with db.connection_pool.get_connection() as conn2:
            assert conn1 is not conn2

if __name__ == '__main__':
    pytest.main([__file__])