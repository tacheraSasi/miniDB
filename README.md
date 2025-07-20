# MiniDB - A Production-Ready Key-Value Database

MiniDB is a lightweight, production-ready key-value database implemented in Python. It provides features commonly found in production databases while maintaining simplicity and ease of use.

## Features

- **Schema Validation**: Define and enforce data schemas for tables
- **Indexing**: Create indexes for faster query performance
- **Transaction Support**: ACID-compliant transactions
- **Connection Pooling**: Efficient connection management
- **Error Handling**: Comprehensive error handling and custom exceptions
- **Logging**: Detailed logging for monitoring and debugging
- **Type Safety**: Full type annotations for better code reliability
- **Backup & Restore**: Database backup and restore capabilities

## Installation

```bash
pip install -r requirements.txt
```

## Usage

### Basic Usage

```python
from minidb import MiniDB
from schema import Schema, StringField, IntegerField

# Initialize database
db = MiniDB('mydb')

# Create schema
user_schema = Schema(
    name=StringField(required=True, max_length=100),
    age=IntegerField(required=True, min_value=0)
)

# Create table with schema
users = db.create_table('users', schema=user_schema)

# Insert data
user = Row(name='John Doe', age=30)
user_id = users.insert(user)

# Query data
user = users.query(user_id)
print(user.columns)  # {'name': 'John Doe', 'age': 30}
```

### Using Transactions

```python
with db.begin_transaction() as transaction:
    # Perform multiple operations atomically
    users.insert(Row(name='Alice', age=25))
    users.insert(Row(name='Bob', age=35))
    # Transaction automatically commits if no errors occur
```

### Using Indexes

```python
# Create an index on the 'name' field
db.create_index('users', 'name')

# Query using the index
results = db.query_by_index('users', 'name', 'John Doe')
```

### Connection Pooling

```python
# Initialize database with custom connection pool size
db = MiniDB('mydb', max_connections=20)

# Connections are automatically managed
with db.connection_pool.get_connection() as connection:
    # Use connection
    pass
```

## Error Handling

```python
from exceptions import TableNotFoundError, SchemaValidationError

try:
    table = db.get_table('non_existent')
except TableNotFoundError as e:
    print(f"Table not found: {e}")

try:
    users.insert(Row(name='Invalid', age=-1))
except SchemaValidationError as e:
    print(f"Validation error: {e}")
```

## Best Practices

1. **Always use schemas** for data validation
2. **Create indexes** for frequently queried fields
3. **Use transactions** for operations that need to be atomic
4. **Handle exceptions** appropriately
5. **Close the database** when done

```python
try:
    # Use database
    pass
finally:
    db.close()
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request. For major changes, please open an issue first to discuss what you would like to change.

## Author

Tachera Sasi

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.