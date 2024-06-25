from minidb import MiniDB
from table import Row

def main():
    # Initialize MiniDB
    db = MiniDB('minidb.db')
    
    # Create a table
    db.create_table('users')
    users_table = db.get_table('users')

    # Insert some rows
    user1 = Row(id=1, name='Alice', email='alice@example.com')
    user2 = Row(id=2, name='Bob', email='bob@example.com')
    
    key1 = users_table.insert(user1)
    key2 = users_table.insert(user2)

    print(f"Inserted Alice with key: {key1}")
    print(f"Inserted Bob with key: {key2}")

    # Query the rows
    queried_user1 = users_table.query(key1)
    queried_user2 = users_table.query(key2)
    
    print("Queried User 1:", queried_user1.columns if queried_user1 else "User not found")
    print("Queried User 2:", queried_user2.columns if queried_user2 else "User not found")

    # Update a row directly
    users_table.update(key1, name='Alice Smith', email='alice.smith@example.com')
    queried_user1_updated = users_table.query(key1)
    print("Queried Updated User 1:", queried_user1_updated.columns if queried_user1_updated else "User not found")

    # Delete a row
    users_table.delete(key2)
    queried_user2_deleted = users_table.query(key2)
    print("Queried User 2 after delete:", queried_user2_deleted.columns if queried_user2_deleted else "User not found")

    # List all rows in the users table
    all_users = users_table.list_rows()
    print("All Users:", [user.columns for user in all_users])

    # Count rows in the users table
    print("Number of users:", users_table.count_rows())

    # List all tables in the database
    print("All Tables in the Database:", db.list_all_tables())

    # Close the database
    db.close()

if __name__ == "__main__":
    main()
