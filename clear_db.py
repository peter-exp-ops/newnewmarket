import sqlite3
import os

def clear_database():
    """Delete all records from all tables in the racing_data.db database"""
    # Check if the database file exists
    if not os.path.exists('racing_data.db'):
        print("Database file 'racing_data.db' not found.")
        return
    
    # Connect to the database
    conn = sqlite3.connect('racing_data.db')
    cursor = conn.cursor()
    
    # Get all table names
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    
    print(f"Found {len(tables)} tables in the database.")
    
    # Start a transaction
    conn.execute("BEGIN TRANSACTION")
    
    try:
        # Delete data from each table
        for table in tables:
            table_name = table[0]
            if table_name != 'sqlite_sequence':  # Don't delete from sqlite_sequence directly
                print(f"Deleting all records from {table_name}...")
                cursor.execute(f"DELETE FROM {table_name}")
        
        # Reset all autoincrement sequences
        print("Resetting autoincrement sequences...")
        cursor.execute("DELETE FROM sqlite_sequence")
        
        # Commit the transaction
        conn.commit()
        print("All records have been deleted successfully.")
        
    except sqlite3.Error as e:
        # If there's an error, roll back the transaction
        conn.rollback()
        print(f"Error during deletion: {e}")
    
    # Verify tables are empty
    for table in tables:
        table_name = table[0]
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        print(f"Table '{table_name}' now has {count} records.")
    
    # Close the connection
    conn.close()

if __name__ == "__main__":
    print("Database Record Deletion Tool")
    print("============================")
    clear_database()
    print("Operation completed.") 