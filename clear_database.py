import sqlite3
import os

def clear_database():
    """Delete all records from all tables in the racing_data.db database"""
    try:
        # Check if the database file exists
        if not os.path.exists('racing_data.db'):
            print("Database file 'racing_data.db' not found.")
            return False
        
        # Connect to the database
        conn = sqlite3.connect('racing_data.db')
        cursor = conn.cursor()
        
        # Get all table names
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        
        if not tables:
            print("No tables found in the database.")
            conn.close()
            return False
        
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
            cursor.execute("DELETE FROM sqlite_sequence")
            
            # Commit the transaction
            conn.commit()
            print("All records have been deleted successfully.")
            
            # Get table count after deletion
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = cursor.fetchall()
            for table in tables:
                table_name = table[0]
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                count = cursor.fetchone()[0]
                print(f"Table '{table_name}' now has {count} records.")
            
            return True
            
        except sqlite3.Error as e:
            # If there's an error, roll back the transaction
            conn.rollback()
            print(f"Error during deletion: {e}")
            return False
        
        finally:
            # Close the connection
            conn.close()
            
    except sqlite3.Error as e:
        print(f"Database error: {e}")
        return False
    except Exception as e:
        print(f"Error: {e}")
        return False

if __name__ == "__main__":
    print("Database Record Deletion Tool")
    print("============================")
    print("This will delete ALL records from ALL tables in the racing_data.db database.")
    print("The database structure will be preserved, but all data will be removed.")
    
    confirm = input("Are you sure you want to proceed? (yes/no): ")
    
    if confirm.lower() in ['yes', 'y']:
        if clear_database():
            print("\nDatabase has been cleared successfully.")
        else:
            print("\nFailed to clear the database completely.")
    else:
        print("\nOperation cancelled. No changes were made to the database.") 