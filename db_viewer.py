# -*- coding: utf-8 -*-
import sqlite3
import pandas as pd

def check_database():
    """Check the contents of the racing_data.db file"""
    try:
        # Connect to the database
        conn = sqlite3.connect('racing_data.db')
        
        # Create a cursor
        cursor = conn.cursor()
        
        # Get table names
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        print(f"Tables in database: {[table[0] for table in tables]}")
        
        # Get column information for the races table
        cursor.execute("PRAGMA table_info(races)")
        columns = cursor.fetchall()
        print("\nColumns in races table:")
        for col in columns:
            print(f"  {col[1]} ({col[2]})")
        
        # Count rows in the races table
        cursor.execute("SELECT COUNT(*) FROM races")
        count = cursor.fetchone()[0]
        print(f"\nTotal races in database: {count}")
        
        # Get all race data and display it using pandas
        query = "SELECT * FROM races"
        df = pd.read_sql_query(query, conn)
        
        # Print the DataFrame with all columns
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', 1000)
        print("\nRace data in database:")
        print(df)
        
        # Close the connection
        conn.close()
        
    except sqlite3.Error as e:
        print(f"Database error: {e}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_database() 