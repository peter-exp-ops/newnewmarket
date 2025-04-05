#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Database_Operations.py - SQLite database operations for the racing database

This script provides utility functions for working with the racing database,
including query, modification, and maintenance operations.
"""

import sqlite3
import os
import pandas as pd
from datetime import datetime, timedelta

def connect_to_database(db_path="racing_data.db"):
    """
    Establish connection to the SQLite database
    
    Args:
        db_path (str): Path to the SQLite database file
        
    Returns:
        sqlite3.Connection: Connection object
    """
    conn = sqlite3.connect(db_path)
    # Enable foreign keys
    conn.execute("PRAGMA foreign_keys = ON")
    return conn

def get_all_races(conn):
    """
    Retrieve all races from the database
    
    Args:
        conn (sqlite3.Connection): Database connection
        
    Returns:
        list: List of race dictionaries
    """
    cursor = conn.cursor()
    cursor.execute("""
        SELECT * FROM races
        ORDER BY date, time
    """)
    
    columns = [description[0] for description in cursor.description]
    races = []
    
    for row in cursor.fetchall():
        race_dict = dict(zip(columns, row))
        races.append(race_dict)
        
    return races

def get_race_by_id(conn, race_id):
    """
    Retrieve a specific race by its ID
    
    Args:
        conn (sqlite3.Connection): Database connection
        race_id (int): The ID of the race to retrieve
        
    Returns:
        dict: Race information or None if not found
    """
    cursor = conn.cursor()
    cursor.execute("""
        SELECT * FROM races
        WHERE id = ?
    """, (race_id,))
    
    row = cursor.fetchone()
    if not row:
        return None
        
    columns = [description[0] for description in cursor.description]
    race_dict = dict(zip(columns, row))
    
    return race_dict

def delete_all_records(conn):
    """
    Delete all records from all tables in the database
    
    Args:
        conn (sqlite3.Connection): Database connection
        
    Returns:
        dict: Count of deleted records per table
    """
    cursor = conn.cursor()
    
    # Disable foreign key constraints temporarily
    conn.execute("PRAGMA foreign_keys = OFF")
    
    # Get all tables in the database
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [table[0] for table in cursor.fetchall() if table[0] != 'sqlite_sequence']
    
    results = {}
    # Delete records from each table
    for table in tables:
        cursor.execute(f"DELETE FROM {table}")
        results[table] = cursor.rowcount
    
    # Reset auto-increment counters
    cursor.execute("DELETE FROM sqlite_sequence")
    
    # Re-enable foreign key constraints
    conn.execute("PRAGMA foreign_keys = ON")
    
    # Commit the changes
    conn.commit()
    return results

def drop_all_tables(conn):
    """
    Drop all tables in the database
    
    Args:
        conn (sqlite3.Connection): Database connection
        
    Returns:
        list: Names of dropped tables
    """
    cursor = conn.cursor()
    
    # Disable foreign key constraints temporarily
    conn.execute("PRAGMA foreign_keys = OFF")
    
    # Get all tables in the database
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [table[0] for table in cursor.fetchall() if table[0] != 'sqlite_sequence']
    
    # Drop each table
    for table in tables:
        cursor.execute(f"DROP TABLE IF EXISTS {table}")
    
    # Commit the changes
    conn.commit()
    return tables

if __name__ == "__main__":
    # Connect to database
    conn = connect_to_database()
    try:
        # Step 1: Delete all records
        print("Step 1: Deleting all records...")
        deleted_records = delete_all_records(conn)
        for table, count in deleted_records.items():
            print(f"  - Deleted {count} record(s) from {table}")
        
        # Step 2: Drop all tables
        print("\nStep 2: Dropping all tables...")
        dropped_tables = drop_all_tables(conn)
        print(f"  - Dropped tables: {', '.join(dropped_tables)}")
        
        print("\nDatabase cleanup complete.")
    finally:
        conn.close() 