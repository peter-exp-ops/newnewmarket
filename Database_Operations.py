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

def create_races_table(conn):
    """
    Create the races table in the database
    
    Args:
        conn (sqlite3.Connection): Database connection
        
    Returns:
        bool: True if table was created or already exists
    """
    cursor = conn.cursor()
    
    # Create races table with specified fields
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS races (
        ID INTEGER PRIMARY KEY AUTOINCREMENT,
        Course TEXT,
        Type TEXT,
        Datetime TIMESTAMP,
        Name TEXT,
        Agerestriction TEXT,
        Class TEXT,
        Distance REAL,
        Going TEXT,
        Runners INTEGER,
        Surface TEXT,
        Offtime REAL,
        Winningtime REAL,
        Prize REAL
    )
    """)
    
    # Create index on datetime for faster queries
    cursor.execute("""
    CREATE INDEX IF NOT EXISTS idx_races_datetime ON races (Datetime)
    """)
    
    # Commit the changes
    conn.commit()
    return True

def create_trainers_table(conn):
    """
    Create the trainers table in the database
    
    Args:
        conn (sqlite3.Connection): Database connection
        
    Returns:
        bool: True if table was created or already exists
    """
    cursor = conn.cursor()
    
    # Create trainers table with specified fields
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS trainers (
        ID INTEGER PRIMARY KEY AUTOINCREMENT,
        Name TEXT UNIQUE NOT NULL
    )
    """)
    
    # Create index on name for faster lookups
    cursor.execute("""
    CREATE INDEX IF NOT EXISTS idx_trainers_name ON trainers (Name)
    """)
    
    # Commit the changes
    conn.commit()
    return True

def create_jockeys_table(conn):
    """
    Create the jockeys table in the database
    
    Args:
        conn (sqlite3.Connection): Database connection
        
    Returns:
        bool: True if table was created or already exists
    """
    cursor = conn.cursor()
    
    # Create jockeys table with specified fields
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS jockeys (
        ID INTEGER PRIMARY KEY AUTOINCREMENT,
        Name TEXT UNIQUE NOT NULL
    )
    """)
    
    # Create index on name for faster lookups
    cursor.execute("""
    CREATE INDEX IF NOT EXISTS idx_jockeys_name ON jockeys (Name)
    """)
    
    # Commit the changes
    conn.commit()
    return True

def create_horses_table(conn):
    """
    Create the horses table in the database
    
    Args:
        conn (sqlite3.Connection): Database connection
        
    Returns:
        bool: True if table was created or already exists
    """
    cursor = conn.cursor()
    
    # Create horses table with specified fields
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS horses (
        ID INTEGER PRIMARY KEY AUTOINCREMENT,
        Name TEXT UNIQUE NOT NULL
    )
    """)
    
    # Create index on name for faster lookups
    cursor.execute("""
    CREATE INDEX IF NOT EXISTS idx_horses_name ON horses (Name)
    """)
    
    # Commit the changes
    conn.commit()
    return True

def rename_racehorse_table(conn):
    """
    Rename the racehorse table to racehorses
    
    Args:
        conn (sqlite3.Connection): Database connection
        
    Returns:
        bool: True if table was renamed successfully
    """
    cursor = conn.cursor()
    
    # Check if the old table exists
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='racehorse'")
    if not cursor.fetchone():
        print("Table 'racehorse' does not exist.")
        return False
    
    # Rename the table - SQLite doesn't have a direct RENAME TABLE command,
    # so we need to create a new table and copy the data
    cursor.execute("""
    CREATE TABLE racehorses (
        ID INTEGER PRIMARY KEY AUTOINCREMENT,
        raceID INTEGER NOT NULL,
        horseID INTEGER NOT NULL,
        jockeyID INTEGER,
        trainerID INTEGER,
        time REAL,
        position INTEGER,
        positionof INTEGER,
        timeahead REAL,
        timebehind REAL,
        FOREIGN KEY (raceID) REFERENCES races(ID) ON DELETE CASCADE,
        FOREIGN KEY (horseID) REFERENCES horses(ID) ON DELETE CASCADE,
        FOREIGN KEY (jockeyID) REFERENCES jockeys(ID) ON DELETE SET NULL,
        FOREIGN KEY (trainerID) REFERENCES trainers(ID) ON DELETE SET NULL
    )
    """)
    
    # Copy data from old table to new table
    cursor.execute("INSERT INTO racehorses SELECT * FROM racehorse")
    
    # Drop the old table
    cursor.execute("DROP TABLE racehorse")
    
    # Recreate the indexes
    cursor.execute("CREATE INDEX idx_racehorses_race ON racehorses (raceID)")
    cursor.execute("CREATE INDEX idx_racehorses_horse ON racehorses (horseID)")
    cursor.execute("CREATE INDEX idx_racehorses_jockey ON racehorses (jockeyID)")
    cursor.execute("CREATE INDEX idx_racehorses_trainer ON racehorses (trainerID)")
    
    # Commit the changes
    conn.commit()
    print("Table 'racehorse' renamed to 'racehorses'.")
    return True

def create_racehorses_table(conn):
    """
    Create the racehorses table in the database
    
    Args:
        conn (sqlite3.Connection): Database connection
        
    Returns:
        bool: True if table was created or already exists
    """
    cursor = conn.cursor()
    
    # Create racehorses table with specified fields
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS racehorses (
        ID INTEGER PRIMARY KEY AUTOINCREMENT,
        raceID INTEGER NOT NULL,
        horseID INTEGER NOT NULL,
        jockeyID INTEGER,
        trainerID INTEGER,
        time REAL,
        position INTEGER,
        positionof INTEGER,
        timeahead REAL,
        timebehind REAL,
        FOREIGN KEY (raceID) REFERENCES races(ID) ON DELETE CASCADE,
        FOREIGN KEY (horseID) REFERENCES horses(ID) ON DELETE CASCADE,
        FOREIGN KEY (jockeyID) REFERENCES jockeys(ID) ON DELETE SET NULL,
        FOREIGN KEY (trainerID) REFERENCES trainers(ID) ON DELETE SET NULL
    )
    """)
    
    # Create indexes for faster lookups and joins
    cursor.execute("""
    CREATE INDEX IF NOT EXISTS idx_racehorses_race ON racehorses (raceID)
    """)
    cursor.execute("""
    CREATE INDEX IF NOT EXISTS idx_racehorses_horse ON racehorses (horseID)
    """)
    cursor.execute("""
    CREATE INDEX IF NOT EXISTS idx_racehorses_jockey ON racehorses (jockeyID)
    """)
    cursor.execute("""
    CREATE INDEX IF NOT EXISTS idx_racehorses_trainer ON racehorses (trainerID)
    """)
    
    # Commit the changes
    conn.commit()
    return True

def initialize_database(db_path="racing_data.db"):
    """
    Initialize the database and create necessary tables
    
    Args:
        db_path (str): Path to the SQLite database file
        
    Returns:
        sqlite3.Connection: Connection object
    """
    # Create database directory if it doesn't exist
    db_dir = os.path.dirname(db_path)
    if db_dir and not os.path.exists(db_dir):
        os.makedirs(db_dir)
    
    # Connect to the database
    conn = connect_to_database(db_path)
    
    # Create tables
    create_races_table(conn)
    create_trainers_table(conn)
    create_jockeys_table(conn)
    create_horses_table(conn)
    create_racehorses_table(conn)
    
    conn.commit()
    return conn

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
    print("Connecting to database...")
    conn = connect_to_database()
    try:
        # Rename racehorse table to racehorses
        print("Renaming racehorse table to racehorses...")
        rename_racehorse_table(conn)
        
        # Get schema for racehorses table
        print("\nSchema for racehorses table:")
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(racehorses)")
        for column in cursor.fetchall():
            column_name = column[1]
            data_type = column[2]
            print(f"  {column_name} ({data_type})")
        
        # Check all tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [table[0] for table in cursor.fetchall()]
        print(f"\nTables in database: {', '.join(tables)}")
        
        print("\nDatabase update complete.")
    finally:
        conn.close() 