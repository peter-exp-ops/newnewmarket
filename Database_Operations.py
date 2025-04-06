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
import tkinter as tk
from tkinter import messagebox, ttk, scrolledtext

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
        Name TEXT UNIQUE NOT NULL,
        Foaled DATETIME,
        Sire TEXT,
        Dam TEXT,
        Owner TEXT
    )
    """)
    
    # Create index on name for faster lookups
    cursor.execute("""
    CREATE INDEX IF NOT EXISTS idx_horses_name ON horses (Name)
    """)
    
    # Commit the changes
    conn.commit()
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
        RaceID INTEGER NOT NULL,
        HorseID INTEGER NOT NULL,
        JockeyID INTEGER,
        TrainerID INTEGER,
        Time REAL,
        Position INTEGER,
        Positionof INTEGER,
        Timeahead REAL,
        Timebehind REAL,
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

def create_urls_table(conn):
    """
    Create the urls table in the database
    
    Args:
        conn (sqlite3.Connection): Database connection
        
    Returns:
        bool: True if table was created or already exists
    """
    cursor = conn.cursor()
    
    # Create urls table with specified fields
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS urls (
        ID INTEGER PRIMARY KEY AUTOINCREMENT,
        URL TEXT UNIQUE NOT NULL,
        Date_accessed TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        status TEXT DEFAULT "unprocessed",
        Type TEXT
    )
    """)
    
    # Create index on URL for faster lookups
    cursor.execute("""
    CREATE INDEX IF NOT EXISTS idx_urls_url ON urls (URL)
    """)
    
    # Create index on status for filtering
    cursor.execute("""
    CREATE INDEX IF NOT EXISTS idx_urls_status ON urls (status)
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
    create_urls_table(conn)
    
    conn.commit()
    return conn

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

def get_database_info(conn):
    """
    Get information about all tables in the database
    
    Args:
        conn (sqlite3.Connection): Database connection
        
    Returns:
        str: Information about the database structure
    """
    cursor = conn.cursor()
    
    # Get all tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [table[0] for table in cursor.fetchall()]
    
    info = "Database Information:\n"
    info += f"Found {len(tables)} tables: {', '.join(tables)}\n\n"
    
    # Get schema for each table
    for table in tables:
        info += f"Schema for {table} table:\n"
        cursor.execute(f"PRAGMA table_info({table})")
        for column in cursor.fetchall():
            column_name = column[1]
            data_type = column[2]
            info += f"  {column_name} ({data_type})\n"
        
        # Get row count
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        row_count = cursor.fetchone()[0]
        info += f"  Total rows: {row_count}\n\n"
    
    return info

class DatabaseUI:
    def __init__(self, root):
        self.root = root
        self.root.title("Newmarket - Database Operations")
        self.root.geometry("800x600")
        
        # Set application icon for both window and taskbar
        try:
            icon_path = "Icon 32px.png"
            if os.path.exists(icon_path):
                # For Windows taskbar and window icon
                self.root.iconbitmap(icon_path)
                
                # For cross-platform window icon (Tkinter PhotoImage)
                icon_img = tk.PhotoImage(file=icon_path)
                self.root.tk.call('wm', 'iconphoto', self.root._w, icon_img)
        except Exception as e:
            print(f"Could not set application icon: {e}")
            # Will log the error after connecting to avoid calling self.log before it's ready
        
        self.conn = None
        
        # Create a frame for the top section
        self.top_frame = ttk.Frame(root, padding=10)
        self.top_frame.pack(fill=tk.X)
        
        # Connection status
        self.status_var = tk.StringVar(value="Not connected")
        ttk.Label(self.top_frame, text="Status:").grid(row=0, column=0, sticky=tk.W, padx=5, pady=5)
        ttk.Label(self.top_frame, textvariable=self.status_var).grid(row=0, column=1, sticky=tk.W, padx=5, pady=5)
        
        # Connect button
        ttk.Button(self.top_frame, text="Connect to Database", command=self.connect).grid(row=0, column=2, padx=5, pady=5)
        
        # Create a frame for the buttons
        self.button_frame = ttk.LabelFrame(root, text="Database Operations", padding=10)
        self.button_frame.pack(fill=tk.X, padx=10, pady=10)
        
        # Create buttons for each function
        self.create_buttons()
        
        # Create a frame for the output
        self.output_frame = ttk.LabelFrame(root, text="Output", padding=10)
        self.output_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # Create a scrolled text widget for output
        self.output_text = scrolledtext.ScrolledText(self.output_frame, wrap=tk.WORD, width=80, height=20)
        self.output_text.pack(fill=tk.BOTH, expand=True)
        
    def create_buttons(self):
        # Row 1
        ttk.Button(self.button_frame, text="Initialize Database", 
                   command=lambda: self.execute_function(initialize_database, "Initializing database...")).grid(
                   row=0, column=0, padx=5, pady=5, sticky=tk.W+tk.E)
        
        ttk.Button(self.button_frame, text="Create Races Table", 
                   command=lambda: self.execute_function(create_races_table, "Creating races table...")).grid(
                   row=0, column=1, padx=5, pady=5, sticky=tk.W+tk.E)
        
        ttk.Button(self.button_frame, text="Create Trainers Table", 
                   command=lambda: self.execute_function(create_trainers_table, "Creating trainers table...")).grid(
                   row=0, column=2, padx=5, pady=5, sticky=tk.W+tk.E)
        
        # Row 2
        ttk.Button(self.button_frame, text="Create Jockeys Table", 
                   command=lambda: self.execute_function(create_jockeys_table, "Creating jockeys table...")).grid(
                   row=1, column=0, padx=5, pady=5, sticky=tk.W+tk.E)
        
        ttk.Button(self.button_frame, text="Create Horses Table", 
                   command=lambda: self.execute_function(create_horses_table, "Creating horses table...")).grid(
                   row=1, column=1, padx=5, pady=5, sticky=tk.W+tk.E)
        
        ttk.Button(self.button_frame, text="Create Racehorses Table", 
                   command=lambda: self.execute_function(create_racehorses_table, "Creating racehorses table...")).grid(
                   row=1, column=2, padx=5, pady=5, sticky=tk.W+tk.E)
        
        # Row 3
        ttk.Button(self.button_frame, text="Create URLs Table", 
                   command=lambda: self.execute_function(create_urls_table, "Creating URLs table...")).grid(
                   row=2, column=0, padx=5, pady=5, sticky=tk.W+tk.E)
        
        ttk.Button(self.button_frame, text="Delete All Records", 
                   command=lambda: self.execute_function(delete_all_records, "Deleting all records...")).grid(
                   row=2, column=1, padx=5, pady=5, sticky=tk.W+tk.E)
        
        ttk.Button(self.button_frame, text="Drop All Tables", 
                   command=lambda: self.execute_function(drop_all_tables, "Dropping all tables...")).grid(
                   row=2, column=2, padx=5, pady=5, sticky=tk.W+tk.E)
        
        # Row 4
        ttk.Button(self.button_frame, text="Database Info", 
                   command=lambda: self.execute_function(get_database_info, "Getting database info...")).grid(
                   row=3, column=0, padx=5, pady=5, sticky=tk.W+tk.E)
        
        # Configure grid weights for responsiveness
        for i in range(3):
            self.button_frame.columnconfigure(i, weight=1)
        
    def connect(self):
        try:
            if self.conn is not None:
                self.conn.close()
            
            self.conn = connect_to_database()
            self.status_var.set("Connected to racing_data.db")
            self.log("Successfully connected to the database.")
            
            # Log any icon errors that might have occurred during initialization
            if not os.path.exists("Icon 32px.png"):
                self.log("Warning: Icon 32px.png not found in the current directory.")
        except Exception as e:
            self.status_var.set("Connection failed")
            self.log(f"Failed to connect to database: {e}")
            messagebox.showerror("Connection Error", f"Failed to connect to database: {e}")
    
    def execute_function(self, func, status_message):
        if self.conn is None:
            messagebox.showerror("Not Connected", "Please connect to the database first.")
            return
        
        try:
            self.log(status_message)
            
            if func == initialize_database:
                # Special case for initialize_database
                self.conn.close()
                self.conn = func()
                self.log("Database initialized successfully.")
            elif func == get_database_info:
                # Special case for get_database_info
                result = func(self.conn)
                self.log(result)
            else:
                # Regular function execution
                result = func(self.conn)
                
                if isinstance(result, dict):
                    # For delete_all_records
                    for table, count in result.items():
                        self.log(f"Deleted {count} record(s) from {table}")
                elif isinstance(result, list):
                    # For drop_all_tables
                    self.log(f"Operation affected tables: {', '.join(result)}")
                elif result is True:
                    # For table creation functions
                    self.log("Operation completed successfully.")
                else:
                    self.log(f"Result: {result}")
            
        except Exception as e:
            self.log(f"Error executing operation: {e}")
            messagebox.showerror("Operation Error", f"Failed to execute operation: {e}")
    
    def log(self, message):
        self.output_text.insert(tk.END, f"{message}\n")
        self.output_text.see(tk.END)

if __name__ == "__main__":
    root = tk.Tk()
    app = DatabaseUI(root)
    root.mainloop() 