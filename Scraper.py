#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Scraper.py - UI for racing data scraping operations

This script provides a basic user interface for scraping racing data
with sections for database connection, crawling, scraping, and output.
"""

import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox
import os
import sys
import sqlite3
import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
import time
import threading
from urllib.parse import urljoin, urlparse

class ScraperUI:
    def __init__(self, root):
        self.root = root
        self.root.title("Newmarket - Racing Data Scraper")
        self.root.geometry("800x600")
        
        # Database connection
        self.conn = None
        
        # Crawl state
        self.crawl_running = False
        self.crawl_thread = None
        
        # Set application icon if available
        try:
            icon_path = "Icon 32px.png"
            if os.path.exists(icon_path):
                # For Windows
                self.root.iconbitmap(icon_path)
                # For cross-platform
                icon_img = tk.PhotoImage(file=icon_path)
                self.root.tk.call('wm', 'iconphoto', self.root._w, icon_img)
        except Exception as e:
            print(f"Icon error: {e}")
        
        # Configure main window
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        
        # Create scrollable canvas
        self.create_scrollable_canvas()
        
        # Create frames for each section inside the canvas
        self.create_database_frame()
        self.create_crawl_frame()
        self.create_scrape_frame()
        self.create_output_frame()
        
        # Update the canvas scroll region
        self.update_scroll_region()
        
        # Ensure window doesn't close immediately
        self.root.protocol("WM_DELETE_WINDOW", self.on_closing)
    
    def on_closing(self):
        """Handle window closing"""
        # Stop any running crawl
        if self.crawl_running and self.crawl_thread and self.crawl_thread.is_alive():
            self.crawl_running = False
            self.crawl_thread.join(2)  # Wait for 2 seconds for thread to terminate
        
        # Close database connection if open
        if self.conn:
            self.conn.close()
        self.root.destroy()
    
    def create_scrollable_canvas(self):
        """Create a scrollable canvas for the main content"""
        # Create a frame with scrollbar
        self.main_frame = ttk.Frame(self.root)
        self.main_frame.grid(row=0, column=0, sticky="nsew")
        self.main_frame.columnconfigure(0, weight=1)
        self.main_frame.rowconfigure(0, weight=1)
        
        # Create a canvas with scrollbar
        self.canvas = tk.Canvas(self.main_frame, borderwidth=0, highlightthickness=0)
        self.scrollbar = ttk.Scrollbar(self.main_frame, orient="vertical", command=self.canvas.yview)
        self.canvas.configure(yscrollcommand=self.scrollbar.set)
        
        # Place canvas and scrollbar in grid
        self.canvas.grid(row=0, column=0, sticky="nsew")
        self.scrollbar.grid(row=0, column=1, sticky="ns")
        
        # Create a frame inside the canvas to hold the content
        self.content_frame = ttk.Frame(self.canvas)
        self.canvas_window = self.canvas.create_window((0, 0), window=self.content_frame, anchor="nw")
        
        # Configure the content frame and canvas
        self.content_frame.columnconfigure(0, weight=1)
        self.content_frame.bind("<Configure>", self.update_scroll_region)
        self.canvas.bind("<Configure>", self.on_canvas_configure)
        
        # Bind scroll events for mouse wheel
        try:
            # Windows mouse wheel binding
            self.canvas.bind_all("<MouseWheel>", self.on_mousewheel_windows)
            # Linux mouse wheel binding
            self.canvas.bind_all("<Button-4>", self.on_mousewheel_linux)
            self.canvas.bind_all("<Button-5>", self.on_mousewheel_linux)
        except Exception as e:
            print(f"Error setting up mouse wheel bindings: {e}")
    
    def on_canvas_configure(self, event):
        """Update the canvas width when the window is resized"""
        canvas_width = event.width
        self.canvas.itemconfig(self.canvas_window, width=canvas_width)
    
    def update_scroll_region(self, event=None):
        """Update the canvas scroll region to encompass all content"""
        self.canvas.configure(scrollregion=self.canvas.bbox("all"))
    
    def on_mousewheel_windows(self, event):
        """Handle mouse wheel scrolling for Windows"""
        try:
            self.canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")
        except Exception as e:
            print(f"Windows scrolling error: {e}")
    
    def on_mousewheel_linux(self, event):
        """Handle mouse wheel scrolling for Linux"""
        try:
            if event.num == 4:  # Scroll up
                self.canvas.yview_scroll(-1, "units")
            elif event.num == 5:  # Scroll down
                self.canvas.yview_scroll(1, "units")
        except Exception as e:
            print(f"Linux scrolling error: {e}")
    
    def connect_to_database(self):
        """Connect to the racing_data.db database"""
        try:
            # Close existing connection if any
            if self.conn:
                self.conn.close()
                self.conn = None
            
            # Connect to the database
            self.conn = sqlite3.connect('racing_data.db')
            self.log("Successfully connected to racing_data.db")
            self.connection_status_var.set("Connected")
            
            # Update the stats
            self.get_database_stats()
            
            # Update the previous session info
            self.update_previous_session_info()
            
        except Exception as e:
            self.log(f"Error connecting to database: {e}")
            self.connection_status_var.set("Connection Failed")
            messagebox.showerror("Database Connection Error", f"Could not connect to racing_data.db: {e}")
    
    def get_database_stats(self):
        """Get statistics from the database and update the table"""
        if not self.conn:
            self.log("Please connect to the database first")
            messagebox.showinfo("Not Connected", "Please connect to the database first.")
            return
        
        try:
            # Check if the urls table exists
            cursor = self.conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='urls'")
            if not cursor.fetchone():
                self.log("The urls table does not exist in the database")
                messagebox.showinfo("Table Missing", "The urls table does not exist in the database.")
                return
            
            # Initialize the data dictionary
            stats_data = {
                'Type': ['Races', 'Jockeys', 'Trainers', 'Horses', 'Total'],
                'Unprocessed': [0, 0, 0, 0, 0],
                'Failed': [0, 0, 0, 0, 0],
                'Succeeded': [0, 0, 0, 0, 0],
                'Total': [0, 0, 0, 0, 0]
            }
            
            # Query for the counts by type and status
            types = ['races', 'jockeys', 'trainers', 'horses']
            statuses = ['unprocessed', 'error', 'processed']
            status_mapping = {'unprocessed': 'Unprocessed', 'error': 'Failed', 'processed': 'Succeeded'}
            
            # Get counts for each type and status
            for i, type_name in enumerate(types):
                for status in statuses:
                    cursor.execute(f"SELECT COUNT(*) FROM urls WHERE Type=? AND status=?", (type_name, status))
                    count = cursor.fetchone()[0]
                    
                    # Update the stats data
                    stats_data[status_mapping[status]][i] = count
                    
                    # Add to the total row
                    stats_data[status_mapping[status]][4] += count
                
                # Calculate the total for this type
                stats_data['Total'][i] = sum(stats_data[col][i] for col in ['Unprocessed', 'Failed', 'Succeeded'])
            
            # Calculate total of totals
            stats_data['Total'][4] = sum(stats_data['Total'][0:4])
            
            # Create a DataFrame for easier handling
            df = pd.DataFrame(stats_data)
            
            # Update the treeview
            self.update_stats_treeview(df)
            
            self.log("Database stats updated successfully")
            
        except Exception as e:
            self.log(f"Error getting database stats: {e}")
            messagebox.showerror("Database Error", f"Error getting stats: {e}")
    
    def update_stats_treeview(self, df):
        """Update the treeview with the stats data"""
        # Clear existing items
        for item in self.stats_tree.get_children():
            self.stats_tree.delete(item)
        
        # Add the new data
        for i in range(len(df)):
            values = [df.iloc[i, col] for col in range(len(df.columns))]
            self.stats_tree.insert("", "end", values=values)
    
    def create_database_frame(self):
        """Create the database connection section"""
        self.db_frame = ttk.LabelFrame(self.content_frame, text="Database Connection")
        self.db_frame.grid(row=0, column=0, padx=5, pady=(0, 2), sticky="ew")
        
        # Make the frame expand horizontally
        self.db_frame.columnconfigure(0, weight=1)
        
        # Connection status
        status_frame = ttk.Frame(self.db_frame)
        status_frame.pack(fill="x", padx=5, pady=5)
        
        ttk.Label(status_frame, text="Status:").pack(side="left", padx=(0, 5))
        self.connection_status_var = tk.StringVar(value="Not Connected")
        ttk.Label(status_frame, textvariable=self.connection_status_var).pack(side="left")
        
        # Connect button
        connect_button = ttk.Button(self.db_frame, text="Connect to database", command=self.connect_to_database)
        connect_button.pack(fill="x", padx=5, pady=5)
        
        # Get stats button
        stats_button = ttk.Button(self.db_frame, text="Get stats", command=self.get_database_stats)
        stats_button.pack(fill="x", padx=5, pady=5)
        
        # Stats table frame - using pack with expand=False to make it fit the content's height
        table_frame = ttk.Frame(self.db_frame)
        table_frame.pack(fill="x", expand=False, padx=5, pady=5)
        
        # Create the treeview (table) with scrollbars
        # Set a fixed height for 5 rows (one for each type + header)
        self.stats_tree = ttk.Treeview(table_frame, columns=("Type", "Unprocessed", "Failed", "Succeeded", "Total"), height=5)
        
        # Configure columns
        self.stats_tree.column("#0", width=0, stretch=tk.NO)  # Hide the first column
        self.stats_tree.column("Type", width=100, anchor="center")  # Center alignment for Type column
        self.stats_tree.column("Unprocessed", width=100, anchor="center")
        self.stats_tree.column("Failed", width=100, anchor="center")
        self.stats_tree.column("Succeeded", width=100, anchor="center")
        self.stats_tree.column("Total", width=100, anchor="center")
        
        # Configure headings
        self.stats_tree.heading("#0", text="")
        self.stats_tree.heading("Type", text="Type")
        self.stats_tree.heading("Unprocessed", text="Unprocessed")
        self.stats_tree.heading("Failed", text="Failed")
        self.stats_tree.heading("Succeeded", text="Succeeded")
        self.stats_tree.heading("Total", text="Total")
        
        # Add a scrollbar
        tree_scrollbar = ttk.Scrollbar(table_frame, orient="vertical", command=self.stats_tree.yview)
        self.stats_tree.configure(yscrollcommand=tree_scrollbar.set)
        
        # Pack the treeview and scrollbar
        self.stats_tree.pack(side="left", fill="x", expand=True)
        tree_scrollbar.pack(side="right", fill="y")
        
        # Add some initial data rows with capitalized type values
        initial_data = [
            ("Races", 0, 0, 0, 0),
            ("Jockeys", 0, 0, 0, 0),
            ("Trainers", 0, 0, 0, 0),
            ("Horses", 0, 0, 0, 0),
            ("Total", 0, 0, 0, 0)
        ]
        
        for row in initial_data:
            self.stats_tree.insert("", "end", values=row)
    
    def create_crawl_frame(self):
        """Create the crawl section"""
        self.crawl_frame = ttk.LabelFrame(self.content_frame, text="Crawl")
        self.crawl_frame.grid(row=1, column=0, padx=5, pady=2, sticky="ew")
        
        # Make the frame expand horizontally
        self.crawl_frame.columnconfigure(0, weight=1)
        
        # Base URL field
        base_url_frame = ttk.Frame(self.crawl_frame)
        base_url_frame.pack(fill="x", padx=5, pady=5)
        ttk.Label(base_url_frame, text="Base URL:").pack(anchor="w")
        self.base_url_var = tk.StringVar(value="https://www.sportinglife.com/racing/results/")
        ttk.Entry(base_url_frame, textvariable=self.base_url_var).pack(fill="x", pady=(2, 0))
        
        # Timeout field
        timeout_frame = ttk.Frame(self.crawl_frame)
        timeout_frame.pack(fill="x", padx=5, pady=5)
        ttk.Label(timeout_frame, text="Timeout (mins):").pack(anchor="w")
        self.timeout_var = tk.StringVar(value="1")
        ttk.Entry(timeout_frame, textvariable=self.timeout_var).pack(fill="x", pady=(2, 0))
        
        # Max URLs field
        max_urls_frame = ttk.Frame(self.crawl_frame)
        max_urls_frame.pack(fill="x", padx=5, pady=5)
        ttk.Label(max_urls_frame, text="Max URLs:").pack(anchor="w")
        self.max_urls_var = tk.StringVar(value="100")
        ttk.Entry(max_urls_frame, textvariable=self.max_urls_var).pack(fill="x", pady=(2, 0))
        
        # Saturation limit field
        saturation_frame = ttk.Frame(self.crawl_frame)
        saturation_frame.pack(fill="x", padx=5, pady=5)
        ttk.Label(saturation_frame, text="Saturation limit (%):").pack(anchor="w")
        self.saturation_var = tk.StringVar(value="5")
        ttk.Entry(saturation_frame, textvariable=self.saturation_var).pack(fill="x", pady=(2, 0))
        
        # Resume crawl checkbox
        resume_frame = ttk.Frame(self.crawl_frame)
        resume_frame.pack(fill="x", padx=5, pady=5)
        self.resume_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(resume_frame, text="Resume from previous crawl", variable=self.resume_var).pack(anchor="w")
        
        # Crawl button
        self.crawl_button = ttk.Button(self.crawl_frame, text="Crawl", command=self.start_crawl)
        self.crawl_button.pack(fill="x", padx=5, pady=5)
        
        # Crawl status
        status_frame = ttk.Frame(self.crawl_frame)
        status_frame.pack(fill="x", padx=5, pady=5)
        
        ttk.Label(status_frame, text="Status:").pack(side="left", padx=(0, 5))
        self.crawl_status_var = tk.StringVar(value="Ready")
        ttk.Label(status_frame, textvariable=self.crawl_status_var).pack(side="left")
        
        # Progress frame
        progress_frame = ttk.Frame(self.crawl_frame)
        progress_frame.pack(fill="x", padx=5, pady=5)
        
        # URLs found label
        self.urls_found_var = tk.StringVar(value="URLs found: 0")
        ttk.Label(progress_frame, textvariable=self.urls_found_var).pack(side="left", padx=(0, 10))
        
        # Saturation label
        self.saturation_rate_var = tk.StringVar(value="Saturation: 0%")
        ttk.Label(progress_frame, textvariable=self.saturation_rate_var).pack(side="left")
        
        # Session info frame
        session_frame = ttk.Frame(self.crawl_frame)
        session_frame.pack(fill="x", padx=5, pady=5)
        
        # Previous session label
        self.previous_session_var = tk.StringVar(value="Previous session: None")
        ttk.Label(session_frame, textvariable=self.previous_session_var).pack(side="left")
        
        # Get previous session info when frame is created
        self.update_previous_session_info()
    
    def update_previous_session_info(self):
        """Update information about previous crawl sessions"""
        if not self.conn:
            try:
                # Try to connect temporarily to get the info
                temp_conn = sqlite3.connect('racing_data.db')
                cursor = temp_conn.cursor()
                
                # Get total URLs count
                cursor.execute("SELECT COUNT(*) FROM urls")
                total_urls = cursor.fetchone()[0]
                
                # Get last crawl date
                cursor.execute("SELECT MAX(Date_accessed) FROM urls")
                last_date = cursor.fetchone()[0]
                
                if total_urls > 0 and last_date:
                    self.previous_session_var.set(f"Previous session: {last_date} ({total_urls} URLs)")
                else:
                    self.previous_session_var.set("Previous session: None")
                
                temp_conn.close()
            except:
                self.previous_session_var.set("Previous session: Unknown")
        else:
            # If already connected, use the existing connection
            cursor = self.conn.cursor()
            
            # Get total URLs count
            cursor.execute("SELECT COUNT(*) FROM urls")
            total_urls = cursor.fetchone()[0]
            
            # Get last crawl date
            cursor.execute("SELECT MAX(Date_accessed) FROM urls")
            last_date = cursor.fetchone()[0]
            
            if total_urls > 0 and last_date:
                self.previous_session_var.set(f"Previous session: {last_date} ({total_urls} URLs)")
            else:
                self.previous_session_var.set("Previous session: None")
    
    def start_crawl(self):
        """Start the crawling process in a separate thread"""
        if self.crawl_running:
            self.log("Crawl already in progress")
            return
        
        if not self.conn:
            self.log("Please connect to the database first")
            messagebox.showinfo("Not Connected", "Please connect to the database first.")
            return
        
        # Validate inputs
        try:
            timeout_mins = float(self.timeout_var.get())
            max_urls = int(self.max_urls_var.get())
            saturation_limit = float(self.saturation_var.get())
            
            if timeout_mins <= 0 or max_urls <= 0 or saturation_limit <= 0:
                raise ValueError("Values must be positive")
                
        except ValueError as e:
            self.log(f"Invalid input values: {e}")
            messagebox.showerror("Invalid Input", "Please enter valid values for all fields.")
            return
        
        # Start crawler in a separate thread
        self.crawl_running = True
        self.crawl_status_var.set("Running")
        self.crawl_button.config(text="Running...", state="disabled")
        
        self.crawl_thread = threading.Thread(target=self.run_crawler)
        self.crawl_thread.daemon = True
        self.crawl_thread.start()
        
        # Periodically check if crawl is still running
        self.root.after(500, self.check_crawl_status)
    
    def check_crawl_status(self):
        """Check if the crawl thread is still running and update UI accordingly"""
        if self.crawl_running and self.crawl_thread and self.crawl_thread.is_alive():
            # Still running, check again later
            self.root.after(500, self.check_crawl_status)
        else:
            # Crawl finished or stopped
            self.crawl_running = False
            self.crawl_status_var.set("Ready")
            self.crawl_button.config(text="Crawl", state="normal")
            
            # Update stats
            self.get_database_stats()
            
            # Update session info
            self.update_previous_session_info()
    
    def run_crawler(self):
        """Run the web crawler"""
        base_url = self.base_url_var.get()
        timeout_mins = float(self.timeout_var.get())
        max_urls = int(self.max_urls_var.get())
        saturation_limit = float(self.saturation_var.get()) / 100.0  # Convert from percentage to decimal
        resume_crawl = self.resume_var.get()
        
        try:
            self.log(f"Starting crawl from {base_url}")
            self.log(f"Timeout: {timeout_mins} mins, Max URLs: {max_urls}, Saturation limit: {saturation_limit*100}%")
            if resume_crawl:
                self.log("Resuming from previous crawl")
            
            # Create a new database connection for this thread
            try:
                # SQLite connections cannot be shared between threads
                # So we need a new connection for the crawler thread
                crawler_conn = sqlite3.connect('racing_data.db')
                cursor = crawler_conn.cursor()
                self.log("Created database connection for crawler thread")
            except Exception as e:
                self.log(f"Failed to create database connection in crawler thread: {e}")
                return
            
            # Initialize crawl variables
            start_time = time.time()
            end_time = start_time + (timeout_mins * 60)
            urls_found = 0
            visited = set()
            
            # Initialize the to_visit list
            to_visit = []
            
            # Initialize session ID for tracking
            session_id = None
            
            # If resuming from previous crawl, initialize from the database
            if resume_crawl:
                # Get all URLs that have been processed
                cursor.execute("SELECT URL FROM urls")
                processed_urls = set(url[0] for url in cursor.fetchall())
                
                # Add all processed URLs to the visited set
                visited.update(processed_urls)
                self.log(f"Loaded {len(visited)} URLs from previous crawls")
                
                # Get unprocessed URLs to start with
                cursor.execute("SELECT URL FROM urls WHERE status='unprocessed' ORDER BY Date_accessed DESC LIMIT 500")
                to_visit = [url[0] for url in cursor.fetchall()]
                
                # If no unprocessed URLs, look for profile pages that might lead to new content
                if not to_visit:
                    cursor.execute("""
                        SELECT URL FROM urls 
                        WHERE (URL LIKE '%/profiles/trainer/%' OR URL LIKE '%/profiles/jockey/%' OR URL LIKE '%/profiles/horse/%')
                        ORDER BY Date_accessed DESC LIMIT 200
                    """)
                    to_visit = [url[0] for url in cursor.fetchall()]
                
                # If still no URLs to visit, fall back to base URL
                if not to_visit:
                    self.log("No previous URLs to resume from, starting with base URL")
                    to_visit = [base_url]
                else:
                    self.log(f"Resuming with {len(to_visit)} URLs from previous crawl")
                    
                # Create a new crawl session record for the resumed crawl
                cursor.execute(
                    "CREATE TABLE IF NOT EXISTS crawl_sessions (ID INTEGER PRIMARY KEY AUTOINCREMENT, start_time TIMESTAMP, end_time TIMESTAMP, urls_found INTEGER, base_url TEXT, is_resume BOOLEAN)"
                )
                cursor.execute(
                    "INSERT INTO crawl_sessions (start_time, base_url, is_resume) VALUES (?, ?, ?)",
                    (time.strftime('%Y-%m-%d %H:%M:%S'), base_url, True)
                )
                crawler_conn.commit()
                
                # Get the session ID for this crawl
                cursor.execute("SELECT last_insert_rowid()")
                session_id = cursor.fetchone()[0]
                self.log(f"Created resumed crawl session #{session_id}")
            else:
                # Not resuming, just start with the base URL
                to_visit = [base_url]
                
                # Create a crawl session record
                cursor.execute(
                    "CREATE TABLE IF NOT EXISTS crawl_sessions (ID INTEGER PRIMARY KEY AUTOINCREMENT, start_time TIMESTAMP, end_time TIMESTAMP, urls_found INTEGER, base_url TEXT, is_resume BOOLEAN)"
                )
                cursor.execute(
                    "INSERT INTO crawl_sessions (start_time, base_url, is_resume) VALUES (?, ?, ?)",
                    (time.strftime('%Y-%m-%d %H:%M:%S'), base_url, False)
                )
                crawler_conn.commit()
                
                # Get the session ID for this crawl
                cursor.execute("SELECT last_insert_rowid()")
                session_id = cursor.fetchone()[0]
                self.log(f"Created new crawl session #{session_id}")
            
            # Create a crawl states table if it doesn't exist
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS crawl_state (
                    session_id INTEGER,
                    url TEXT,
                    status TEXT,
                    timestamp TIMESTAMP,
                    FOREIGN KEY (session_id) REFERENCES crawl_sessions(ID)
                )
            """)
            
            # Count URLs found by type for reporting
            urls_by_type = {
                "races": 0,
                "horses": 0,
                "jockeys": 0,
                "trainers": 0
            }
            
            # Track URLs already in database by type
            if resume_crawl:
                cursor.execute("SELECT Type, COUNT(*) FROM urls GROUP BY Type")
                for type_name, count in cursor.fetchall():
                    if type_name in urls_by_type:
                        urls_by_type[type_name] = count
            
            # Save URLs visited in this session to a file for future reference
            session_file = f"crawl_session_{time.strftime('%Y%m%d_%H%M%S')}.txt"
            with open(session_file, 'w') as f:
                f.write(f"Crawl session started at {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"Base URL: {base_url}\n")
                f.write(f"Resume: {resume_crawl}\n\n")
            
            # Improved URL patterns with more flexible matching
            url_patterns = {
                "races": re.compile(r'https?://www\.sportinglife\.com/racing/results/\d{4}-\d{2}-\d{2}/[\w-]+/\d+/[\w-]+'),
                "horses": re.compile(r'https?://www\.sportinglife\.com/racing/profiles/horse/\d+'),
                "jockeys": re.compile(r'https?://www\.sportinglife\.com/racing/profiles/jockey/\d+'),
                "trainers": re.compile(r'https?://www\.sportinglife\.com/racing/profiles/trainer/\d+')
            }
            
            # Additional pattern for profile links that might need special handling
            profile_pattern = re.compile(r'https?://www\.sportinglife\.com/racing/profiles/(horse|jockey|trainer)/\d+')
            
            # Define patterns for incomplete/relative URLs
            relative_patterns = {
                "jockeys": re.compile(r'/racing/profiles/jockey/\d+'),
                "trainers": re.compile(r'/racing/profiles/trainer/\d+'),
                "horses": re.compile(r'/racing/profiles/horse/\d+')
            }
            
            # Initialize statistics for saturation calculation
            total_links_found = 0
            relevant_links_found = 0
            
            # Create a session for better performance
            session = requests.Session()
            
            # Set headers to emulate a browser
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            
            # Process URLs until stop conditions are met
            while (to_visit and 
                   time.time() < end_time and 
                   urls_found < max_urls and
                   self.crawl_running):
                
                # Get next URL to process
                current_url = to_visit.pop(0)
                
                # Remove URL fragments (anything after #)
                if '#' in current_url:
                    current_url = current_url.split('#')[0]
                    self.log(f"Removed URL fragment: {current_url}")
                
                if current_url in visited:
                    continue
                
                # Add to visited set
                visited.add(current_url)
                
                # Log the URL to the session file
                with open(session_file, 'a') as f:
                    f.write(f"Visiting: {current_url}\n")
                
                # Update UI for current progress
                self.urls_found_var.set(f"URLs found: {urls_found}")
                type_counts = ", ".join([f"{k}: {v}" for k, v in urls_by_type.items()])
                self.log(f"URL count by type: {type_counts}")
                
                if total_links_found > 0:
                    saturation_rate = relevant_links_found / total_links_found
                    self.saturation_rate_var.set(f"Saturation: {saturation_rate*100:.1f}%")
                    
                    # Check saturation stop condition
                    if saturation_rate < saturation_limit and urls_found > 0:
                        self.log(f"Stopping due to low saturation rate: {saturation_rate*100:.1f}%")
                        break
                
                # Log current URL being processed
                self.log(f"Processing: {current_url}")
                
                try:
                    # Fetch the page
                    response = session.get(current_url, headers=headers, timeout=10)
                    response.raise_for_status()
                    
                    # Parse HTML
                    soup = BeautifulSoup(response.text, 'html.parser')
                    
                    # Check if this page is a profile page that we missed earlier
                    if '/profiles/jockey/' in current_url or '/profiles/trainer/' in current_url:
                        url_type = None
                        if '/profiles/jockey/' in current_url:
                            url_type = 'jockeys'
                            self.log(f"This is a jockey profile page: {current_url}")
                        elif '/profiles/trainer/' in current_url:
                            url_type = 'trainers'
                            self.log(f"This is a trainer profile page: {current_url}")
                            
                        if url_type:
                            # Check if this URL is already in the database
                            cursor.execute("SELECT ID FROM urls WHERE URL = ?", (current_url,))
                            if not cursor.fetchone():  # URL doesn't exist in the database
                                # Add to database with status='unprocessed'
                                cursor.execute(
                                    "INSERT INTO urls (URL, Date_accessed, status, Type) VALUES (?, ?, ?, ?)",
                                    (current_url, time.strftime('%Y-%m-%d %H:%M:%S'), 'unprocessed', url_type)
                                )
                                crawler_conn.commit()
                                urls_found += 1
                                urls_by_type[url_type] += 1
                                self.log(f"Added {url_type} profile page to database: {current_url}")
                    
                    # Special handling for horse profile pages - extract trainer and jockey links
                    if '/profiles/horse/' in current_url:
                        # Find the trainer link (often in a format like [B Haslam](/racing/profiles/trainer/435))
                        trainer_links = soup.select('a[href*="/racing/profiles/trainer/"]')
                        for trainer_link in trainer_links:
                            href = trainer_link['href']
                            if href.startswith('/'):
                                parsed_base = urlparse(base_url)
                                full_url = f"{parsed_base.scheme}://{parsed_base.netloc}{href}"
                                
                                # Add to database if not already there
                                cursor.execute("SELECT ID FROM urls WHERE URL = ?", (full_url,))
                                if not cursor.fetchone():
                                    cursor.execute(
                                        "INSERT INTO urls (URL, Date_accessed, status, Type) VALUES (?, ?, ?, ?)",
                                        (full_url, time.strftime('%Y-%m-%d %H:%M:%S'), 'unprocessed', 'trainers')
                                    )
                                    crawler_conn.commit()
                                    urls_found += 1
                                    urls_by_type['trainers'] += 1
                                    self.log(f"Found trainer link on horse page: {full_url}")
                                
                                # Add to visit queue if not already there
                                if full_url not in visited and full_url not in to_visit:
                                    to_visit.insert(0, full_url)
                                    self.log(f"Prioritized trainer page in visit queue: {full_url}")
                    
                    # Special handling for race result pages - extract all profile links
                    if '/racing/results/' in current_url:
                        # Find all profile links on the page
                        all_profile_links = soup.select('a[href*="/racing/profiles/"]')
                        for profile_link in all_profile_links:
                            href = profile_link['href']
                            if href.startswith('/'):
                                parsed_base = urlparse(base_url)
                                full_url = f"{parsed_base.scheme}://{parsed_base.netloc}{href}"
                                
                                # Determine the type
                                profile_type = None
                                if '/profiles/jockey/' in href:
                                    profile_type = 'jockeys'
                                elif '/profiles/trainer/' in href:
                                    profile_type = 'trainers'
                                elif '/profiles/horse/' in href:
                                    profile_type = 'horses'
                                
                                if profile_type:
                                    # Add to database if not already there
                                    cursor.execute("SELECT ID FROM urls WHERE URL = ?", (full_url,))
                                    if not cursor.fetchone():
                                        cursor.execute(
                                            "INSERT INTO urls (URL, Date_accessed, status, Type) VALUES (?, ?, ?, ?)",
                                            (full_url, time.strftime('%Y-%m-%d %H:%M:%S'), 'unprocessed', profile_type)
                                        )
                                        crawler_conn.commit()
                                        urls_found += 1
                                        urls_by_type[profile_type] += 1
                                        self.log(f"Found {profile_type} link on race page: {full_url}")
                                    
                                    # Add jockey and trainer links to visit queue with priority
                                    if profile_type in ('jockeys', 'trainers') and full_url not in visited and full_url not in to_visit:
                                        to_visit.insert(0, full_url)
                                        self.log(f"Prioritized {profile_type} page in visit queue: {full_url}")
                    
                    # Find all links
                    links = soup.find_all('a', href=True)
                    
                    # Count all links for saturation calculation
                    page_links = 0
                    page_relevant_links = 0
                    
                    for link in links:
                        href = link['href']
                        
                        # Remove URL fragments
                        if '#' in href:
                            href = href.split('#')[0]
                        
                        # Check for profile links even before converting to absolute URLs
                        is_profile = False
                        profile_type = None
                        for type_name, pattern in relative_patterns.items():
                            if pattern.match(href):
                                is_profile = True
                                profile_type = type_name
                                self.log(f"Found relative {type_name} link: {href}")
                                break
                        
                        # Convert relative URLs to absolute
                        if href.startswith('/'):
                            parsed_base = urlparse(base_url)
                            href = f"{parsed_base.scheme}://{parsed_base.netloc}{href}"
                        elif not href.startswith(('http://', 'https://')):
                            href = urljoin(current_url, href)
                        
                        # Skip external links or non-sportinglife links
                        if "sportinglife.com" not in href:
                            continue
                        
                        page_links += 1
                        
                        # Check if the URL matches any of our patterns
                        url_type = None
                        
                        # First check our main patterns
                        for type_name, pattern in url_patterns.items():
                            if pattern.match(href):
                                url_type = type_name
                                break
                        
                        # If we identified it as a profile link earlier, use that type
                        if not url_type and is_profile:
                            url_type = profile_type
                            self.log(f"Using profile type from relative pattern: {url_type} for {href}")
                        
                        # Special handling for profile pages
                        if not url_type and profile_pattern.match(href):
                            profile_match = profile_pattern.match(href)
                            entity_type = profile_match.group(1)  # Extract horse, jockey, or trainer
                            url_type = f"{entity_type}s"  # Convert to plural for our type system
                            self.log(f"Matched profile pattern: {href} as {url_type}")
                        
                        if url_type:
                            page_relevant_links += 1
                            
                            # Check if this URL is already in the database
                            cursor.execute("SELECT ID FROM urls WHERE URL = ?", (href,))
                            if not cursor.fetchone():  # URL doesn't exist in the database
                                # Add to database with status='unprocessed'
                                cursor.execute(
                                    "INSERT INTO urls (URL, Date_accessed, status, Type) VALUES (?, ?, ?, ?)",
                                    (href, time.strftime('%Y-%m-%d %H:%M:%S'), 'unprocessed', url_type)
                                )
                                crawler_conn.commit()
                                urls_found += 1
                                urls_by_type[url_type] += 1
                                
                                # Log newly found URL
                                self.log(f"Found {url_type}: {href}")
                        
                        # Prioritize profile links in the crawl queue
                        if href not in visited and href not in to_visit:
                            # For trainer/jockey profile pages, add them to the front of the queue
                            if '/profiles/jockey/' in href or '/profiles/trainer/' in href:
                                to_visit.insert(0, href)
                                self.log(f"Prioritized profile page in visit queue: {href}")
                            # Add results pages and other profile pages next
                            elif '/results/' in href or '/profiles/' in href:
                                to_visit.append(href)
                                self.log(f"Added to visit queue: {href}")
                            # For other pages, only add if they might be relevant
                            elif any(key in href for key in ['/racing/', '/horse/', '/jockey/', '/trainer/']):
                                to_visit.append(href)
                    
                    # Update saturation statistics
                    total_links_found += page_links
                    relevant_links_found += page_relevant_links
                    
                except Exception as e:
                    self.log(f"Error processing {current_url}: {e}")
            
            # Determine why we stopped
            elapsed_time = time.time() - start_time
            stop_reason = None
            
            if time.time() >= end_time:
                stop_reason = "timeout"
                self.log(f"Crawl completed due to timeout ({timeout_mins} mins)")
            elif urls_found >= max_urls:
                stop_reason = "max_urls_reached"
                self.log(f"Crawl completed after finding {urls_found} URLs (max: {max_urls})")
            elif not self.crawl_running:
                stop_reason = "user_stopped"
                self.log("Crawl was stopped by user")
            else:
                stop_reason = "completed"
                self.log(f"Crawl completed in {elapsed_time:.1f} seconds")
            
            self.log(f"Found {urls_found} new URLs")
            self.log(f"URLs by type: {', '.join([f'{k}: {v}' for k, v in urls_by_type.items()])}")
            self.log(f"Visited {len(visited)} pages")
            
            if total_links_found > 0:
                final_saturation = relevant_links_found / total_links_found
                self.log(f"Final saturation rate: {final_saturation*100:.1f}%")
            
            # Update the crawl session with final statistics
            if session_id:
                try:
                    cursor.execute(
                        """UPDATE crawl_sessions SET 
                           end_time = ?, 
                           urls_found = ?,
                           stop_reason = ?
                           WHERE ID = ?""",
                        (time.strftime('%Y-%m-%d %H:%M:%S'), urls_found, stop_reason, session_id)
                    )
                    crawler_conn.commit()
                    self.log(f"Updated crawl session #{session_id} with final statistics")
                except Exception as e:
                    self.log(f"Error updating crawl session: {e}")
            
            # Save crawl state for unprocessed URLs
            try:
                # Only save a subset of URLs to keep the state table manageable
                urls_to_save = to_visit[:500]
                for url in urls_to_save:
                    cursor.execute(
                        "INSERT INTO crawl_state (session_id, url, status, timestamp) VALUES (?, ?, ?, ?)",
                        (session_id, url, "unvisited", time.strftime('%Y-%m-%d %H:%M:%S'))
                    )
                crawler_conn.commit()
                self.log(f"Saved {len(urls_to_save)} URLs in crawl state for future resume")
            except Exception as e:
                self.log(f"Error saving crawl state: {e}")
            
            # Save final statistics to the session file
            with open(session_file, 'a') as f:
                f.write(f"\nCrawl session ended at {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"Duration: {elapsed_time:.1f} seconds\n")
                f.write(f"Stop reason: {stop_reason}\n")
                f.write(f"URLs found: {urls_found}\n")
                f.write(f"URLs by type: {', '.join([f'{k}: {v}' for k, v in urls_by_type.items()])}\n")
                f.write(f"Pages visited: {len(visited)}\n")
                if total_links_found > 0:
                    final_saturation = relevant_links_found / total_links_found
                    f.write(f"Final saturation rate: {final_saturation*100:.1f}%\n")
                f.write(f"Unprocessed URLs remaining: {len(to_visit)}\n")
            
            self.log(f"Crawl session details saved to {session_file}")
            
            # Close the crawler's database connection
            if crawler_conn:
                crawler_conn.close()
                self.log("Closed crawler thread database connection")
                
        except Exception as e:
            self.log(f"Crawl error: {e}")
        finally:
            self.crawl_running = False
    
    def create_scrape_frame(self):
        """Create the scrape section"""
        self.scrape_frame = ttk.LabelFrame(self.content_frame, text="Scrape")
        self.scrape_frame.grid(row=2, column=0, padx=5, pady=2, sticky="ew")
        
        # Make the frame expand horizontally
        self.scrape_frame.columnconfigure(0, weight=1)
        
        # Add a placeholder widget to make the frame visible
        ttk.Label(self.scrape_frame, text="Scrape section").pack(pady=5)
    
    def create_output_frame(self):
        """Create the output section"""
        self.output_frame = ttk.LabelFrame(self.content_frame, text="Output")
        self.output_frame.grid(row=3, column=0, padx=5, pady=(2, 0), sticky="ew")
        
        # Make the frame expand horizontally
        self.output_frame.columnconfigure(0, weight=1)
        self.output_frame.rowconfigure(0, weight=1)
        
        # Add a text area for output
        self.output_text = scrolledtext.ScrolledText(self.output_frame, height=10)
        self.output_text.grid(row=0, column=0, sticky="ew", padx=5, pady=5)
        self.output_text.insert(tk.END, "Output will appear here...")
    
    def log(self, message):
        """Log a message to the output text area"""
        # Schedule logging on the main thread to avoid threading issues
        self.root.after(0, self._log_on_main_thread, message)
    
    def _log_on_main_thread(self, message):
        """Actually perform the logging on the main thread"""
        self.output_text.insert(tk.END, f"\n{message}")
        self.output_text.see(tk.END)

def main():
    """Main function to run the application"""
    
    root = tk.Tk()
    app = ScraperUI(root)
    
    # This will block until the window is closed
    root.mainloop()

# Make sure this block executes properly
if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Error initializing application: {e}") 