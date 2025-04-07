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
    
    def run_crawler(self):
        """Run the web crawler"""
        base_url = self.base_url_var.get()
        timeout_mins = float(self.timeout_var.get())
        max_urls = int(self.max_urls_var.get())
        saturation_limit = float(self.saturation_var.get()) / 100.0  # Convert from percentage to decimal
        
        try:
            self.log(f"Starting crawl from {base_url}")
            self.log(f"Timeout: {timeout_mins} mins, Max URLs: {max_urls}, Saturation limit: {saturation_limit*100}%")
            
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
            to_visit = [base_url]
            
            # Count URLs found by type for reporting
            urls_by_type = {
                "races": 0,
                "horses": 0,
                "jockeys": 0,
                "trainers": 0
            }
            
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
                    
                    # Special handling for profile pages
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
                        # 1. Look for trainer information in the data table (more reliable)
                        trainer_found = False
                        
                        # Check the main horse info table, typically showing fields like Age, Trainer, Sex, etc.
                        info_tables = soup.select('table')
                        for table in info_tables:
                            rows = table.select('tr')
                            for row in rows:
                                # Check if this row contains trainer info
                                if row.text and 'Trainer' in row.text:
                                    # Look for links in this row
                                    trainer_links = row.select('a[href*="/racing/profiles/trainer/"]')
                                    for trainer_link in trainer_links:
                                        href = trainer_link['href']
                                        if href.startswith('/'):
                                            parsed_base = urlparse(base_url)
                                            full_url = f"{parsed_base.scheme}://{parsed_base.netloc}{href}"
                                            
                                            # Mark that we found a trainer
                                            trainer_found = True
                                            
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
                                                self.log(f"Found trainer link in horse info table: {full_url}")
                                            
                                            # Add to visit queue if not already there
                                            if full_url not in visited and full_url not in to_visit:
                                                to_visit.insert(0, full_url)
                                                self.log(f"Prioritized trainer page in visit queue: {full_url}")
                        
                        # 2. Fallback: more general search for trainer links if not found in the table
                        if not trainer_found:
                            # Look for trainer links anywhere on the page
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
                                        
                            # If still no trainer links found, try to look around the trainer label text
                            if not trainer_links:
                                # Look for elements containing "Trainer" text
                                trainer_elements = [el for el in soup.find_all(['td', 'th', 'div', 'span', 'p']) 
                                               if el.text and 'Trainer' in el.text]
                                
                                for element in trainer_elements:
                                    # Try to find nearby trainer links
                                    # Check parent
                                    parent = element.parent
                                    if parent:
                                        trainer_links = parent.select('a[href*="/racing/profiles/trainer/"]')
                                        # If not found, check adjacent siblings
                                        if not trainer_links and parent.find_next_sibling():
                                            trainer_links = parent.find_next_sibling().select('a[href*="/racing/profiles/trainer/"]')
                                        # Also check previous sibling
                                        if not trainer_links and parent.find_previous_sibling():
                                            trainer_links = parent.find_previous_sibling().select('a[href*="/racing/profiles/trainer/"]')
                                    
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
                                                self.log(f"Found trainer link near 'Trainer' text: {full_url}")
                                            
                                            # Add to visit queue if not already there
                                            if full_url not in visited and full_url not in to_visit:
                                                to_visit.insert(0, full_url)
                                                self.log(f"Prioritized trainer page in visit queue: {full_url}")
                        
                        # Also find jockey links on horse profile pages
                        jockey_links = soup.select('a[href*="/racing/profiles/jockey/"]')
                        for jockey_link in jockey_links:
                            href = jockey_link['href']
                            if href.startswith('/'):
                                parsed_base = urlparse(base_url)
                                full_url = f"{parsed_base.scheme}://{parsed_base.netloc}{href}"
                                
                                # Add to database if not already there
                                cursor.execute("SELECT ID FROM urls WHERE URL = ?", (full_url,))
                                if not cursor.fetchone():
                                    cursor.execute(
                                        "INSERT INTO urls (URL, Date_accessed, status, Type) VALUES (?, ?, ?, ?)",
                                        (full_url, time.strftime('%Y-%m-%d %H:%M:%S'), 'unprocessed', 'jockeys')
                                    )
                                    crawler_conn.commit()
                                    urls_found += 1
                                    urls_by_type['jockeys'] += 1
                                    self.log(f"Found jockey link on horse page: {full_url}")
                                
                                # Add to visit queue if not already there
                                if full_url not in visited and full_url not in to_visit:
                                    to_visit.insert(0, full_url)
                                    self.log(f"Prioritized jockey page in visit queue: {full_url}")
                    
                    # Enhanced handling for race result pages - extract all profile links
                    if '/racing/results/' in current_url:
                        # 1. First check the race table rows (which contain most of the profile links)
                        race_tables = soup.select('table')
                        for table in race_tables:
                            table_rows = table.select('tr')
                            for row in table_rows:
                                # Find all profile links in this table row
                                profile_links = row.select('a[href*="/racing/profiles/"]')
                                for profile_link in profile_links:
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
                                                self.log(f"Found {profile_type} link in race table: {full_url}")
                                            
                                            # Add to visit queue with priority for trainers and jockeys
                                            if full_url not in visited and full_url not in to_visit:
                                                if profile_type in ('jockeys', 'trainers'):
                                                    to_visit.insert(0, full_url)
                                                    self.log(f"Prioritized {profile_type} page in visit queue: {full_url}")
                                                else:
                                                    to_visit.append(full_url)
                        
                        # 2. Look for "My Stable" links (these often contain horse profile links)
                        my_stable_links = soup.select('a.my-stable-link, a[href*="my-stable"]')
                        for stable_link in my_stable_links:
                            # The actual profile link might be nearby or in a parent element
                            parent = stable_link.parent
                            # Look for profile links in this region of the DOM
                            region_profile_links = []
                            # Check in the parent element
                            parent_links = parent.select('a[href*="/racing/profiles/"]')
                            region_profile_links.extend(parent_links)
                            # Also check in siblings
                            if parent.find_previous_sibling():
                                prev_links = parent.find_previous_sibling().select('a[href*="/racing/profiles/"]')
                                region_profile_links.extend(prev_links)
                            if parent.find_next_sibling():
                                next_links = parent.find_next_sibling().select('a[href*="/racing/profiles/"]')
                                region_profile_links.extend(next_links)
                            
                            for profile_link in region_profile_links:
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
                                            self.log(f"Found {profile_type} link near 'My Stable': {full_url}")
                                        
                                        # Add profile page to visit queue
                                        if full_url not in visited and full_url not in to_visit:
                                            to_visit.append(full_url)
                        
                        # 3. Look for trainer and jockey information in the race details sections
                        race_details = soup.select('.race-details, .race-info, .result-details')
                        for section in race_details:
                            profile_links = section.select('a[href*="/racing/profiles/"]')
                            for profile_link in profile_links:
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
                                            self.log(f"Found {profile_type} link in race details: {full_url}")
                                        
                                        # Add to visit queue with priority
                                        if full_url not in visited and full_url not in to_visit:
                                            if profile_type in ('jockeys', 'trainers'):
                                                to_visit.insert(0, full_url)
                                            else:
                                                to_visit.append(full_url)
                        
                        # 4. Also do the general profile link search as before
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
                                    
                                    # Add to visit queue if not already there
                                    if full_url not in visited and full_url not in to_visit:
                                        to_visit.append(full_url)
                    
                    # General link discovery for all pages
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
            
            if time.time() >= end_time:
                self.log(f"Crawl completed due to timeout ({timeout_mins} mins)")
            elif urls_found >= max_urls:
                self.log(f"Crawl completed after finding {urls_found} URLs (max: {max_urls})")
            elif not self.crawl_running:
                self.log("Crawl was stopped by user")
            else:
                self.log(f"Crawl completed in {elapsed_time:.1f} seconds")
            
            self.log(f"Found {urls_found} new URLs")
            self.log(f"URLs by type: {', '.join([f'{k}: {v}' for k, v in urls_by_type.items()])}")
            self.log(f"Visited {len(visited)} pages")
            
            if total_links_found > 0:
                final_saturation = relevant_links_found / total_links_found
                self.log(f"Final saturation rate: {final_saturation*100:.1f}%")
            
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