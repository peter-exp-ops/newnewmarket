#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Collector.py - Tool for directly collecting and scraping racing data from sportinglife.com
"""

import requests
from bs4 import BeautifulSoup
import re
import sqlite3
import os
import pandas as pd
from datetime import datetime
import time
import random
import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext, filedialog, StringVar
import traceback
import platform
import threading

# HTTP Headers for requests
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1'
}

# === Database Connection Functions ===

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

def check_database_structure():
    """
    Check if the database has all required tables
    
    Returns:
        bool: True if all required tables exist
    """
    required_tables = ['races', 'horses', 'jockeys', 'trainers', 'racehorses', 'urls']
    
    try:
        conn = connect_to_database()
        cursor = conn.cursor()
        
        # Get all tables in the database
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [table[0] for table in cursor.fetchall()]
        
        # Check if all required tables exist
        all_tables_exist = all(table in tables for table in required_tables)
        
        # If 'urls' table doesn't exist, create it
        if 'urls' not in tables:
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS urls (
                url TEXT PRIMARY KEY,
                date_accessed TIMESTAMP,
                success BOOLEAN,
                type TEXT
            )
            ''')
            conn.commit()
        
        conn.close()
        return all_tables_exist
    except Exception as e:
        print(f"Error checking database structure: {e}")
        return False

def get_url_status(conn, url):
    """
    Check if a URL has already been processed
    
    Args:
        conn (sqlite3.Connection): Database connection
        url (str): URL to check
        
    Returns:
        str: Status of URL ('processed', 'unprocessed', 'failed', or None if not found)
    """
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT status FROM urls WHERE url = ?", (url,))
        result = cursor.fetchone()
        return result[0] if result else None
    except Exception as e:
        print(f"Error checking URL status: {str(e)}")
        return None

def save_urls_to_database(conn, urls, status="unprocessed"):
    """
    Save URLs to the database
    
    Args:
        conn (sqlite3.Connection): Database connection
        urls (list): URLs to save
        status (str): Status value (default: "unprocessed")
        
    Returns:
        int: Number of URLs saved
    """
    if not urls:
        return 0
        
    try:
        cursor = conn.cursor()
        
        # Create table if it doesn't exist
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS urls (
            url TEXT PRIMARY KEY,
            date_accessed TIMESTAMP,
            status TEXT DEFAULT "unprocessed",
            type TEXT
        )
        ''')
        
        # Get current timestamp
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Define URL patterns for each type with less restrictive matching
        url_patterns = {
            'jockeys': re.compile(r'/racing/profiles/jockey/'),
            'trainers': re.compile(r'/racing/profiles/trainer/'),
            'horses': re.compile(r'/racing/profiles/horse/'),
            'races': re.compile(r'/racing/results/\d{4}-\d{2}-\d{2}/.+/\d+/')  # Less strict race pattern
        }
        
        # Helper function to determine URL type
        def get_url_type(url):
            # Remove protocol and domain for matching
            url_path = url.replace('https://www.sportinglife.com', '')
            
            for type_name, pattern in url_patterns.items():
                if pattern.search(url_path):
                    return type_name
            return 'unknown'
        
        # Filter out date-only URLs that might have slipped through
        date_pattern = re.compile(r'/racing/results/\d{4}-\d{2}-\d{2}$')
        filtered_urls = [url for url in urls if not date_pattern.search(url.replace('https://www.sportinglife.com', ''))]
        
        if len(filtered_urls) < len(urls):
            print(f"Filtered out {len(urls) - len(filtered_urls)} date-only URLs")
        
        # Use executemany for better performance
        url_data = [(url, timestamp, status, get_url_type(url)) for url in filtered_urls]
        
        # Debug URL types being saved
        type_counts = {}
        for _, _, _, url_type in url_data:
            type_counts[url_type] = type_counts.get(url_type, 0) + 1
        print(f"URL types being saved: {type_counts}")
        
        cursor.executemany(
            "INSERT OR IGNORE INTO urls (url, date_accessed, status, type) VALUES (?, ?, ?, ?)",
            url_data
        )
        
        conn.commit()
        return cursor.rowcount
    except Exception as e:
        print(f"Error saving URLs to database: {str(e)}")
        return 0

def get_url_stats(conn):
    """
    Get statistics of URLs by type and status
    
    Args:
        conn (sqlite3.Connection): Database connection
        
    Returns:
        dict: Dictionary with URL statistics
    """
    try:
        cursor = conn.cursor()
        
        # Check if urls table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='urls'")
        if not cursor.fetchone():
            return {"error": "URLs table does not exist"}
        
        # Get counts by type and status
        cursor.execute("""
            SELECT 
                COALESCE(type, 'unknown') as type,
                COALESCE(status, 'unknown') as status,
                COUNT(*) as count
            FROM urls
            GROUP BY type, status
            ORDER BY type, status
        """)
        
        results = cursor.fetchall()
        
        # Process results into a structured format
        stats = {
            "types": set(),
            "statuses": set(),
            "counts": {},
            "totals": {"by_type": {}, "by_status": {}, "overall": 0}
        }
        
        # Define status mapping to standardize status names
        status_mapping = {
            "successful": "processed",
            "success": "processed",
            "completed": "processed",
            "error": "failed",
            "errors": "failed",
            "failure": "failed"
        }
        
        for row in results:
            type_name = row[0] 
            status = row[1].lower()  # Normalize to lowercase
            count = row[2]
            
            # Map status to standard names if needed
            if status in status_mapping:
                status = status_mapping[status]
            
            # Add to sets
            stats["types"].add(type_name)
            stats["statuses"].add(status)
            
            # Add counts
            if type_name not in stats["counts"]:
                stats["counts"][type_name] = {}
            
            # Add or increment count
            if status in stats["counts"][type_name]:
                stats["counts"][type_name][status] += count
            else:
                stats["counts"][type_name][status] = count
            
            # Update totals
            if type_name not in stats["totals"]["by_type"]:
                stats["totals"]["by_type"][type_name] = 0
            if status not in stats["totals"]["by_status"]:
                stats["totals"]["by_status"][status] = 0
                
            stats["totals"]["by_type"][type_name] += count
            stats["totals"]["by_status"][status] += count
            stats["totals"]["overall"] += count
        
        # Convert sets to sorted lists
        stats["types"] = sorted(list(stats["types"]))
        stats["statuses"] = sorted(list(stats["statuses"]))
        
        return stats
        
    except Exception as e:
        return {"error": str(e)}

# === Crawling Functions ===

def crawl_website(base_url, max_urls=1000, data_type=None, log_callback=None, progress_callback=None, conn=None, timeout_callback=None):
    """
    Crawl a website to find URLs matching a specific data type
    
    Args:
        base_url (str): Base URL to start crawling from
        max_urls (int): Maximum number of URLs to discover
        data_type (str): Type of data to look for ('races', 'horses', 'jockeys', 'trainers')
        log_callback (function): Callback function for logging
        progress_callback (function): Callback function for updating progress
        conn (sqlite3.Connection): Database connection
        timeout_callback (function): Callback function to check if timeout has been reached
        
    Returns:
        list: List of discovered URLs
    """
    discovered_urls = []
    visited_urls = set()
    urls_to_visit = [base_url]
    
    # Get already captured URLs from database
    captured_urls = {}
    if conn:
        cursor = conn.cursor()
        cursor.execute("SELECT url, success FROM urls")
        captured_urls = {row[0]: row[1] for row in cursor.fetchall()}
    
    # Log function (use callback if provided, otherwise print)
    def log(message):
        if log_callback:
            log_callback(message)
        else:
            print(message)
    
    # Update progress if callback provided
    def update_progress(current, total):
        if progress_callback:
            percent = min(100, int((current / total) * 100))
            progress_callback(percent)
    
    # Define URL patterns for each type - more strict patterns
    url_patterns = {
        'jockeys': re.compile(r'/racing/profiles/jockey/\d+$'),
        'trainers': re.compile(r'/racing/profiles/trainer/\d+$'),
        'horses': re.compile(r'/racing/profiles/horse/\d+$'),
        'races': re.compile(r'/racing/results/\d{4}-\d{2}-\d{2}/[^/]+/\d+/\d+$')  # Must include course and race ID
    }
    
    # Useful patterns that we follow but don't save
    date_pattern = re.compile(r'/racing/results/\d{4}-\d{2}-\d{2}$')
    
    # Add debugging
    log(f"Initial queue: {urls_to_visit}")
    log(f"Using patterns: {[p.pattern for p in url_patterns.values()]}")
    
    # Helper function to normalize URLs
    def normalize_url(url):
        # Ensure it's an absolute URL
        if url.startswith('/'):
            url = 'https://www.sportinglife.com' + url
        # Remove trailing slash if present
        if url.endswith('/'):
            url = url[:-1]
        return url
    
    # Helper function to determine URL type
    def get_url_type(url):
        for type_name, pattern in url_patterns.items():
            if pattern.search(url):
                return type_name
        return None
    
    # Log start of crawl
    log(f"Starting crawl from {base_url}")
    log(f"Skipping {len([u for u, s in captured_urls.items() if s == 1])} URLs already successfully captured")
    
    # Initialize counters for summary
    pages_checked = 0
    skipped_count = 0
    found_by_type = {'races': 0, 'jockeys': 0, 'trainers': 0, 'horses': 0}
    no_new_urls_count = 0
    consecutive_empty_count = 0
    
    # Process URLs in queue until max_urls discovered or queue empty
    while urls_to_visit and len(discovered_urls) < max_urls:
        # Get the next URL to visit
        current_url = urls_to_visit.pop(0)
        
        # Skip if already visited
        if current_url in visited_urls:
            continue
        
        # Skip if already successfully captured in database
        if current_url in captured_urls and captured_urls[current_url] == 1:
            skipped_count += 1
            continue
        
        # Mark as visited
        visited_urls.add(current_url)
        pages_checked += 1
        
        # Check for timeout
        if timeout_callback and timeout_callback():
            log("Timeout reached. Stopping crawl.")
            break
        
        # Update progress
        update_progress(len(discovered_urls), max_urls)
        
        # Log progress periodically
        if pages_checked % 20 == 0:
            log(f"Progress: Checked {pages_checked} pages, found {len(discovered_urls)} matching URLs " +
                f"(Races: {found_by_type['races']}, Jockeys: {found_by_type['jockeys']}, " +
                f"Trainers: {found_by_type['trainers']}, Horses: {found_by_type['horses']})")
        
        try:
            # Add small delay to avoid overloading the server
            time.sleep(random.uniform(0.2, 0.5))
            
            # Make request
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            response = requests.get(current_url, headers=headers, timeout=10)
            
            # Skip error pages
            if response.status_code != 200:
                continue
            
            # First, check if this URL matches any of our target types
            url_type = get_url_type(current_url)
            if url_type and current_url not in discovered_urls:
                discovered_urls.append(current_url)
                found_by_type[url_type] += 1
                
                # Save new URL to database immediately with success=0 (not processed)
                if conn:
                    try:
                        # Get current timestamp
                        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        cursor = conn.cursor()
                        
                        # Add to URLs table with success=0 (not scraped)
                        cursor.execute(
                            "INSERT OR IGNORE INTO urls (url, date_accessed, status, type) VALUES (?, ?, ?, ?)",
                            (current_url, timestamp, "unprocessed", url_type)
                        )
                        conn.commit()
                    except Exception as db_error:
                        log(f"Error saving URL to database: {str(db_error)}")
                
                # Log discovery
                if len(discovered_urls) % 10 == 0:
                    log(f"Found {len(discovered_urls)} matching URLs so far")
                
                # If we've reached max_urls, stop
                if len(discovered_urls) >= max_urls:
                    log(f"Reached maximum URL limit of {max_urls}")
                    break
            
            # Continue crawling by parsing HTML and finding more links
            soup = BeautifulSoup(response.text, 'html.parser')
            found_new_urls = False
            
            # Find all links
            for link in soup.find_all('a', href=True):
                href = link['href']
                
                # Convert relative URLs to absolute
                if href.startswith('/'):
                    href = "https://www.sportinglife.com" + href
                # Skip non-http links
                elif not (href.startswith('http://') or href.startswith('https://')):
                    continue
                # Skip external links
                elif 'sportinglife.com' not in href:
                    continue
                
                # Skip URLs already in discovered
                if href in discovered_urls:
                    continue
                
                # Skip URLs already successfully processed in database
                if href in captured_urls and captured_urls[href] == 1:
                    continue
                
                # Skip URLs already in visit queue
                if href in urls_to_visit or href in visited_urls:
                    continue
                
                # Check if URL matches any of our patterns
                link_type = get_url_type(href)
                if link_type:
                    if href not in urls_to_visit:
                        urls_to_visit.append(href)
                        found_new_urls = True
                
                # Special handling for race dates (add these with high priority)
                elif '/racing/results/' in href and re.search(r'/\d{4}-\d{2}-\d{2}', href):
                    if href not in urls_to_visit and href not in visited_urls:
                        # Insert near the beginning for priority (but not at index 0)
                        insert_pos = min(10, len(urls_to_visit))
                        urls_to_visit.insert(insert_pos, href)
                        found_new_urls = True
                
                # Add other sportinglife.com racing URLs with lower priority
                elif '/racing/' in href and href not in urls_to_visit and href not in visited_urls:
                    urls_to_visit.append(href)
                    found_new_urls = True
            
            # Track consecutive pages with no new URLs (to detect when we're not making progress)
            if not found_new_urls:
                consecutive_empty_count += 1
                if consecutive_empty_count >= 50:
                    log("No new URLs found in 50 consecutive pages. Stopping crawl.")
                    break
            else:
                consecutive_empty_count = 0
                
        except requests.exceptions.RequestException as e:
            # Handle request exceptions gracefully
            if "timeout" in str(e).lower():
                log(f"Request timeout for {current_url}")
            elif "connection" in str(e).lower():
                log(f"Connection error for {current_url}")
            else:
                log(f"Request error for {current_url}: {str(e)}")
        except Exception as e:
            log(f"Error processing URL {current_url}: {str(e)}")
    
    # Update progress to 100% when done
    update_progress(100, 100)
    
    # Log final summary
    log(f"Crawl complete. Found {len(discovered_urls)} matching URLs after checking {pages_checked} pages")
    log(f"Races: {found_by_type['races']}, Jockeys: {found_by_type['jockeys']}, Trainers: {found_by_type['trainers']}, Horses: {found_by_type['horses']}")
    log(f"Skipped {skipped_count} URLs already successfully captured")
    
    # Save discovered URLs to database if not already saved above
    if conn and discovered_urls:
        try:
            # Create URLs table if it doesn't exist
            cursor = conn.cursor()
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS urls (
                url TEXT PRIMARY KEY,
                date_accessed TIMESTAMP,
                status TEXT DEFAULT "unprocessed",
                type TEXT
            )
            ''')
            
            # Get current timestamp
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # Prepare batch of values for insertion
            url_data = []
            for url in discovered_urls:
                url_type = get_url_type(url)
                url_data.append((url, timestamp, "unprocessed", url_type))
            
            # Insert URLs in batches to improve performance
            cursor.executemany(
                "INSERT OR IGNORE INTO urls (url, date_accessed, status, type) VALUES (?, ?, ?, ?)",
                url_data
            )
            conn.commit()
            
            log(f"All discovered URLs added to database")
        except Exception as e:
            log(f"Error batch saving URLs to database: {str(e)}")
    
    return discovered_urls

def filter_urls_by_type(urls, url_type):
    """
    Filter URLs by data type
    
    Args:
        urls (list): List of URLs
        url_type (str): Type of data ('races', 'horses', 'jockeys', 'trainers')
        
    Returns:
        list: Filtered list of URLs
    """
    # URL patterns to match for different types
    url_patterns = {
        'jockeys': r'https?://www\.sportinglife\.com/racing/profiles/jockey/\d+',
        'trainers': r'https?://www\.sportinglife\.com/racing/profiles/trainer/\d+',
        'races': r'https?://www\.sportinglife\.com/racing/results/\d{4}-\d{2}-\d{2}/[\w-]+/\d+/[\w-]+',
        'horses': r'https?://www\.sportinglife\.com/racing/profiles/horse/\d+'
    }
    
    # If we want all types, return all URLs that match any pattern
    if url_type == 'all':
        filtered_urls = []
        for url in urls:
            if any(re.match(pattern, url) for pattern in url_patterns.values()):
                filtered_urls.append(url)
        return filtered_urls
    
    # Get pattern for the specified type
    pattern = url_patterns.get(url_type)
    if not pattern:
        return []  # Invalid type
    
    # Return URLs that match the pattern
    return [url for url in urls if re.match(pattern, url)]

# === Scraping Functions ===

def scrape_race_page(url):
    """
    Scrape a race page to extract race information
    
    Args:
        url (str): URL of race page
        
    Returns:
        dict: Extracted race data or None if failed
    """
    # Function implementation will go here
    pass

def scrape_horse_page(url):
    """
    Scrape a horse page to extract horse information
    
    Args:
        url (str): URL of horse page
        
    Returns:
        dict: Extracted horse data or None if failed
    """
    # Function implementation will go here
    pass

def scrape_jockey_page(url):
    """
    Scrape a jockey page to extract jockey information
    
    Args:
        url (str): URL of jockey page
        
    Returns:
        dict: Extracted jockey data or None if failed
    """
    # Function implementation will go here
    pass

def scrape_trainer_page(url):
    """
    Scrape a trainer page to extract trainer information
    
    Args:
        url (str): URL of trainer page
        
    Returns:
        dict: Extracted trainer data or None if failed
    """
    # Function implementation will go here
    pass

def scrape_urls_by_type(urls, url_type, limit, conn=None, log_callback=None, progress_callback=None):
    """
    Placeholder for URL scraping function - does not implement actual scraping
    
    Args:
        urls (list): List of URLs to scrape
        url_type (str): Type of data ('races', 'horses', 'jockeys', 'trainers')
        limit (int): Maximum number of URLs to scrape
        conn (sqlite3.Connection): Database connection
        log_callback (function): Callback function for logging
        progress_callback (function): Callback function for updating progress
        
    Returns:
        list: Empty list (no scraping implemented)
    """
    if log_callback:
        log_callback("Scraping not implemented as requested")
    
    # Update progress to 100% to indicate completion
    if progress_callback:
        progress_callback(len(urls), len(urls))
    
    # Mark URLs as processed in database (success = 1)
    if conn:
        try:
            cursor = conn.cursor()
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            for i, url in enumerate(urls):
                # Update URL status to processed
                cursor.execute(
                    "UPDATE urls SET date_accessed = ?, status = 'processed' WHERE url = ?",
                    (timestamp, url)
                )
                
                # Update progress every few URLs
                if progress_callback and i % 5 == 0:
                    progress_callback(i + 1, len(urls))
            
            conn.commit()
            
            if log_callback:
                log_callback(f"Marked {len(urls)} URLs as processed in the database")
        except Exception as e:
            if log_callback:
                log_callback(f"Error updating URL status: {str(e)}")
    
    return []  # Return empty list as no actual scraping is performed

def process_scraped_data(conn, data, data_type):
    """
    Placeholder for data processing function - does not implement actual processing
    
    Args:
        conn (sqlite3.Connection): Database connection
        data (list): Scraped data (empty in this implementation)
        data_type (str): Type of data ('races', 'horses', 'jockeys', 'trainers')
    """
    self.log(f"No data processing implemented as requested")
    return

def save_data_to_database(conn, data, data_type):
    """
    Save processed data to the database
    
    Args:
        conn (sqlite3.Connection): Database connection
        data (list): Processed data
        data_type (str): Type of data ('races', 'horses', 'jockeys', 'trainers')
        
    Returns:
        int: Number of records saved
    """
    # Function implementation will go here
    pass

# === UI Class ===

class CollectorUI:
    """UI for controlling the data collection and scraping process"""
    
    def __init__(self, root):
        """Initialize the UI"""
        self.root = root
        self.root.title("Newmarket - Collector")
        self.root.geometry("900x700")
        
        # Set application icon
        try:
            icon_path = "Icon 32px.png"
            if os.path.exists(icon_path):
                # Set taskbar icon for Windows
                self.root.iconbitmap(icon_path)
        except Exception as e:
            print(f"Could not set application icon: {e}")
        
        self.conn = None
        self.discovered_urls = []
        self.setup_ui()
    
    def setup_ui(self):
        """Set up the UI components"""
        # Create a canvas with scrollbar for the main content
        self.main_canvas = tk.Canvas(self.root)
        self.main_canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        
        # Add a scrollbar to the canvas
        self.scrollbar = ttk.Scrollbar(self.root, orient=tk.VERTICAL, command=self.main_canvas.yview)
        self.scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        # Configure the canvas to use the scrollbar
        self.main_canvas.configure(yscrollcommand=self.scrollbar.set)
        self.main_canvas.bind('<Configure>', lambda e: self.main_canvas.configure(scrollregion=self.main_canvas.bbox("all")))
        
        # Create the main frame inside the canvas
        main_frame = ttk.Frame(self.main_canvas, padding="10")
        self.main_canvas.create_window((0, 0), window=main_frame, anchor="nw")
        
        # Add cross-platform mouse wheel scrolling
        def _on_mousewheel(event):
            if platform.system() == 'Windows':
                self.main_canvas.yview_scroll(int(-1*(event.delta/120)), "units")
            elif platform.system() == 'Darwin':  # macOS
                self.main_canvas.yview_scroll(int(-1*event.delta), "units")
        
        self.main_canvas.bind_all("<MouseWheel>", _on_mousewheel)
        self.main_canvas.bind_all("<Button-4>", lambda e: self.main_canvas.yview_scroll(-1, "units"))
        self.main_canvas.bind_all("<Button-5>", lambda e: self.main_canvas.yview_scroll(1, "units"))
        
        # 1. Database section (renamed)
        conn_frame = ttk.LabelFrame(main_frame, text="Database", padding="10")
        conn_frame.pack(fill=tk.X, pady=5)
        
        # Top row with connection controls
        self.status_var = StringVar(value="Not Connected")
        ttk.Label(conn_frame, text="Status:").grid(row=0, column=0, sticky=tk.W, padx=5, pady=5)
        ttk.Label(conn_frame, textvariable=self.status_var).grid(row=0, column=1, sticky=tk.W, padx=5, pady=5)
        ttk.Button(conn_frame, text="Connect to Database", command=self.connect_to_db).grid(row=0, column=2, padx=5, pady=5)
        
        # URL Statistics table
        ttk.Label(conn_frame, text="URL Statistics:", font=("", 10, "bold")).grid(row=1, column=0, sticky=tk.W, padx=5, pady=(10, 5))
        
        # Create a frame for the table
        self.stats_frame = ttk.Frame(conn_frame)
        self.stats_frame.grid(row=2, column=0, columnspan=3, padx=5, pady=5, sticky=tk.W)
        
        # Placeholder for the table (will be populated by update_url_stats)
        self.url_stats_table = None
        
        # Add refresh button
        ttk.Button(conn_frame, text="Refresh Stats", command=self.update_url_stats).grid(row=3, column=0, sticky=tk.W, padx=5, pady=5)
        
        # 2. Crawler section (renamed and simplified)
        crawler_frame = ttk.LabelFrame(main_frame, text="Crawl", padding="10")
        crawler_frame.pack(fill=tk.X, pady=5)
        
        ttk.Label(crawler_frame, text="URL:").grid(row=0, column=0, sticky=tk.W, padx=5, pady=5)
        self.base_url_var = StringVar(value="https://www.sportinglife.com/racing/results/")
        ttk.Entry(crawler_frame, textvariable=self.base_url_var, width=70).grid(row=0, column=1, columnspan=2, padx=5, pady=5)
                
        # Add timeout control
        ttk.Label(crawler_frame, text="Timeout (mins):").grid(row=1, column=0, sticky=tk.W, padx=5, pady=5)
        self.timeout_var = tk.IntVar(value=10)
        timeout_spinner = ttk.Spinbox(crawler_frame, from_=1, to=60, increment=1, textvariable=self.timeout_var, width=5)
        timeout_spinner.grid(row=1, column=1, sticky=tk.W, padx=5, pady=5)
        
        # Add max URLs control
        ttk.Label(crawler_frame, text="Max URLs:").grid(row=2, column=0, sticky=tk.W, padx=5, pady=5)
        self.max_urls_var = tk.IntVar(value=100)
        max_urls_spinner = ttk.Spinbox(crawler_frame, from_=10, to=10000, increment=10, textvariable=self.max_urls_var, width=5)
        max_urls_spinner.grid(row=2, column=1, sticky=tk.W, padx=5, pady=5)
        
        # Add saturation threshold control
        ttk.Label(crawler_frame, text="Saturation Threshold (%):").grid(row=3, column=0, sticky=tk.W, padx=5, pady=5)
        self.saturation_var = tk.DoubleVar(value=5.0)
        saturation_spinner = ttk.Spinbox(crawler_frame, from_=0.1, to=20.0, increment=0.5, textvariable=self.saturation_var, width=5)
        saturation_spinner.grid(row=3, column=1, sticky=tk.W, padx=5, pady=5)
        
        # Add window size control for saturation detection
        ttk.Label(crawler_frame, text="Window Size:").grid(row=4, column=0, sticky=tk.W, padx=5, pady=5)
        self.window_size_var = tk.IntVar(value=1000)
        window_spinner = ttk.Spinbox(crawler_frame, from_=100, to=5000, increment=100, textvariable=self.window_size_var, width=5)
        window_spinner.grid(row=4, column=1, sticky=tk.W, padx=5, pady=5)
        
        ttk.Button(crawler_frame, text="Crawl Website", command=self.crawl_website).grid(row=5, column=0, padx=5, pady=5)
        
        self.crawl_stats_var = StringVar(value="Not started")
        ttk.Label(crawler_frame, textvariable=self.crawl_stats_var).grid(row=5, column=1, sticky=tk.W, padx=5, pady=5)
        
        # 3. Scraper section
        scraper_frame = ttk.LabelFrame(main_frame, text="Scrape", padding="10")
        scraper_frame.pack(fill=tk.X, pady=5)
        
        # Add data type selection
        ttk.Label(scraper_frame, text="Data Type:").grid(row=0, column=0, sticky=tk.W, padx=5, pady=5)
        self.scrape_type_var = StringVar(value="all")
        scrape_type_combo = ttk.Combobox(scraper_frame, textvariable=self.scrape_type_var, width=10, 
                                        values=["all", "races", "jockeys", "trainers", "horses"], 
                                        state="readonly")
        scrape_type_combo.grid(row=0, column=1, sticky=tk.W, padx=5, pady=5)
        
        # Add limit control
        ttk.Label(scraper_frame, text="URL Limit:").grid(row=1, column=0, sticky=tk.W, padx=5, pady=5)
        self.url_limit_var = tk.IntVar(value=50)
        url_limit_spinner = ttk.Spinbox(scraper_frame, from_=1, to=1000, increment=10, textvariable=self.url_limit_var, width=5)
        url_limit_spinner.grid(row=1, column=1, sticky=tk.W, padx=5, pady=5)
        
        # Add success filter
        ttk.Label(scraper_frame, text="Status:").grid(row=2, column=0, sticky=tk.W, padx=5, pady=5)
        self.url_status_var = StringVar(value="unprocessed")
        status_combo = ttk.Combobox(scraper_frame, textvariable=self.url_status_var, width=10, 
                                   values=["unprocessed", "all", "failed", "successful"], 
                                   state="readonly")
        status_combo.grid(row=2, column=1, sticky=tk.W, padx=5, pady=5)
        
        ttk.Button(scraper_frame, text="Scrape URLs", command=self.scrape_urls).grid(row=3, column=0, padx=5, pady=5)
        
        self.scrape_stats_var = StringVar(value="Not started")
        ttk.Label(scraper_frame, textvariable=self.scrape_stats_var).grid(row=3, column=1, sticky=tk.W, padx=5, pady=5)
        
        # 4. Progress section
        progress_frame = ttk.LabelFrame(main_frame, text="Progress", padding="10")
        progress_frame.pack(fill=tk.X, pady=5)
        
        self.progress_var = tk.DoubleVar()
        self.progress_bar = ttk.Progressbar(progress_frame, orient=tk.HORIZONTAL, length=100, 
                                            mode='determinate', variable=self.progress_var)
        self.progress_bar.pack(fill=tk.X, padx=5, pady=5)
        
        self.stats_var = StringVar(value="Ready")
        ttk.Label(progress_frame, textvariable=self.stats_var).pack(anchor=tk.W, padx=5)
        
        # 5. Log section
        log_frame = ttk.LabelFrame(main_frame, text="Log", padding="10")
        log_frame.pack(fill=tk.BOTH, expand=True, pady=5)
        
        self.log_text = scrolledtext.ScrolledText(log_frame, height=15)
        self.log_text.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # Control buttons
        control_frame = ttk.Frame(main_frame)
        control_frame.pack(fill=tk.X, pady=10)
        
        ttk.Button(control_frame, text="Clear Log", 
                  command=lambda: self.log_text.delete(1.0, tk.END)).pack(side=tk.RIGHT, padx=5)
        
        ttk.Button(control_frame, text="Exit", 
                  command=self.root.destroy).pack(side=tk.RIGHT, padx=5)
    
    def connect_to_db(self):
        """Connect to the database"""
        try:
            if self.conn is not None:
                self.conn.close()
                
            self.conn = self.connect_to_database()
            
            # Check if the database has all required tables
            if self.check_database_structure():
                self.status_var.set("Connected")
                self.log("Successfully connected to the database.")
                # Update URL statistics
                self.update_url_stats()
            else:
                self.status_var.set("Connected (incomplete schema)")
                self.log("Connected to database but schema is incomplete.")
                # Still try to update URL statistics
                self.update_url_stats()
        except Exception as e:
            self.status_var.set("Connection Failed")
            self.log(f"Failed to connect to database: {str(e)}")
            messagebox.showerror("Connection Error", f"Failed to connect to database: {str(e)}")
    
    def check_database_structure(self):
        """
        Check if the database has all required tables
        
        Returns:
            bool: True if all required tables exist
        """
        required_tables = ['races', 'horses', 'jockeys', 'trainers', 'racehorses', 'urls']
        
        try:
            if not self.conn:
                return False
                
            cursor = self.conn.cursor()
            
            # Get all tables in the database
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [table[0] for table in cursor.fetchall()]
            
            # Check if all required tables exist
            all_tables_exist = all(table in tables for table in required_tables)
            
            # If 'urls' table doesn't exist, create it
            if 'urls' not in tables:
                self.log("Creating 'urls' table...")
                cursor.execute('''
                CREATE TABLE IF NOT EXISTS urls (
                    url TEXT PRIMARY KEY,
                    date_accessed TIMESTAMP,
                    status TEXT DEFAULT "unprocessed",
                    type TEXT
                )
                ''')
                self.conn.commit()
            
            return all_tables_exist
        except Exception as e:
            self.log(f"Error checking database structure: {e}")
            return False
    
    def connect_to_database(self, db_path="racing_data.db"):
        """
        Establish connection to the SQLite database
        
        Args:
            db_path (str): Path to the SQLite database file
            
        Returns:
            sqlite3.Connection: Connection object
        """
        try:
            conn = sqlite3.connect(db_path)
            # Enable foreign keys
            conn.execute("PRAGMA foreign_keys = ON")
            return conn
        except Exception as e:
            self.log(f"Error connecting to database: {str(e)}")
            return None
    
    def crawl_website(self):
        """
        Crawl the website to find URLs of races, jockeys, trainers, and horses.
        URLs are saved to the database with status="unprocessed".
        """
        base_url = self.base_url_var.get()
        max_urls = self.max_urls_var.get()
        timeout_mins = self.timeout_var.get()
        saturation_threshold = self.saturation_var.get()
        window_size = self.window_size_var.get()
        resume_crawl = True  # Add option to resume if a saved state exists
        
        def run_crawler():
            try:
                # Initialize log and variables
                log_message = f"Starting crawl of {base_url} for all URL types with timeout {timeout_mins} mins, max {max_urls} URLs..."
                self.log(log_message)
                
                # Update UI
                self.crawl_stats_var.set("Crawling in progress...")
                
                # Set timeout
                timeout_seconds = timeout_mins * 60
                start_time = time.time()
                
                # Timeout callback for crawl function
                def timeout_reached():
                    elapsed_time = time.time() - start_time
                    return elapsed_time > timeout_seconds
                
                conn = self.connect_to_database()
                if not conn:
                    self.log("Failed to connect to database.")
                    return
                
                self.check_database_structure()
                
                # Get existing URLs from database to avoid duplicates
                cursor = conn.cursor()
                cursor.execute("SELECT url, status FROM urls")
                existing_urls = {row[0]: row[1] for row in cursor.fetchall()}
                
                # Try to load saved state if resume is enabled
                crawl_state = {}
                if resume_crawl:
                    crawl_state = load_crawl_state()
                    if crawl_state:
                        self.log(f"Loaded saved crawl state with {len(crawl_state.get('visited_urls', []))} visited and {len(crawl_state.get('urls_to_visit', []))} pending URLs")
                
                # Initialize or resume crawl state
                visited_urls = crawl_state.get('visited_urls', set())
                discovered_urls = []  # We'll track these as we go
                urls_to_visit = crawl_state.get('urls_to_visit', [base_url])
                
                if not urls_to_visit:
                    urls_to_visit = [base_url]  # Ensure we have a starting point
                
                # Define URL patterns for each type
                url_patterns = {
                    'jockeys': re.compile(r'/racing/profiles/jockey/'),
                    'trainers': re.compile(r'/racing/profiles/trainer/'),
                    'horses': re.compile(r'/racing/profiles/horse/'),
                    'races': re.compile(r'/racing/results/\d{4}-\d{2}-\d{2}/.+/\d+/')  # Less strict race pattern
                }
                
                # Useful patterns that we follow but don't save
                date_pattern = re.compile(r'/racing/results/\d{4}-\d{2}-\d{2}$')
                
                # Add debugging
                self.log(f"Initial queue: {urls_to_visit[:5]}... (total: {len(urls_to_visit)})")
                self.log(f"Using patterns: {[p.pattern for p in url_patterns.values()]}")
                
                # Helper function to normalize URLs
                def normalize_url(url):
                    # Ensure it's an absolute URL
                    if url.startswith('/'):
                        url = 'https://www.sportinglife.com' + url
                    # Remove trailing slash if present
                    if url.endswith('/'):
                        url = url[:-1]
                    return url
                
                # Helper function to determine URL type
                def get_url_type(url):
                    for type_name, pattern in url_patterns.items():
                        if pattern.search(url):
                            return type_name
                    return None
                
                # Helper function to process discovered URL
                def process_url(href, source_url, url_type=None):
                    if not href or href == '#':
                        return False
                        
                    # Normalize URL
                    href = normalize_url(href)
                    
                    # Skip if not a Sporting Life URL
                    if not href.startswith('https://www.sportinglife.com/'):
                        return False
                    
                    # Detect URL type if not provided
                    if url_type is None:
                        url_type = get_url_type(href)
                        
                    # Skip if no match to target patterns
                    if url_type is None:
                        return False
                        
                    # Skip if already processed or queued
                    if href in discovered_urls or href in visited_urls:
                        return False
                    
                    # Skip if already in database with processed status
                    if href in existing_urls and existing_urls[href] == 'processed':
                        return False
                    
                    # Add to discovered URLs and queue
                    discovered_urls.append(href)
                    urls_to_visit.append(href)
                    
                    # Save to database
                    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    try:
                        cursor.execute(
                            "INSERT OR IGNORE INTO urls (url, date_accessed, status, type) VALUES (?, ?, ?, ?)",
                            (href, timestamp, "unprocessed", url_type)
                        )
                        conn.commit()
                    except Exception as db_error:
                        self.log(f"Error saving URL to database: {str(db_error)}")
                    
                    return True
                
                # Variables for detecting saturation
                discovery_window = []  # Track new URLs discovered in the window
                discovery_threshold = saturation_threshold / 100
                save_interval = 100  # Save state every 100 pages
                
                # Main crawling loop
                pages_visited = 0
                
                while urls_to_visit and len(discovered_urls) < max_urls:
                    # Check for timeout
                    if timeout_reached():
                        self.log("Timeout reached. Saving state and stopping crawl.")
                        break
                    
                    # Get next URL
                    current_url = urls_to_visit.pop(0)
                    
                    # Skip if already visited
                    if current_url in visited_urls:
                        continue
                    
                    # Add to visited
                    visited_urls.add(current_url)
                    pages_visited += 1
                    
                    # Save state periodically
                    if pages_visited % save_interval == 0:
                        state = {
                            'visited_urls': visited_urls,
                            'urls_to_visit': urls_to_visit,
                            'last_url': current_url,
                            'pages_visited': pages_visited,
                            'discovered_count': len(discovered_urls),
                            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        }
                        if save_crawl_state(state):
                            self.log(f"Saved crawl state at {pages_visited} pages visited")
                    
                    # Update progress
                    progress_pct = min(100, int((len(discovered_urls) / max_urls) * 100))
                    self.progress_var.set(progress_pct)
                    
                    # Update UI periodically
                    if pages_visited % 20 == 0 or len(discovered_urls) % 100 == 0:
                        elapsed = time.time() - start_time
                        
                        # Calculate current saturation rate if we have enough data
                        current_saturation = 0.0
                        if len(discovery_window) > 0:
                            current_saturation = (sum(discovery_window) / len(discovery_window)) * 100
                            
                        stats_msg = (f"Visited: {pages_visited} pages, Found: {len(discovered_urls)} URLs, "
                                    f"Queue: {len(urls_to_visit)}, Saturation: {current_saturation:.2f}%")
                        self.crawl_stats_var.set(stats_msg)
                        self.root.update()
                        
                        # Log progress
                        self.log(stats_msg)
                    
                    # Add small delay to avoid overloading server
                    time.sleep(random.uniform(0.3, 0.7))
                    
                    # Try to fetch the page
                    try:
                        pre_discovery_count = len(discovered_urls)
                        
                        headers = {
                            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
                        }
                        response = requests.get(current_url, headers=headers, timeout=10)
                        
                        if response.status_code != 200:
                            self.log(f"Failed to fetch {current_url}: {response.status_code}")
                            continue
                        
                        # Parse the HTML
                        soup = BeautifulSoup(response.text, 'html.parser')
                        
                        # Check if this is a race page and process entity links
                        is_race_page = '/racing/results/' in current_url and get_url_type(current_url) == 'races'
                        if is_race_page:
                            # Find all horse links on race page
                            horse_links = soup.find_all('a', href=lambda href: href and '/racing/profiles/horse/' in href)
                            horse_count = 0
                            for link in horse_links:
                                href = link.get('href')
                                if process_url(href, current_url, 'horses'):
                                    horse_count += 1
                            
                            # Find all jockey links
                            jockey_links = soup.find_all('a', href=lambda href: href and '/racing/profiles/jockey/' in href)
                            jockey_count = 0
                            for link in jockey_links:
                                href = link.get('href')
                                if process_url(href, current_url, 'jockeys'):
                                    jockey_count += 1
                            
                            # Find all trainer links
                            trainer_links = soup.find_all('a', href=lambda href: href and '/racing/profiles/trainer/' in href)
                            trainer_count = 0
                            for link in trainer_links:
                                href = link.get('href')
                                if process_url(href, current_url, 'trainers'):
                                    trainer_count += 1
                            
                            self.log(f"Race page {current_url}: found {horse_count} horses, {jockey_count} jockeys, {trainer_count} trainers")
                        
                        # Find all links on the page
                        for link in soup.find_all('a', href=True):
                            href = link.get('href')
                            process_url(href, current_url)
                        
                        # Track discovery rate for saturation detection
                        new_discoveries = len(discovered_urls) - pre_discovery_count
                        discovery_window.append(new_discoveries)
                        
                        # Keep window at fixed size
                        if len(discovery_window) > window_size:
                            discovery_window.pop(0)
                        
                        # Check for saturation when window is full
                        if len(discovery_window) >= window_size:
                            avg_discovery_rate = sum(discovery_window) / window_size
                            current_saturation_pct = avg_discovery_rate * 100
                            
                            # Log saturation status periodically
                            if pages_visited % 200 == 0:
                                self.log(f"Current saturation rate: {current_saturation_pct:.2f}% (threshold: {saturation_threshold:.2f}%)")
                            
                            # Check if we've reached saturation threshold
                            if avg_discovery_rate < discovery_threshold and pages_visited > window_size * 2:
                                self.log(f"CRAWL SATURATION DETECTED: Current rate {current_saturation_pct:.2f}% is below threshold {saturation_threshold:.2f}%")
                                self.log(f"Stopping crawl after visiting {pages_visited} pages and discovering {len(discovered_urls)} URLs")
                                
                                # Update UI to show saturation was reached
                                self.crawl_stats_var.set(f"SATURATION REACHED at {current_saturation_pct:.2f}% - Found {len(discovered_urls)} URLs")
                                break
                    
                    except requests.RequestException as e:
                        self.log(f"Error fetching {current_url}: {str(e)}")
                    except Exception as e:
                        self.log(f"Error processing {current_url}: {str(e)}")
                
                # Save final state
                final_state = {
                    'visited_urls': visited_urls,
                    'urls_to_visit': urls_to_visit,
                    'pages_visited': pages_visited,
                    'discovered_count': len(discovered_urls),
                    'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'completed': True
                }
                save_crawl_state(final_state)
                
                # Update URL statistics
                self.update_url_stats()
                
                # Final statistics
                elapsed = time.time() - start_time
                self.log(f"Crawl completed in {elapsed:.1f} seconds")
                self.log(f"Visited {pages_visited} pages")
                self.log(f"Discovered {len(discovered_urls)} URLs")
                
                # Calculate final saturation rate
                final_saturation = 0.0
                if len(discovery_window) > 0:
                    final_saturation = (sum(discovery_window) / len(discovery_window)) * 100
                    self.log(f"Final saturation rate: {final_saturation:.2f}% (threshold: {saturation_threshold:.2f}%)")
                
                type_counts = {}
                cursor.execute("SELECT type, COUNT(*) FROM urls GROUP BY type")
                for row in cursor.fetchall():
                    type_counts[row[0]] = row[1]
                
                self.log(f"URL counts by type: {type_counts}")
                
                # Set progress to 100%
                self.progress_var.set(100)
                
                # Show completion message with saturation info
                if not self.crawl_stats_var.get().startswith("SATURATION REACHED"):
                    reason = ""
                    if len(discovered_urls) >= max_urls:
                        reason = "MAX URLS REACHED"
                    elif timeout_reached():
                        reason = "TIMEOUT REACHED"
                    else:
                        reason = "NO MORE URLS"
                        
                    self.crawl_stats_var.set(f"{reason} - Found {len(discovered_urls)} URLs, Saturation: {final_saturation:.2f}%")
                
                # Close database connection
                conn.close()
                
            except Exception as e:
                self.log(f"Error in crawler: {str(e)}")
                self.log(traceback.format_exc())
        
        # Run crawler in a separate thread
        threading.Thread(target=run_crawler, daemon=True).start()
    
    def scrape_urls(self):
        """
        Get URLs from the database based on type and status filters,
        then scrape and process them.
        """
        # Get parameters from UI
        data_type = self.scrape_type_var.get()
        url_limit = self.url_limit_var.get()
        status_filter = self.url_status_var.get()
        
        def run_scraper():
            try:
                # Update UI
                self.scrape_stats_var.set("Scraping in progress...")
                self.log(f"Starting scrape of {data_type} URLs with status {status_filter}, limit {url_limit}")
                
                # Connect to database
                conn = self.connect_to_database()
                if not conn:
                    self.log("Failed to connect to database")
                    self.scrape_stats_var.set("Error: Database connection failed")
                    return
                
                # Get URLs from database based on filters
                cursor = conn.cursor()
                
                # Build query based on filters
                query = "SELECT url FROM urls WHERE 1=1"
                params = []
                
                # Filter by type if not 'all'
                if data_type != 'all':
                    query += " AND type = ?"
                    params.append(data_type)
                
                # Filter by status
                if status_filter == 'unprocessed':
                    query += " AND (status = 'unprocessed' OR status IS NULL)"
                elif status_filter == 'failed':
                    query += " AND status = 'failed'"
                elif status_filter == 'successful':
                    query += " AND status = 'successful'"
                
                # Add limit
                query += " LIMIT ?"
                params.append(url_limit)
                
                # Execute query
                cursor.execute(query, params)
                urls_to_scrape = [row[0] for row in cursor.fetchall()]
                
                if not urls_to_scrape:
                    self.log("No URLs found matching the criteria")
                    self.scrape_stats_var.set("No URLs found")
                    conn.close()
                    return
                
                self.log(f"Found {len(urls_to_scrape)} URLs to scrape")
                
                # Define progress callback
                def update_progress(current, total):
                    progress = int((current / total) * 100)
                    self.progress_var.set(progress)
                    stats = f"Scraped {current}/{total} URLs"
                    self.scrape_stats_var.set(stats)
                    root.update()
                
                # Scrape URLs
                scraped_data = self.scrape_urls_by_type(
                    urls_to_scrape,
                    data_type,
                    url_limit,
                    conn,
                    log_callback=self.log,
                    progress_callback=update_progress
                )
                
                # Process and save the scraped data
                if scraped_data:
                    self.process_scraped_data(conn, scraped_data, data_type)
                
                conn.close()
                
                # Update UI with final stats
                self.scrape_stats_var.set(f"Completed: {len(scraped_data)} items scraped")
                self.log(f"Scraping completed. {len(scraped_data)} items scraped.")
                
            except Exception as e:
                self.log(f"Error during scraping: {str(e)}")
                self.scrape_stats_var.set(f"Error: {str(e)}")
                traceback.print_exc()
        
        # Run scraper in a separate thread
        threading.Thread(target=run_scraper).start()
    
    def log_message(self, message):
        """Log a message to the UI"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.log_text.insert(tk.END, f"[{timestamp}] {message}\n")
        self.log_text.see(tk.END)  # Scroll to the end
        # Update the UI immediately to show the new log entry
        self.root.update()  # Forces an immediate update of the UI

    def log(self, message):
        """Log a message to the UI"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.log_text.insert(tk.END, f"[{timestamp}] {message}\n")
        self.log_text.see(tk.END)  # Scroll to the end
        # Update the UI immediately to show the new log entry
        self.root.update()  # Forces an immediate update of the UI

    def save_urls_to_database(self, conn, urls, status="unprocessed"):
        """
        Save URLs to the database
        
        Args:
            conn (sqlite3.Connection): Database connection
            urls (list): URLs to save
            status (str): Status value (default: "unprocessed")
            
        Returns:
            int: Number of URLs saved
        """
        if not urls:
            return 0
            
        try:
            cursor = conn.cursor()
            
            # Create table if it doesn't exist
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS urls (
                url TEXT PRIMARY KEY,
                date_accessed TIMESTAMP,
                status TEXT DEFAULT "unprocessed",
                type TEXT
            )
            ''')
            
            # Get current timestamp
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # Define URL patterns for each type with less restrictive matching
            url_patterns = {
                'jockeys': re.compile(r'/racing/profiles/jockey/'),
                'trainers': re.compile(r'/racing/profiles/trainer/'),
                'horses': re.compile(r'/racing/profiles/horse/'),
                'races': re.compile(r'/racing/results/\d{4}-\d{2}-\d{2}/.+/\d+/')  # Less strict race pattern
            }
            
            # Helper function to determine URL type
            def get_url_type(url):
                # Remove protocol and domain for matching
                url_path = url.replace('https://www.sportinglife.com', '')
                
                for type_name, pattern in url_patterns.items():
                    if pattern.search(url_path):
                        return type_name
                return 'unknown'
            
            # Filter out date-only URLs that might have slipped through
            date_pattern = re.compile(r'/racing/results/\d{4}-\d{2}-\d{2}$')
            filtered_urls = [url for url in urls if not date_pattern.search(url.replace('https://www.sportinglife.com', ''))]
            
            if len(filtered_urls) < len(urls):
                self.log(f"Filtered out {len(urls) - len(filtered_urls)} date-only URLs")
            
            # Use executemany for better performance
            url_data = [(url, timestamp, status, get_url_type(url)) for url in filtered_urls]
            
            # Debug URL types being saved
            type_counts = {}
            for _, _, _, url_type in url_data:
                type_counts[url_type] = type_counts.get(url_type, 0) + 1
            self.log(f"URL types being saved: {type_counts}")
            
            cursor.executemany(
                "INSERT OR IGNORE INTO urls (url, date_accessed, status, type) VALUES (?, ?, ?, ?)",
                url_data
            )
            
            conn.commit()
            return cursor.rowcount
        except Exception as e:
            self.log(f"Error saving URLs to database: {str(e)}")
            return 0

    def update_url_stats(self):
        """Update the URL statistics table"""
        try:
            if self.conn is None:
                self.log("Please connect to the database first")
                return
            
            stats = get_url_stats(self.conn)
            self.display_url_stats(stats)
            self.log("URL statistics updated")
        except Exception as e:
            self.log(f"Error updating URL statistics: {str(e)}")

    def display_url_stats(self, stats):
        """Display the URL statistics in the UI"""
        # Clear existing widgets in stats_frame
        for widget in self.stats_frame.winfo_children():
            widget.destroy()
        
        # Check for errors
        if "error" in stats:
            error_label = ttk.Label(self.stats_frame, text=f"Error: {stats['error']}")
            error_label.pack(padx=5, pady=5)
            return
            
        # Check if we have data
        if not stats["types"] or not stats["statuses"]:
            no_data_label = ttk.Label(self.stats_frame, text="No URL data available")
            no_data_label.pack(padx=5, pady=5)
            return
        
        # Define the specific status columns we want to show in the specified order
        ordered_statuses = ["unprocessed", "failed", "processed", "all"]
        
        # Create table as a grid of labels
        # Header row with status types
        row = 0
        col = 1
        
        # Empty cell in top-left corner
        ttk.Label(self.stats_frame, text="", width=12).grid(row=row, column=0)
        
        # Status headers
        for status in ordered_statuses:
            ttk.Label(self.stats_frame, text=status, font=("", 9, "bold"), 
                     width=12, anchor="center").grid(row=row, column=col)
            col += 1
        
        # Data rows
        for type_name in stats["types"]:
            row += 1
            col = 0
            
            # Type name in first column
            ttk.Label(self.stats_frame, text=type_name, font=("", 9, "bold"), 
                     width=12, anchor="w").grid(row=row, column=col)
            
            # Status counts
            col = 1
            for status in ordered_statuses:
                if status == "all":
                    # For "all" status, sum up all statuses for this type
                    count = stats["totals"]["by_type"].get(type_name, 0)
                else:
                    count = stats["counts"].get(type_name, {}).get(status, 0)
                    # If count is None, set it to 0
                    if count is None:
                        count = 0
                ttk.Label(self.stats_frame, text=str(count), width=12, 
                         anchor="center").grid(row=row, column=col)
                col += 1
        
        # Total row
        row += 1
        col = 0
        ttk.Label(self.stats_frame, text="Total", font=("", 9, "bold"), 
                 width=12, anchor="w").grid(row=row, column=col)
        
        # Status totals
        col = 1
        for status in ordered_statuses:
            if status == "all":
                # For "all" status, use the overall total
                count = stats["totals"]["overall"]
            else:
                count = stats["totals"]["by_status"].get(status, 0)
                # If count is None, set it to 0
                if count is None:
                    count = 0
            ttk.Label(self.stats_frame, text=str(count), width=12, 
                     anchor="center", font=("", 9, "bold")).grid(row=row, column=col)
            col += 1

def save_crawl_state(state, filename="crawl_state.json"):
    """
    Save crawl state to a JSON file for resuming later
    
    Args:
        state (dict): Crawl state to save
        filename (str): Filename to save to
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        import json
        
        # Convert sets to lists for JSON serialization
        state_copy = state.copy()
        if 'visited_urls' in state_copy:
            state_copy['visited_urls'] = list(state_copy['visited_urls'])
        if 'discovered_urls' in state_copy and isinstance(state_copy['discovered_urls'], set):
            state_copy['discovered_urls'] = list(state_copy['discovered_urls'])
            
        with open(filename, 'w') as f:
            json.dump(state_copy, f)
        return True
    except Exception as e:
        print(f"Error saving crawl state: {str(e)}")
        return False

def load_crawl_state(filename="crawl_state.json"):
    """
    Load crawl state from a JSON file
    
    Args:
        filename (str): Filename to load from
        
    Returns:
        dict: Loaded crawl state or empty dict if not found/error
    """
    try:
        import json
        import os
        
        if not os.path.exists(filename):
            return {}
            
        with open(filename, 'r') as f:
            state = json.load(f)
            
        # Convert lists back to sets
        if 'visited_urls' in state:
            state['visited_urls'] = set(state['visited_urls'])
        if 'discovered_urls' in state and isinstance(state['discovered_urls'], list):
            state['discovered_urls'] = set(state['discovered_urls'])
            
        return state
    except Exception as e:
        print(f"Error loading crawl state: {str(e)}")
        return {}

# === Main Entry Point ===

if __name__ == "__main__":
    root = tk.Tk()
    app = CollectorUI(root)
    root.mainloop() 