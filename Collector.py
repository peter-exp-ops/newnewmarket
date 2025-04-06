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
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
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
        
        # Helper function to determine URL type based on pattern
        def get_url_type(url):
            patterns = {
                'jockeys': r'https?://www\.sportinglife\.com/racing/profiles/jockey/\d+',
                'trainers': r'https?://www\.sportinglife\.com/racing/profiles/trainer/\d+',
                'races': r'https?://www\.sportinglife\.com/racing/results/\d{4}-\d{2}-\d{2}/[\w-]+/\d+/[\w-]+',
                'horses': r'https?://www\.sportinglife\.com/racing/profiles/horse/\d+'
            }
            
            for url_type, pattern in patterns.items():
                if re.match(pattern, url):
                    return url_type
            
            return 'unknown'
        
        # Use executemany for better performance
        url_data = [(url, timestamp, status, get_url_type(url)) for url in urls]
        cursor.executemany(
            "INSERT OR IGNORE INTO urls (url, date_accessed, status, type) VALUES (?, ?, ?, ?)",
            url_data
        )
        
        conn.commit()
        return cursor.rowcount
    except Exception as e:
        print(f"Error saving URLs to database: {str(e)}")
        return 0

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
    
    # URL patterns to match for different types
    url_patterns = {
        'jockeys': r'https?://www\.sportinglife\.com/racing/profiles/jockey/\d+$',
        'trainers': r'https?://www\.sportinglife\.com/racing/profiles/trainer/\d+$',
        'races': r'https?://www\.sportinglife\.com/racing/results/\d{4}-\d{2}-\d{2}/[\w-]+/\d+/[\w-]+',
        'horses': r'https?://www\.sportinglife\.com/racing/profiles/horse/\d+$',
        # 'all' pattern matches any of the above
        'all': r'https?://www\.sportinglife\.com/racing/(results/\d{4}-\d{2}-\d{2}/[\w-]+/\d+/[\w-]+|profiles/(jockey|trainer|horse)/\d+)$'
    }

    # URL base patterns by type for direct ID-based enumeration
    base_patterns = {
        'jockeys': 'https://www.sportinglife.com/racing/profiles/jockey/',
        'trainers': 'https://www.sportinglife.com/racing/profiles/trainer/',
        'horses': 'https://www.sportinglife.com/racing/profiles/horse/',
        'races': 'https://www.sportinglife.com/racing/results/'
    }
    
    # Check if we're looking for all types
    if data_type == 'all':
        active_pattern = url_patterns['all']
        log("Searching for all URL types: races, jockeys, trainers, and horses")
    else:
        # Check if the data type is valid
        active_pattern = url_patterns.get(data_type)
        if not active_pattern:
            log(f"Error: Invalid data type '{data_type}'")
            return []
    
    # Special handling for race results page
    if base_url == "https://www.sportinglife.com/racing/results/":
        log("Special handling for race results page - scanning for date-based links")
        try:
            # Get the main results page
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            response = requests.get(base_url, headers=headers, timeout=10)
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Look for date-based links (calendar items, date tabs)
                date_links = []
                
                # Find date links in the calendar
                for link in soup.find_all('a', href=True):
                    href = link['href']
                    if '/racing/results/' in href and re.search(r'/\d{4}-\d{2}-\d{2}', href):
                        full_url = "https://www.sportinglife.com" + href if href.startswith('/') else href
                        if full_url not in date_links:
                            date_links.append(full_url)
                
                log(f"Found {len(date_links)} date-based links in the results page")
                
                # Add date links to visit queue
                for link in date_links:
                    if link not in urls_to_visit:
                        urls_to_visit.append(link)
        except Exception as e:
            log(f"Error processing main results page: {str(e)}")
    
    # Special handling for direct ID-based URL types (jockeys, trainers, horses)
    elif data_type in ['jockeys', 'trainers', 'horses'] and base_url.endswith('/'):
        base_pattern = base_patterns.get(data_type)
        if base_pattern and base_url.startswith(base_pattern):
            log(f"Direct ID search for {data_type} - will try sequential IDs")
            
            # Start with a reasonable ID range based on type
            start_id = 1
            max_id = 10000  # Default
            
            if data_type == 'jockeys':
                max_id = 5000  # Fewer jockeys than horses
            elif data_type == 'trainers':
                max_id = 10000  # Fewer trainers than horses
            elif data_type == 'horses':
                max_id = 2000000  # Lots of horses
                
            # Generate URLs with sequential IDs
            for id_num in range(start_id, min(start_id + max_urls, max_id + 1)):
                url = f"{base_pattern}{id_num}"
                # Skip if already processed successfully
                if url in captured_urls and captured_urls[url] == 1:
                    continue
                urls_to_visit.append(url)
                # Check for timeout every 100 IDs to avoid excessive checks
                if id_num % 100 == 0 and timeout_callback and timeout_callback():
                    log("Timeout reached during ID generation.")
                    break
            
            log(f"Generated {len(urls_to_visit)} potential {data_type} URLs to check")
    
    # Log start of crawl
    log(f"Starting crawl from {base_url}")
    log(f"Skipping {len([u for u, s in captured_urls.items() if s == 1])} URLs already successfully captured")
    
    # Initialize counters for summary
    pages_checked = 0
    skipped_count = 0
    found_by_type = {'races': 0, 'jockeys': 0, 'trainers': 0, 'horses': 0}
    no_new_urls_count = 0
    consecutive_empty_count = 0
    
    # Helper function to check URL type
    def get_url_type(url):
        for type_name, pattern in url_patterns.items():
            if type_name != 'all' and re.match(pattern, url):
                return type_name
        return None
    
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
        self.root.title("Racing Data Collector")
        self.root.geometry("900x700")
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
        
        self.status_var = StringVar(value="Not Connected")
        ttk.Label(conn_frame, text="Status:").grid(row=0, column=0, sticky=tk.W, padx=5, pady=5)
        ttk.Label(conn_frame, textvariable=self.status_var).grid(row=0, column=1, sticky=tk.W, padx=5, pady=5)
        ttk.Button(conn_frame, text="Connect to Database", command=self.connect_to_db).grid(row=0, column=2, padx=5, pady=5)
        
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
        
        ttk.Button(crawler_frame, text="Crawl Website", command=self.crawl_website).grid(row=3, column=0, padx=5, pady=5)
        
        self.crawl_stats_var = StringVar(value="Not started")
        ttk.Label(crawler_frame, textvariable=self.crawl_stats_var).grid(row=3, column=1, sticky=tk.W, padx=5, pady=5)
        
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
            if self.check_database_structure(self.conn):
                self.status_var.set("Connected")
                self.log("Successfully connected to the database.")
            else:
                self.status_var.set("Connected (incomplete schema)")
                self.log("Connected to database but schema is incomplete.")
        except Exception as e:
            self.status_var.set("Connection Failed")
            self.log(f"Failed to connect to database: {str(e)}")
            messagebox.showerror("Connection Error", f"Failed to connect to database: {str(e)}")
    
    def crawl_website(self):
        """
        Crawl the website to find URLs of races, jockeys, trainers, and horses.
        URLs are saved to the database with status="unprocessed".
        """
        base_url = self.base_url_var.get()
        max_urls = self.max_urls_var.get()
        timeout_mins = self.timeout_var.get()
        
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
                
                self.check_database_structure(conn)
                
                # Get existing URLs from database to avoid duplicates
                cursor = conn.cursor()
                cursor.execute("SELECT url FROM urls")
                existing_urls = {row[0] for row in cursor.fetchall()}
                
                discovered_urls = set()
                visited_urls = set()
                queue = [base_url]
                url_count = 0
                
                # Define URL patterns for each type - more lenient patterns
                url_patterns = {
                    'jockeys': re.compile(r'/racing/profiles/jockey/\d+'),
                    'trainers': re.compile(r'/racing/profiles/trainer/\d+'),
                    'races': re.compile(r'/racing/results/\d{4}-\d{2}-\d{2}'),
                    'horses': re.compile(r'/racing/profiles/horse/\d+')
                }
                
                # Additional pattern for race results pages
                race_results_pattern = re.compile(r'/racing/results/\d{4}-\d{2}-\d{2}/[^/]+/\d+/\d+')
                
                # Add debugging
                self.log(f"Initial queue: {queue}")
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
                
                while queue and url_count < max_urls and not timeout_reached():
                    current_url = queue.pop(0)
                    current_url = normalize_url(current_url)
                    
                    if current_url in visited_urls:
                        continue
                    
                    visited_urls.add(current_url)
                    self.log(f"Visiting: {current_url}")
                    
                    try:
                        # Add delay to avoid overloading the server
                        time.sleep(0.2)
                        
                        response = requests.get(current_url, headers=HEADERS)
                        if response.status_code != 200:
                            self.log(f"Failed to get {current_url}: {response.status_code}")
                            continue
                        
                        soup = BeautifulSoup(response.text, 'html.parser')
                        
                        # Find all links in the page
                        links = soup.find_all('a', href=True)
                        self.log(f"Found {len(links)} links on the page")
                        
                        # Debug first 5 links
                        for i, link in enumerate(links[:5]):
                            self.log(f"Sample link {i+1}: {link['href']}")
                        
                        # Count how many links match our patterns
                        matching_count = 0
                        
                        # Process each link
                        for link in links:
                            href = link['href']
                            
                            # Normalize URL
                            href = normalize_url(href)
                            
                            # Skip if not a Sporting Life URL
                            if not href.startswith('https://www.sportinglife.com/'):
                                continue
                            
                            # Check if it matches any of our patterns
                            matched = False
                            match_type = None
                            
                            # Check all patterns since we're crawling for all types
                            for type_name, pattern in url_patterns.items():
                                if pattern.search(href):
                                    matched = True
                                    match_type = type_name
                                    break
                            
                            # Also check race results pattern
                            if not matched and race_results_pattern.search(href):
                                matched = True
                                match_type = 'races'
                            
                            # Skip if no match
                            if not matched:
                                continue
                                
                            matching_count += 1
                            
                            # Skip if already processed or queued
                            if href in discovered_urls or href in existing_urls:
                                continue
                            
                            # Add to discovered URLs and queue
                            discovered_urls.add(href)
                            queue.append(href)
                            url_count += 1
                            self.log(f"Found matching URL ({match_type}): {href}")
                            
                            # Periodically update UI
                            if url_count % 10 == 0 or url_count == 1:  # Also update on first URL
                                elapsed = time.time() - start_time
                                stats = f"Found {url_count} URLs in {elapsed:.1f} seconds"
                                self.crawl_stats_var.set(stats)
                                self.root.update()
                            
                            # Check if we've hit the max
                            if url_count >= max_urls:
                                break
                        
                        self.log(f"Page had {matching_count} links matching our patterns")
                            
                    except Exception as e:
                        self.log(f"Error processing {current_url}: {str(e)}")
                        traceback.print_exc()
                        continue
                        
                    # Check timeout every few URLs
                    if len(visited_urls) % 5 == 0 and timeout_reached():
                        self.log(f"Timeout reached after {time.time() - start_time:.1f} seconds. Stopping crawl.")
                        break
                
                # Save discovered URLs to database
                if discovered_urls:
                    saved_count = self.save_urls_to_database(conn, list(discovered_urls))
                    self.log(f"Saved {saved_count} URLs to database")
                
                # Update UI with final stats
                elapsed_time = time.time() - start_time
                stats = f"Found {len(discovered_urls)} URLs in {elapsed_time:.1f} seconds"
                self.crawl_stats_var.set(stats)
                
                conn.close()
                self.log(f"Crawl completed. Found {len(discovered_urls)} URLs.")
                
            except Exception as e:
                self.log(f"Error during crawl: {str(e)}")
                self.crawl_stats_var.set(f"Error: {str(e)}")
                traceback.print_exc()
        
        # Run crawler in a separate thread
        threading.Thread(target=run_crawler).start()
    
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

    def check_database_structure(self, conn):
        """
        Check if the database has all required tables
        
        Args:
            conn (sqlite3.Connection): Database connection
            
        Returns:
            bool: True if all required tables exist
        """
        required_tables = ['races', 'horses', 'jockeys', 'trainers', 'racehorses', 'urls']
        
        try:
            cursor = conn.cursor()
            
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
                conn.commit()
            
            return all_tables_exist
        except Exception as e:
            self.log(f"Error checking database structure: {str(e)}")
            return False

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
            
            # Helper function to determine URL type based on pattern
            def get_url_type(url):
                patterns = {
                    'jockeys': r'https?://www\.sportinglife\.com/racing/profiles/jockey/\d+',
                    'trainers': r'https?://www\.sportinglife\.com/racing/profiles/trainer/\d+',
                    'races': r'https?://www\.sportinglife\.com/racing/results/\d{4}-\d{2}-\d{2}/[\w-]+/\d+/[\w-]+',
                    'horses': r'https?://www\.sportinglife\.com/racing/profiles/horse/\d+'
                }
                
                for url_type, pattern in patterns.items():
                    if re.match(pattern, url):
                        return url_type
                
                return 'unknown'
            
            # Use executemany for better performance
            url_data = [(url, timestamp, status, get_url_type(url)) for url in urls]
            cursor.executemany(
                "INSERT OR IGNORE INTO urls (url, date_accessed, status, type) VALUES (?, ?, ?, ?)",
                url_data
            )
            
            conn.commit()
            return cursor.rowcount
        except Exception as e:
            self.log(f"Error saving URLs to database: {str(e)}")
            return 0

# === Main Entry Point ===

if __name__ == "__main__":
    root = tk.Tk()
    app = CollectorUI(root)
    root.mainloop() 