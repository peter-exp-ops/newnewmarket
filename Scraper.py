#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Scraper.py - Web scraping tool for racing data

This script handles crawling, scraping, processing, and storing racing data
including races, horses, jockeys, and trainers.
"""

import requests
from bs4 import BeautifulSoup
import re
import sqlite3
import os
import pandas as pd
from datetime import datetime, timedelta
import time
import random
import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext, filedialog, StringVar

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
        
        conn.close()
        return all_tables_exist
    except Exception as e:
        print(f"Error checking database structure: {e}")
        return False

# === Crawling Functions ===

def crawl_website(base_url, max_urls=1000, data_type=None):
    """
    Crawl a website to find all sub-URLs to any depth
    
    Args:
        base_url (str): The URL to start crawling from
        max_urls (int): Maximum number of URLs to collect
        data_type (str): Type of URLs to look for ('races', 'horses', 'jockeys', 'trainers')
        
    Returns:
        list: List of discovered URLs
    """
    discovered_urls = []
    visited_urls = set()
    urls_to_visit = [base_url]
    
    # URL patterns to match for different types
    url_patterns = {
        'jockeys': r'https?://www\.sportinglife\.com/racing/profiles/jockey/\d+',
        'trainers': r'https?://www\.sportinglife\.com/racing/profiles/trainer/\d+',
        'races': r'https?://www\.sportinglife\.com/racing/results/\d{4}-\d{2}-\d{2}/[\w-]+/\d+/[\w-]+',
        'horses': r'https?://www\.sportinglife\.com/racing/profiles/horse/\d+'
    }
    
    # Prioritized link patterns - URLs containing these are more likely to lead to valuable content
    priority_patterns = {
        'trainers': ['/racing/profiles/trainer', '/racing/trainer', '/trainers'],
        'jockeys': ['/racing/profiles/jockey', '/racing/jockey', '/jockeys'],
        'horses': ['/racing/profiles/horse', '/racing/horse', '/horses'],
        'races': ['/racing/results', '/racing/racecards', '/meetings']
    }
    
    # If checking for a specific target URL (for testing), add it directly
    if data_type == 'trainers' and 'profiles' in base_url:
        test_url = 'https://www.sportinglife.com/racing/profiles/trainer/414'  # W P Mullins
        urls_to_visit.append(test_url)
        print(f"Added test URL for trainer: {test_url}")
    
    active_pattern = url_patterns.get(data_type)
    if not active_pattern:
        print(f"Error: Invalid data type '{data_type}'")
        return []
    
    print(f"Starting crawl. Looking for {data_type} URLs from {base_url}")
    
    # Counter for logging
    pages_checked = 0
    
    while urls_to_visit and len(discovered_urls) < max_urls:
        # Get the next URL to visit
        current_url = urls_to_visit.pop(0)
        
        # Skip if already visited
        if current_url in visited_urls:
            continue
        
        visited_urls.add(current_url)
        pages_checked += 1
        
        # Check if the current URL matches our pattern
        if re.match(active_pattern, current_url) and current_url not in discovered_urls:
            discovered_urls.append(current_url)
            print(f"Found matching {data_type} URL: {current_url}")
        
        # Print progress every 5 pages
        if pages_checked % 5 == 0:
            print(f"Checked {pages_checked} pages, found {len(discovered_urls)} matching URLs so far")
        
        try:
            # Add some delay to avoid overwhelming the server
            time.sleep(random.uniform(0.3, 0.7))
            
            # Make request
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            response = requests.get(current_url, headers=headers, timeout=10)
            
            if response.status_code != 200:
                print(f"Failed to fetch {current_url}: {response.status_code}")
                continue
                
            # Parse HTML
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Find all links
            links = soup.find_all('a', href=True)
            
            # Sort links by priority for the specific data type
            if data_type in priority_patterns:
                links = sorted(links, key=lambda link: any(pattern in link['href'] for pattern in priority_patterns[data_type]), reverse=True)
            
            for link in links:
                href = link['href']
                
                # Convert relative URLs to absolute
                if href.startswith('/'):
                    href = "https://www.sportinglife.com" + href
                # Skip anchors, javascript, etc
                elif not (href.startswith('http://') or href.startswith('https://')):
                    continue
                # Skip external links
                elif 'sportinglife.com' not in href:
                    continue
                    
                # Debug output for URLs that might be of interest
                priority_terms = priority_patterns.get(data_type, [])
                if any(term in href for term in priority_terms) and href not in discovered_urls and href not in visited_urls:
                    print(f"Found potential {data_type} URL: {href}")
                
                # Normalize URL by removing trailing slash
                href = href.rstrip('/')
                
                # Check if URL matches the pattern for the desired type
                if re.match(active_pattern, href) and href not in discovered_urls:
                    discovered_urls.append(href)
                    print(f"Found matching {data_type} URL: {href}")
                
                # Add to visit queue if not already visited or queued
                if href not in visited_urls and href not in urls_to_visit:
                    # Always add URLs to the queue for unlimited depth traversal
                    urls_to_visit.append(href)
                    
                    # Prioritize URLs with relevant patterns by moving them to the front of the queue
                    if data_type in priority_patterns and any(pattern in href for pattern in priority_patterns[data_type]):
                        urls_to_visit.remove(href)
                        urls_to_visit.insert(0, href)
                
                # Check if we've reached the max URLs limit
                if len(discovered_urls) >= max_urls:
                    break
            
            # Print progress update
            if len(discovered_urls) > 0 and len(discovered_urls) % 10 == 0:
                print(f"Progress: Found {len(discovered_urls)} URLs, visited {len(visited_urls)} pages, {len(urls_to_visit)} pages in queue")
            
        except Exception as e:
            print(f"Error processing {current_url}: {str(e)}")
    
    print(f"Crawl complete. Found {len(discovered_urls)} matching URLs after checking {pages_checked} pages")
    return discovered_urls

def filter_urls_by_type(urls, url_type):
    """
    Filter URLs based on their type (races, horses, jockeys, trainers)
    
    Args:
        urls (list): List of URLs to filter
        url_type (str): Type of URLs to keep ('races', 'horses', 'jockeys', 'trainers')
        
    Returns:
        list: Filtered list of URLs
    """
    url_patterns = {
        'jockeys': r'https?://www\.sportinglife\.com/racing/profiles/jockey/\d+',
        'trainers': r'https?://www\.sportinglife\.com/racing/profiles/trainer/\d+',
        'races': r'https?://www\.sportinglife\.com/racing/results/\d{4}-\d{2}-\d{2}/[\w-]+/\d+/[\w-]+',
        'horses': r'https?://www\.sportinglife\.com/racing/profiles/horse/\d+'
    }
    
    pattern = url_patterns.get(url_type)
    if not pattern:
        return []
    
    return [url for url in urls if re.match(pattern, url)]

def get_captured_urls(conn):
    """
    Get list of URLs that have already been captured in the database
    
    Args:
        conn (sqlite3.Connection): Database connection
        
    Returns:
        list: List of URLs already in the database
    """
    cursor = conn.cursor()
    cursor.execute("SELECT url FROM urls")
    return [row[0] for row in cursor.fetchall()]

def analyze_url_coverage(all_urls, captured_urls):
    """
    Analyze how many URLs have been captured vs. total available
    
    Args:
        all_urls (list): All discovered URLs
        captured_urls (list): URLs already captured in the database
        
    Returns:
        dict: Statistics about URL coverage
    """
    # Convert captured_urls to a set for faster lookup
    captured_set = set(captured_urls)
    
    # Count URLs by type
    url_patterns = {
        'jockeys': r'https://www\.sportinglife\.com/racing/profiles/jockey/\d+',
        'trainers': r'https://www\.sportinglife\.com/racing/profiles/trainer/\d+',
        'races': r'https://www\.sportinglife\.com/racing/results/\d{4}-\d{2}-\d{2}/[\w-]+/\d+/[\w-]+',
        'horses': r'https://www\.sportinglife\.com/racing/profiles/horse/\d+'
    }
    
    stats = {
        'total_discovered': len(all_urls),
        'total_captured': len(captured_set),
        'new_urls': len(set(all_urls) - captured_set),
        'types': {}
    }
    
    for url_type, pattern in url_patterns.items():
        type_urls = [url for url in all_urls if re.match(pattern, url)]
        type_captured = [url for url in type_urls if url in captured_set]
        
        stats['types'][url_type] = {
            'discovered': len(type_urls),
            'captured': len(type_captured),
            'new': len(type_urls) - len(type_captured)
        }
    
    return stats

# === Scraping Functions ===

def scrape_urls_by_type(urls, url_type, limit):
    """
    Scrape a limited number of URLs of a specific type
    
    Args:
        urls (list): List of URLs to scrape
        url_type (str): Type of URLs to scrape ('races', 'horses', 'jockeys', 'trainers')
        limit (int): Maximum number of URLs to scrape
        
    Returns:
        list: Scraped data
    """
    pass

def process_scraped_data(data, data_type):
    """
    Process scraped data based on its type
    
    Args:
        data (list): Scraped data to process
        data_type (str): Type of data ('races', 'horses', 'jockeys', 'trainers')
        
    Returns:
        list: Processed data ready for database insertion
    """
    pass

def save_data_to_database(conn, data, data_type):
    """
    Save processed data to the appropriate database table
    
    Args:
        conn (sqlite3.Connection): Database connection
        data (list): Processed data to save
        data_type (str): Type of data ('races', 'horses', 'jockeys', 'trainers')
        
    Returns:
        int: Number of records saved
    """
    pass

# === UI Class ===

class ScraperUI:
    """UI for controlling the web crawler and scraper"""
    
    def __init__(self, root):
        """Initialize the UI"""
        self.root = root
        self.root.title("Web Crawler and Scraper")
        self.root.geometry("900x700")
        self.conn = None
        self.discovered_urls = []
        self.captured_urls = []
        self.setup_ui()
    
    def setup_ui(self):
        """Set up the UI components"""
        # Create main frame with padding
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # Connection section
        conn_frame = ttk.LabelFrame(main_frame, text="Database Connection", padding="10")
        conn_frame.pack(fill=tk.X, pady=5)
        
        self.status_var = StringVar(value="Not Connected")
        ttk.Label(conn_frame, text="Status:").grid(row=0, column=0, sticky=tk.W, padx=5, pady=5)
        ttk.Label(conn_frame, textvariable=self.status_var).grid(row=0, column=1, sticky=tk.W, padx=5, pady=5)
        ttk.Button(conn_frame, text="Connect to Database", command=self.connect_to_db).grid(row=0, column=2, padx=5, pady=5)
        
        # Crawler section
        crawler_frame = ttk.LabelFrame(main_frame, text="URL Crawler", padding="10")
        crawler_frame.pack(fill=tk.X, pady=5)
        
        ttk.Label(crawler_frame, text="Base URL:").grid(row=0, column=0, sticky=tk.W, padx=5, pady=5)
        # Dynamic default URL based on data type
        self.base_url_var = StringVar(value="https://www.sportinglife.com/racing/results/")
        ttk.Entry(crawler_frame, textvariable=self.base_url_var, width=70).grid(row=0, column=1, columnspan=2, padx=5, pady=5)
        
        ttk.Label(crawler_frame, text="Data Type:").grid(row=1, column=0, sticky=tk.W, padx=5, pady=5)
        self.data_type_var = StringVar(value="races")
        data_type_combo = ttk.Combobox(crawler_frame, textvariable=self.data_type_var, 
                     values=["races", "horses", "jockeys", "trainers"], 
                     state="readonly")
        data_type_combo.grid(row=1, column=1, sticky=tk.W, padx=5, pady=5)
        data_type_combo.bind("<<ComboboxSelected>>", self.update_default_url)
        
        ttk.Label(crawler_frame, text="Max URLs:").grid(row=2, column=0, sticky=tk.W, padx=5, pady=5)
        self.max_urls_var = tk.IntVar(value=1000)
        ttk.Spinbox(crawler_frame, from_=100, to=10000, increment=100, textvariable=self.max_urls_var, width=10).grid(row=2, column=1, sticky=tk.W, padx=5, pady=5)
        
        ttk.Button(crawler_frame, text="Crawl Website", command=self.crawl_and_analyze).grid(row=3, column=0, padx=5, pady=5)
        
        self.crawl_stats_var = StringVar(value="Not started")
        ttk.Label(crawler_frame, textvariable=self.crawl_stats_var).grid(row=3, column=1, sticky=tk.W, padx=5, pady=5)
        
        # Scraper section
        scraper_frame = ttk.LabelFrame(main_frame, text="Targeted Scraper", padding="10")
        scraper_frame.pack(fill=tk.X, pady=5)
        
        ttk.Label(scraper_frame, text="Limit:").grid(row=0, column=0, sticky=tk.W, padx=5, pady=5)
        self.limit_var = tk.IntVar(value=10)
        ttk.Spinbox(scraper_frame, from_=1, to=1000, textvariable=self.limit_var, width=10).grid(row=0, column=1, sticky=tk.W, padx=5, pady=5)
        
        ttk.Button(scraper_frame, text="Scrape Discovered URLs", command=self.scrape_selected_type).grid(row=1, column=0, padx=5, pady=5)
        
        self.scrape_stats_var = StringVar(value="Not started")
        ttk.Label(scraper_frame, textvariable=self.scrape_stats_var).grid(row=1, column=1, sticky=tk.W, padx=5, pady=5)
        
        # Progress section
        progress_frame = ttk.LabelFrame(main_frame, text="Progress", padding="10")
        progress_frame.pack(fill=tk.X, pady=5)
        
        self.progress_var = tk.DoubleVar()
        ttk.Progressbar(progress_frame, orient=tk.HORIZONTAL, length=100, 
                       mode='determinate', variable=self.progress_var).pack(fill=tk.X, padx=5, pady=5)
        
        self.stats_var = StringVar(value="Ready")
        ttk.Label(progress_frame, textvariable=self.stats_var).pack(anchor=tk.W, padx=5)
        
        # Log section
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
        
        # Initialize data structures
        self.loaded_urls = []
    
    def connect_to_db(self):
        """Connect to the database"""
        try:
            if self.conn is not None:
                self.conn.close()
                
            self.conn = connect_to_database()
            
            # Check if the database has all required tables
            if check_database_structure():
                self.status_var.set("Connected")
                self.log_message("Successfully connected to the database.")
            else:
                self.status_var.set("Connected (incomplete schema)")
                self.log_message("Connected to database but schema is incomplete. Please initialize the database first.")
        except Exception as e:
            self.status_var.set("Connection Failed")
            self.log_message(f"Failed to connect to database: {str(e)}")
            messagebox.showerror("Connection Error", f"Failed to connect to database: {str(e)}")
    
    def crawl_and_analyze(self):
        """Crawl website and analyze URL coverage"""
        if self.conn is None:
            messagebox.showerror("Not Connected", "Please connect to the database first.")
            return
        
        base_url = self.base_url_var.get()
        max_urls = self.max_urls_var.get()
        data_type = self.data_type_var.get()
        
        self.log_message(f"Starting crawl of {base_url} for {data_type} URLs with max limit {max_urls}")
        self.progress_var.set(0)
        
        try:
            # Update UI
            self.crawl_stats_var.set("Crawling...")
            self.root.update_idletasks()
            
            # Get captured URLs from database
            self.captured_urls = get_captured_urls(self.conn)
            self.log_message(f"Found {len(self.captured_urls)} URLs already in database")
            
            # Start the crawl
            self.log_message("Beginning web crawl...")
            self.discovered_urls = crawl_website(base_url, max_urls, data_type)
            
            # Update progress
            self.progress_var.set(50)
            self.root.update_idletasks()
            
            # Analyze URL coverage
            self.log_message("Analyzing discovered URLs...")
            stats = analyze_url_coverage(self.discovered_urls, self.captured_urls)
            
            # Complete progress
            self.progress_var.set(100)
            
            # Update statistics in UI
            total_discovered = stats['total_discovered']
            total_new = stats['new_urls']
            
            self.crawl_stats_var.set(f"Found: {total_discovered}, New: {total_new}")
            
            # Log detailed statistics
            self.log_message(f"Crawl complete. Found {total_discovered} URLs, {total_new} are new.")
            
            type_stats = stats['types'].get(data_type, {})
            self.log_message(f"  {data_type.capitalize()}: {type_stats.get('discovered', 0)} found, {type_stats.get('new', 0)} new")
            
            # Update stats variable
            self.stats_var.set(f"Ready - {total_discovered} URLs discovered")
            
        except Exception as e:
            self.crawl_stats_var.set("Error")
            self.log_message(f"Error during crawl: {str(e)}")
            messagebox.showerror("Crawl Error", f"Failed to crawl website: {str(e)}")
            self.progress_var.set(0)
            self.stats_var.set("Error occurred")
    
    def scrape_selected_type(self):
        """Scrape selected URL type up to the specified limit"""
        if self.conn is None:
            messagebox.showerror("Not Connected", "Please connect to the database first.")
            return
        
        if not self.discovered_urls:
            messagebox.showwarning("No URLs", "Please crawl the website first to discover URLs.")
            return
        
        data_type = self.data_type_var.get()
        if data_type == "all":
            messagebox.showwarning("Type Selection", "Please select a specific data type to scrape in the crawler section.")
            return
            
        limit = self.limit_var.get()
        
        self.log_message(f"Starting scrape of {limit} {data_type} URLs")
        self.progress_var.set(0)
        
        try:
            # Update UI
            self.scrape_stats_var.set("Scraping...")
            self.root.update_idletasks()
            
            # Filter URLs by type - no need to filter again if already filtered during crawl
            filtered_urls = self.discovered_urls
            
            if not filtered_urls:
                self.scrape_stats_var.set("No matching URLs")
                self.log_message(f"No URLs found to scrape.")
                return
            
            # Limit number of URLs to scrape
            urls_to_scrape = filtered_urls[:limit]
            
            # Placeholder for actual implementation
            self.log_message(f"This is a placeholder. Actual scraping of {len(urls_to_scrape)} {data_type} URLs would happen here.")
            
            # Simulate scraping with progress updates
            for i in range(len(urls_to_scrape)):
                # Update progress
                self.progress_var.set((i + 1) / len(urls_to_scrape) * 100)
                self.root.update_idletasks()
                
                # Simulate processing delay
                time.sleep(0.5)
            
            # Update statistics
            self.scrape_stats_var.set(f"Scraped: {len(urls_to_scrape)} {data_type}")
            self.log_message(f"Scrape complete. Processed {len(urls_to_scrape)} {data_type} URLs.")
            
        except Exception as e:
            self.scrape_stats_var.set("Error")
            self.log_message(f"Error during scrape: {str(e)}")
            messagebox.showerror("Scrape Error", f"Failed to scrape URLs: {str(e)}")
    
    def log_message(self, message):
        """Log a message to the UI"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.log_text.insert(tk.END, f"[{timestamp}] {message}\n")
        self.log_text.see(tk.END)  # Scroll to the end
        # Also update the UI
        self.root.update_idletasks()

    def update_default_url(self, event=None):
        """Update the default URL based on the selected data type"""
        data_type = self.data_type_var.get()
        
        default_urls = {
            "races": "https://www.sportinglife.com/racing/results/",
            "horses": "https://www.sportinglife.com/racing/profiles/",
            "jockeys": "https://www.sportinglife.com/racing/profiles/",
            "trainers": "https://www.sportinglife.com/racing/profiles/"
        }
        
        self.base_url_var.set(default_urls.get(data_type, "https://www.sportinglife.com/racing/"))
        self.log_message(f"Updated base URL for {data_type} data type")

# === Main Entry Point ===

if __name__ == "__main__":
    root = tk.Tk()
    app = ScraperUI(root)
    root.mainloop()
