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
import json

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
    try:
        # Use check_same_thread=False to allow connections to be used across threads
        conn = sqlite3.connect(db_path, check_same_thread=False)
        # Enable foreign keys
        conn.execute("PRAGMA foreign_keys = ON")
        return conn
    except Exception as e:
        print(f"Error connecting to database: {str(e)}")
        return None

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
        
        # Check if urls table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='urls'")
        if not cursor.fetchone():
            raise Exception("Required table 'urls' does not exist in the database")
        
        # Get current timestamp
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Define URL patterns for each type with less restrictive matching
        url_patterns = {
            'jockeys': re.compile(r'/racing/profiles/jockey/\d+$'),
            'trainers': re.compile(r'/racing/profiles/trainer/\d+$'),
            'horses': re.compile(r'/racing/profiles/horse/\d+$'),
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
            "successful": "succeeded",
            "success": "succeeded",
            "completed": "succeeded",
            "processed": "succeeded",
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
    # Check if the required tables exist
    if conn:
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='urls'")
        if not cursor.fetchone():
            if log_callback:
                log_callback("Required table 'urls' does not exist in the database")
            return []

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
        'horses': re.compile(r'/racing/profiles/horse/\d+$'),  # Strict pattern ensuring it ends with the horse ID
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
            # Check if URLs table exists
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='urls'")
            if not cursor.fetchone():
                log("Required table 'urls' does not exist in the database")
                return discovered_urls
            
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
        'jockeys': r'https?://www\.sportinglife\.com/racing/profiles/jockey/\d+$',
        'trainers': r'https?://www\.sportinglife\.com/racing/profiles/trainer/\d+$',
        'races': r'https?://www\.sportinglife\.com/racing/results/\d{4}-\d{2}-\d{2}/[\w-]+/\d+/[\w-]+',
        'horses': r'https?://www\.sportinglife\.com/racing/profiles/horse/\d+$'  # Strict pattern with $ to match end of URL
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
    Scrape a race results page to extract race and finishing order information
    
    Args:
        url (str): URL of the race results page
        
    Returns:
        dict: Extracted race data or None if scraping failed
    """
    # Placeholder function for race scraping
    # This will be updated in a future version
    return None

def scrape_horse_page(url):
    """
    Scrape a horse profile page to extract horse information
    
    Args:
        url (str): URL of the horse profile page
        
    Returns:
        dict: Extracted horse data or None if scraping failed
    """
    try:
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Extract horse ID from URL
        horse_id = int(url.split('/')[-1])
        
        # Extract horse name from the page header
        horse_name = soup.find('h1').text.strip() if soup.find('h1') else None
        
        if not horse_name:
            print(f"Could not find horse name at URL: {url}")
            return None
            
        return {
            'id': horse_id,
            'name': horse_name
        }
        
    except Exception as e:
        print(f"Error scraping horse page {url}: {str(e)}")
        return None

def scrape_jockey_page(url):
    """
    Scrape a jockey profile page to extract jockey information
    
    Args:
        url (str): URL of the jockey profile page
        
    Returns:
        dict: Extracted jockey data or None if scraping failed
    """
    try:
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Extract jockey ID from URL
        jockey_id = int(url.split('/')[-1])
        
        # Extract jockey name from the page header
        jockey_name = soup.find('h1').text.strip() if soup.find('h1') else None
        
        if not jockey_name:
            print(f"Could not find jockey name at URL: {url}")
            return None
            
        return {
            'id': jockey_id,
            'name': jockey_name
        }
        
    except Exception as e:
        print(f"Error scraping jockey page {url}: {str(e)}")
        return None

def scrape_trainer_page(url):
    """
    Scrape a trainer profile page to extract trainer information
    
    Args:
        url (str): URL of the trainer profile page
        
    Returns:
        dict: Extracted trainer data or None if scraping failed
    """
    try:
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Extract trainer ID from URL
        trainer_id = int(url.split('/')[-1])
        
        # Extract trainer name from the page header
        trainer_name = soup.find('h1').text.strip() if soup.find('h1') else None
        
        if not trainer_name:
            print(f"Could not find trainer name at URL: {url}")
            return None
            
        return {
            'id': trainer_id,
            'name': trainer_name
        }
        
    except Exception as e:
        print(f"Error scraping trainer page {url}: {str(e)}")
        return None

def scrape_urls_by_type(urls, url_type, limit, conn=None, log_callback=None, progress_callback=None):
    """
    Scrape a list of URLs of a specific type
    
    Args:
        urls (list): List of URLs to scrape
        url_type (str): Type of URLs ('horses', 'jockeys', 'trainers', 'races')
        limit (int): Maximum number of URLs to scrape
        conn (sqlite3.Connection, optional): Database connection
        log_callback (function, optional): Function to call for logging
        progress_callback (function, optional): Function to call for progress updates
        
    Returns:
        list: List of scraped data
    """
    # Initialize variables
    results = []
    count = 0
    success_count = 0
    
    # Should we close the connection when done?
    close_conn = False
    if not conn:
        conn = connect_to_database()
        close_conn = True
    
    # Define a default log function if none provided
    if not log_callback:
        log_callback = print
        
    # Limit the number of URLs to process
    urls_to_process = urls[:min(limit, len(urls))]
    total_urls = len(urls_to_process)
    
    log_callback(f"Starting to scrape {total_urls} {url_type} URLs...")
    
    # Process each URL
    for i, url in enumerate(urls_to_process):
        try:
            # Update progress if callback provided
            if progress_callback:
                progress_callback(i / total_urls * 100)
                
            log_callback(f"Scraping {url_type} URL ({i+1}/{total_urls}): {url}")
            
            # Scrape based on URL type
            data = None
            if url_type == 'horses':
                data = scrape_horse_page(url)
            elif url_type == 'jockeys':
                data = scrape_jockey_page(url)
            elif url_type == 'trainers':
                data = scrape_trainer_page(url)
            elif url_type == 'races':
                data = scrape_race_page(url)
                
            # Check if we got data
            if data:
                results.append(data)
                success_count += 1
                
                # Update database status
                cursor = conn.cursor()
                timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                cursor.execute(
                    "UPDATE urls SET date_accessed = ?, status = ? WHERE url = ?",
                    (timestamp, "succeeded", url)
                )
                
                # Save data to appropriate table
                if url_type == 'horses':
                    cursor.execute(
                        "INSERT OR IGNORE INTO horses (ID, Name) VALUES (?, ?)",
                        (data['id'], data['name'])
                    )
                elif url_type == 'jockeys':
                    cursor.execute(
                        "INSERT OR IGNORE INTO jockeys (ID, Name) VALUES (?, ?)",
                        (data['id'], data['name'])
                    )
                elif url_type == 'trainers':
                    cursor.execute(
                        "INSERT OR IGNORE INTO trainers (ID, Name) VALUES (?, ?)",
                        (data['id'], data['name'])
                    )
                
                conn.commit()
                log_callback(f"Saved {url_type[:-1]} {data.get('name', '')}")
            else:
                # Update database with failed status
                cursor = conn.cursor()
                timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                cursor.execute(
                    "UPDATE urls SET date_accessed = ?, status = ? WHERE url = ?",
                    (timestamp, "failed", url)
                )
                conn.commit()
                log_callback(f"Failed to scrape {url_type} URL: {url}")
                
            # Increment count
            count += 1
            
        except Exception as e:
            log_callback(f"Error processing {url}: {str(e)}")
            # Update database with failed status
            try:
                cursor = conn.cursor()
                timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                cursor.execute(
                    "UPDATE urls SET date_accessed = ?, status = ? WHERE url = ?",
                    (timestamp, "failed", url)
                )
                conn.commit()
            except Exception as db_error:
                log_callback(f"Error updating URL status: {str(db_error)}")
                
            count += 1
    
    # Close connection if we opened it
    if close_conn and conn:
        conn.close()
    
    # Final progress update
    if progress_callback:
        progress_callback(100)
        
    log_callback(f"Finished scraping {count} {url_type} URLs. Successfully scraped: {success_count}")
    
    return results

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
            self.log(f"Icon error: {e}")
        
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
        
        # Add limit control
        ttk.Label(scraper_frame, text="URL Limit:").grid(row=0, column=0, sticky=tk.W, padx=5, pady=5)
        self.url_limit_var = tk.IntVar(value=50)
        url_limit_spinner = ttk.Spinbox(scraper_frame, from_=1, to=1000, increment=10, textvariable=self.url_limit_var, width=5)
        url_limit_spinner.grid(row=0, column=1, sticky=tk.W, padx=5, pady=5)
        
        # Add timeout control for scraping
        ttk.Label(scraper_frame, text="Timeout (mins):").grid(row=1, column=0, sticky=tk.W, padx=5, pady=5)
        self.scrape_timeout_var = tk.IntVar(value=5)
        scrape_timeout_spinner = ttk.Spinbox(scraper_frame, from_=1, to=60, increment=1, textvariable=self.scrape_timeout_var, width=5)
        scrape_timeout_spinner.grid(row=1, column=1, sticky=tk.W, padx=5, pady=5)
        
        ttk.Button(scraper_frame, text="Scrape URLs", command=self.scrape_urls).grid(row=2, column=0, padx=5, pady=5)
        
        self.scrape_stats_var = StringVar(value="Not started")
        ttk.Label(scraper_frame, textvariable=self.scrape_stats_var).grid(row=2, column=1, sticky=tk.W, padx=5, pady=5)
        
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
            
            if not all_tables_exist:
                missing_tables = [table for table in required_tables if table not in tables]
                self.log(f"Missing required tables: {', '.join(missing_tables)}")
            
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
            # Use check_same_thread=False to allow connections to be used across threads
            conn = sqlite3.connect(db_path, check_same_thread=False)
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
        State is inferred from the database rather than an external file.
        """
        base_url = self.base_url_var.get()
        max_urls = self.max_urls_var.get()
        timeout_mins = self.timeout_var.get()
        saturation_threshold = self.saturation_var.get()
        window_size = self.window_size_var.get()
        
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
                
                # Check if required tables exist
                if not self.check_database_structure():
                    self.log("Database is missing required tables. Cannot proceed with crawl.")
                    self.crawl_stats_var.set("Error: Missing required tables")
                    return
                
                # Get existing URLs from database
                cursor = conn.cursor()
                cursor.execute("SELECT url, status, type FROM urls")
                url_data = cursor.fetchall()
                
                # Prepare data structures
                existing_urls = {row[0]: row[1] for row in url_data}
                visited_urls = set()
                discovered_urls = []
                
                # Initialize visited_urls based on processed URLs in database
                for url, status, _ in url_data:
                    if status in ['succeeded', 'failed', 'processed']:
                        visited_urls.add(url)
                
                # Determine URLs to visit (unprocessed URLs + base_url if needed)
                urls_to_visit = [
                    row[0] for row in url_data 
                    if row[1] == 'unprocessed' and row[0] not in visited_urls
                ]
                
                # Always ensure base_url is in the queue if it's not already visited
                if base_url not in visited_urls and base_url not in urls_to_visit:
                    urls_to_visit.insert(0, base_url)
                
                self.log(f"Loaded crawl state from database: {len(visited_urls)} visited URLs, {len(urls_to_visit)} URLs to visit")
                
                # If no URLs to visit, start with base_url
                if not urls_to_visit:
                    urls_to_visit = [base_url]
                
                # Define URL patterns for each type
                url_patterns = {
                    'jockeys': re.compile(r'/racing/profiles/jockey/\d+$'),
                    'trainers': re.compile(r'/racing/profiles/trainer/\d+$'),
                    'horses': re.compile(r'/racing/profiles/horse/\d+$'),
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
                    
                    # URLs automatically saved to database as discovered
                    
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
                    
                    # Log progress periodically at page intervals
                    if pages_visited % 100 == 0:
                        # No need to save state to file - using database for persistence
                        self.log(f"Progress: {pages_visited} pages visited, {len(discovered_urls)} URLs discovered to database")
                    
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
                
                # Database contains all discovered URLs - state is preserved there for future crawls
                
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
        Scrape URLs from the database based on type and status filters,
        then scrape and process them.
        """
        # Get parameters from UI
        url_limit = self.url_limit_var.get()
        timeout_mins = self.scrape_timeout_var.get()
        
        def run_scraper():
            try:
                # Initialize variables
                start_time = time.time()
                timeout_seconds = timeout_mins * 60
                scraped_count = 0
                self.progress_var.set(0)
                
                # Update UI
                self.scrape_stats_var.set("Scraping in progress...")
                self.log(f"Starting scrape of unprocessed URLs with limit {url_limit} and timeout {timeout_mins} mins")
                
                # Connect to database
                conn = self.connect_to_database()
                if not conn:
                    self.log("Failed to connect to database")
                    self.scrape_stats_var.set("Error: Database connection failed")
                    return
                
                # Get unprocessed URLs from database
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT url, type FROM urls WHERE status = 'unprocessed' LIMIT ?", 
                    (url_limit,)
                )
                urls_to_scrape = [(row[0], row[1]) for row in cursor.fetchall()]
                
                if not urls_to_scrape:
                    self.log("No unprocessed URLs found in the database")
                    self.scrape_stats_var.set("No URLs found")
                    conn.close()
                    return
                
                self.log(f"Found {len(urls_to_scrape)} unprocessed URLs to scrape")
                
                # Group URLs by type for easier processing
                urls_by_type = {}
                for url, url_type in urls_to_scrape:
                    if url_type not in urls_by_type:
                        urls_by_type[url_type] = []
                    urls_by_type[url_type].append(url)
                
                total_urls = len(urls_to_scrape)
                processed_urls = 0
                
                # Process each URL until timeout or limit reached
                for url, url_type in urls_to_scrape:
                    # Check for timeout
                    if time.time() - start_time > timeout_seconds:
                        self.log("Timeout reached. Stopping scraping.")
                        break
                    
                    # Skip race URLs as requested
                    if url_type == 'races':
                        self.log(f"Skipping race URL (not implemented): {url}")
                        processed_urls += 1
                        self.progress_var.set(int((processed_urls / total_urls) * 100))
                        continue
                    
                    try:
                        self.log(f"Scraping {url_type} URL: {url}")
                        
                        data = None
                        if url_type == 'trainers':
                            data = self.scrape_trainer_page(url)
                        elif url_type == 'jockeys':
                            data = self.scrape_jockey_page(url)
                        elif url_type == 'horses':
                            data = self.scrape_horse_page(url)
                        
                        # Update database with scraped status
                        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        if data:
                            cursor.execute(
                                "UPDATE urls SET date_accessed = ?, status = ? WHERE url = ?",
                                (timestamp, "succeeded", url)
                            )
                            self.log(f"Successfully scraped {url_type}: {data.get('name', '')}")
                            
                            # Save data to appropriate table
                            self.save_entity_to_database(conn, data, url_type)
                            scraped_count += 1
                        else:
                            cursor.execute(
                                "UPDATE urls SET date_accessed = ?, status = ? WHERE url = ?",
                                (timestamp, "failed", url)
                            )
                            self.log(f"Failed to scrape {url_type} URL: {url}")
                        
                        conn.commit()
                        
                    except Exception as e:
                        self.log(f"Error scraping {url}: {str(e)}")
                        # Mark as failed in database
                        try:
                            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            cursor.execute(
                                "UPDATE urls SET date_accessed = ?, status = ? WHERE url = ?",
                                (timestamp, "failed", url)
                            )
                            conn.commit()
                        except Exception as db_error:
                            self.log(f"Error updating URL status: {str(db_error)}")
                    
                    # Update progress
                    processed_urls += 1
                    self.progress_var.set(int((processed_urls / total_urls) * 100))
                    
                    # Update stats in UI
                    self.scrape_stats_var.set(f"Scraped: {processed_urls}/{total_urls} URLs")
                    self.root.update()
                    
                    # Add small delay to avoid overloading server
                    time.sleep(random.uniform(0.2, 0.5))
                
                # Close database connection
                conn.close()
                
                # Final update
                elapsed_time = time.time() - start_time
                self.log(f"Scraping completed in {elapsed_time:.1f} seconds")
                self.log(f"Processed {processed_urls} URLs, successfully scraped {scraped_count}")
                self.scrape_stats_var.set(f"Completed: {scraped_count}/{processed_urls} URLs scraped")
                
                # Update URL statistics
                self.connect_to_db()  # Reconnect to refresh stats
                
            except Exception as e:
                self.log(f"Error during scraping: {str(e)}")
                self.scrape_stats_var.set(f"Error: {str(e)}")
                self.log(traceback.format_exc())
        
        # Run scraper in a separate thread
        threading.Thread(target=run_scraper, daemon=True).start()
    
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
            
            # Check if urls table exists
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='urls'")
            if not cursor.fetchone():
                raise Exception("Required table 'urls' does not exist in the database")
                
            # Get current timestamp
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # Define URL patterns for each type with less restrictive matching
            url_patterns = {
                'jockeys': re.compile(r'/racing/profiles/jockey/\d+$'),
                'trainers': re.compile(r'/racing/profiles/trainer/\d+$'),
                'horses': re.compile(r'/racing/profiles/horse/\d+$'),
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
        ordered_statuses = ["unprocessed", "failed", "succeeded", "all"]
        
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

    def scrape_trainer_page(self, url):
        """
        Scrape a trainer profile page to extract trainer information
        
        Args:
            url (str): URL of the trainer profile page
            
        Returns:
            dict: Extracted trainer data or None if scraping failed
        """
        try:
            response = requests.get(url, headers=HEADERS)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Extract trainer ID from URL
            trainer_id = int(url.split('/')[-1])
            
            # Extract trainer name from the page header
            trainer_name = soup.find('h1').text.strip() if soup.find('h1') else None
            
            if not trainer_name:
                print(f"Could not find trainer name at URL: {url}")
                return None
                
            return {
                'id': trainer_id,
                'name': trainer_name
            }
            
        except Exception as e:
            print(f"Error scraping trainer page {url}: {str(e)}")
            return None
    
    def scrape_jockey_page(self, url):
        """
        Scrape a jockey profile page to extract jockey information
        
        Args:
            url (str): URL of the jockey profile page
            
        Returns:
            dict: Extracted jockey data or None if scraping failed
        """
        try:
            response = requests.get(url, headers=HEADERS)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Extract jockey ID from URL
            jockey_id = int(url.split('/')[-1])
            
            # Extract jockey name from the page header
            jockey_name = soup.find('h1').text.strip() if soup.find('h1') else None
            
            if not jockey_name:
                print(f"Could not find jockey name at URL: {url}")
                return None
                
            return {
                'id': jockey_id,
                'name': jockey_name
            }
            
        except Exception as e:
            print(f"Error scraping jockey page {url}: {str(e)}")
            return None
    
    def scrape_horse_page(self, url):
        """
        Scrape a horse profile page to extract horse information
        
        Args:
            url (str): URL of the horse profile page
            
        Returns:
            dict: Extracted horse data or None if scraping failed
        """
        try:
            response = requests.get(url, headers=HEADERS)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Extract horse ID from URL
            horse_id = int(url.split('/')[-1])
            
            # Extract horse name from the page header
            horse_name = soup.find('h1').text.strip() if soup.find('h1') else None
            
            if not horse_name:
                print(f"Could not find horse name at URL: {url}")
                return None
                
            return {
                'id': horse_id,
                'name': horse_name
            }
            
        except Exception as e:
            print(f"Error scraping horse page {url}: {str(e)}")
            return None
    
    def save_entity_to_database(self, conn, data, data_type):
        """
        Save entity data to the appropriate database table
        
        Args:
            conn (sqlite3.Connection): Database connection
            data (dict): Entity data to save
            data_type (str): Type of data ('horses', 'jockeys', 'trainers')
            
        Returns:
            bool: True if saved successfully, False otherwise
        """
        if not data or not conn:
            return False
            
        cursor = conn.cursor()
        
        try:
            if data_type == 'trainers':
                # Check if trainers table exists
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='trainers'")
                if not cursor.fetchone():
                    raise Exception("Required table 'trainers' does not exist in the database")
                
                cursor.execute(
                    "INSERT OR IGNORE INTO trainers (ID, Name) VALUES (?, ?)",
                    (data['id'], data['name'])
                )
                self.log(f"Saved trainer: {data['name']} (ID: {data['id']})")
                
            elif data_type == 'jockeys':
                # Check if jockeys table exists
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='jockeys'")
                if not cursor.fetchone():
                    raise Exception("Required table 'jockeys' does not exist in the database")
                
                cursor.execute(
                    "INSERT OR IGNORE INTO jockeys (ID, Name) VALUES (?, ?)",
                    (data['id'], data['name'])
                )
                self.log(f"Saved jockey: {data['name']} (ID: {data['id']})")
                
            elif data_type == 'horses':
                # Check if horses table exists
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='horses'")
                if not cursor.fetchone():
                    raise Exception("Required table 'horses' does not exist in the database")
                
                # Get existing columns to handle column differences
                cursor.execute("PRAGMA table_info(horses)")
                existing_columns = [column[1].lower() for column in cursor.fetchall()]
                
                # Build dynamic query based on existing columns
                columns = ['ID', 'Name']
                values = [data['id'], data['name']]
                
                # Map data to available columns
                column_map = {
                    'foaled': 'Foaled',
                    'sex': 'Sex',
                    'trainer': 'Trainer',
                    'trainer_id': 'TrainerID',
                    'sire': 'Sire',
                    'sire_id': 'SireID',
                    'dam': 'Dam', 
                    'dam_id': 'DamID',
                    'owner': 'Owner'
                }
                
                for data_key, db_column in column_map.items():
                    if db_column.lower() in existing_columns and data.get(data_key) is not None:
                        columns.append(db_column)
                        values.append(data.get(data_key))
                
                # Construct the SQL query
                placeholders = ', '.join(['?'] * len(values))
                column_str = ', '.join(columns)
                
                cursor.execute(
                    f"INSERT OR REPLACE INTO horses ({column_str}) VALUES ({placeholders})",
                    values
                )
                self.log(f"Saved horse: {data['name']} (ID: {data['id']})")
            
            conn.commit()
            return True
            
        except Exception as e:
            self.log(f"Error saving {data_type} data: {str(e)}")
            conn.rollback()
            return False

def delete_crawl_state_file(filename="crawl_state.json"):
    """
    Delete crawl state file if it exists
    
    Args:
        filename (str): File to delete
        
    Returns:
        bool: True if file was deleted, False if it doesn't exist
    """
    try:
        if os.path.isfile(filename):
            os.remove(filename)
            print(f"Deleted crawl state file: {filename}")
            return True
        return False
    except Exception as e:
        print(f"Error deleting crawl state file: {str(e)}")
        return False

# Try to delete the state file at import time
delete_crawl_state_file()

# === Main Entry Point ===

if __name__ == "__main__":
    root = tk.Tk()
    app = CollectorUI(root)
    root.mainloop() 
