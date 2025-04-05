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
import traceback

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

def crawl_website(base_url, max_urls=1000, data_type=None, log_callback=None, progress_callback=None, conn=None):
    """
    Crawl a website to find all sub-URLs to any depth
    
    Args:
        base_url (str): The URL to start crawling from
        max_urls (int): Maximum number of URLs to collect
        data_type (str): Type of URLs to look for ('races', 'horses', 'jockeys', 'trainers')
        log_callback (callable): Optional callback function to log messages in real-time
        progress_callback (callable): Optional callback function to update progress
        conn (sqlite3.Connection): Database connection to check existing URLs
        
    Returns:
        list: List of discovered URLs
    """
    discovered_urls = []
    visited_urls = set()
    urls_to_visit = [base_url]
    
    # Get already captured URLs from database
    captured_urls = {}
    if conn:
        captured_urls = get_captured_urls(conn)
        
    # Log function (use callback if provided, otherwise print)
    def log(message, verbose=False):
        if not verbose and log_callback:
            log_callback(message)
        elif not verbose:
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
        'horses': r'https?://www\.sportinglife\.com/racing/profiles/horse/\d+$'
    }
    
    # URL pages to exclude
    exclude_patterns = {
        'trainers': ['/future-entries', '/form'],
        'jockeys': ['/future-entries', '/form'],
        'horses': ['/form'],
        'races': []
    }
    
    # Function to check if a race page has published results
    def has_published_results(html_content):
        """Check if the race page contains published results"""
        if not html_content or data_type != 'races':
            return True  # Not a race page or no content, skip this check
            
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Look for result indicators
        result_indicators = [
            # Table with race results
            soup.find('table', class_='ui-table'),
            # "Result" text
            soup.find(string=re.compile(r'Result', re.IGNORECASE)),
            # Winner details
            soup.find(string=re.compile(r'Winner', re.IGNORECASE))
        ]
        
        # Check if we found any result indicators
        return any(indicator is not None for indicator in result_indicators)
    
    active_pattern = url_patterns.get(data_type)
    if not active_pattern:
        log(f"Error: Invalid data type '{data_type}'")
        return []
    
    # Handle URLs with numerical IDs (trainers, jockeys, horses)
    if data_type in ['trainers', 'jockeys', 'horses'] and any(base_url.endswith(f"{data_type[:-1]}/") for ending in ['trainer/', 'jockey/', 'horse/']):
        # Clear the visit queue to use our optimized approach
        urls_to_visit = []
        # Start with ID 1 and increment
        for item_id in range(1, max_urls + 1):
            item_url = f"{base_url}{item_id}"
            # Skip URLs that have already been successfully processed
            if item_url in captured_urls and captured_urls[item_url] == 1:
                continue
            urls_to_visit.append(item_url)
        log(f"Generated {len(urls_to_visit)} direct {data_type} URLs to check (excluding already captured)")
    
    excluded_patterns = exclude_patterns.get(data_type, [])
    log(f"Starting crawl. Looking for {data_type} URLs from {base_url}")
    log(f"Excluding URLs containing: {excluded_patterns}")
    log(f"Skipping {len([u for u, s in captured_urls.items() if s == 1])} URLs already successfully captured")
    
    if data_type == 'races':
        log("For races, only including URLs with published results")
    
    # Summary statistics
    total_to_check = min(max_urls * 5, len(urls_to_visit) if urls_to_visit else 5000)  # Estimate
    pages_checked = 0
    skipped_count = 0
    skipped_no_results = 0
    last_summary_time = time.time()
    summary_interval = 2  # seconds
    match_count = 0
    
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
            
        # Skip if URL contains excluded patterns
        if any(excluded in current_url for excluded in excluded_patterns):
            continue
        
        visited_urls.add(current_url)
        pages_checked += 1
        
        # Update progress bar regularly
        update_progress(pages_checked, total_to_check)
        
        # Show periodic summary instead of per-URL logging
        current_time = time.time()
        if current_time - last_summary_time >= summary_interval:
            summary_msg = (f"Progress: Checked {pages_checked} pages, found {len(discovered_urls)} matching URLs, "
                          f"skipped {skipped_count} already captured")
            if data_type == 'races':
                summary_msg += f", skipped {skipped_no_results} without results"
            summary_msg += f", {len(urls_to_visit)} remaining in queue"
            log(summary_msg)
            last_summary_time = current_time
        
        try:
            # Add some delay to avoid overwhelming the server
            time.sleep(random.uniform(0.2, 0.5))
            
            # Make request
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            response = requests.get(current_url, headers=headers, timeout=10)
            
            # Skip pages that don't exist (404) or other errors
            if response.status_code != 200:
                # Not logging every failed URL
                continue
            
            # For race URLs, check if results are published
            if data_type == 'races' and re.match(active_pattern, current_url):
                if not has_published_results(response.text):
                    skipped_no_results += 1
                    continue  # Skip races without published results
                
            # Check if URL matches the pattern
            if re.match(active_pattern, current_url) and current_url not in discovered_urls:
                discovered_urls.append(current_url)
                match_count += 1
                
                # Only log every 5th match to reduce verbosity
                if match_count % 5 == 0:
                    log(f"Found {match_count} matching {data_type} URLs so far")
                
                # If we've reached max_urls, stop
                if len(discovered_urls) >= max_urls:
                    break
                
            # For specific data types using numerical IDs, we might not need to parse the page
            if data_type in ['trainers', 'jockeys', 'horses'] and any(
                base_url.endswith(ending) for ending in ['trainer/', 'jockey/', 'horse/']):
                # Skip parsing for direct numeric traversal
                continue
                
            # Parse HTML
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Find all links
            for link in soup.find_all('a', href=True):
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
                    
                # Skip URLs with excluded patterns
                if any(excluded in href for excluded in excluded_patterns):
                    continue
                
                # Normalize URL by removing trailing slash
                href = href.rstrip('/')
                
                # Skip URLs already successfully captured
                if href in captured_urls and captured_urls[href] == 1:
                    continue
                
                # For race URLs, we need to check content before adding to discovered URLs
                # but since we haven't visited the URL yet, we'll add it to urls_to_visit
                # and let the main loop check if it has published results
                
                # Check if URL matches the pattern for the desired type
                if re.match(active_pattern, href) and href not in discovered_urls:
                    if data_type != 'races':
                        # For non-race URLs, add directly to discovered_urls
                        discovered_urls.append(href)
                        match_count += 1
                    # For race URLs, we'll check when we visit the page
                    
                    # If we've reached max_urls, stop
                    if len(discovered_urls) >= max_urls:
                        break
                
                # Add to visit queue if not already visited or queued
                if href not in visited_urls and href not in urls_to_visit:
                    urls_to_visit.append(href)
            
        except Exception as e:
            # Only log occasional errors to reduce verbosity
            if random.random() < 0.1:  # Log roughly 10% of errors
                log(f"Error processing URL: {str(e)}")
    
    # Set final progress
    update_progress(100, 100)
    
    # Final summary
    log(f"Crawl complete. Found {len(discovered_urls)} matching {data_type} URLs after checking {pages_checked} pages")
    log(f"Skipped {skipped_count} URLs already successfully captured")
    if data_type == 'races':
        log(f"Skipped {skipped_no_results} race pages without published results")
    if len(discovered_urls) >= max_urls:
        log(f"Reached maximum URL limit of {max_urls}")
        
    # Save discovered URLs to database if provided
    if conn and discovered_urls:
        added_count = save_urls_to_database(conn, discovered_urls, 1)
        log(f"Added {added_count} new URLs to database")
        
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
        dict: Dictionary of URLs and their success status {url: success}
    """
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT url, success FROM urls")
        return {row[0]: row[1] for row in cursor.fetchall()}
    except Exception as e:
        print(f"Error fetching URLs from database: {str(e)}")
        return {}

def save_urls_to_database(conn, urls, success=0):
    """
    Save a list of URLs to the database
    
    Args:
        conn (sqlite3.Connection): Database connection
        urls (list): List of URLs to save
        success (int): Success status to set for the URLs (1 for success, 0 for not processed/failed)
        
    Returns:
        int: Number of URLs added
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
            success BOOLEAN,
            type TEXT
        )
        ''')
        
        # Get current timestamp
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Use executemany for better performance
        rows = [(url, timestamp, success, get_url_type(url)) for url in urls]
        cursor.executemany(
            "INSERT OR IGNORE INTO urls (url, date_accessed, success, type) VALUES (?, ?, ?, ?)",
            rows
        )
        
        conn.commit()
        return cursor.rowcount
    except Exception as e:
        print(f"Error saving URLs to database: {str(e)}")
        return 0

def update_url_success(conn, url, success):
    """
    Update the success status of a URL in the database
    
    Args:
        conn (sqlite3.Connection): Database connection
        url (str): URL to update
        success (int): New success status (1 for success, 0 for failed)
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        cursor = conn.cursor()
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        cursor.execute(
            "UPDATE urls SET success = ?, date_accessed = ? WHERE url = ?",
            (success, timestamp, url)
        )
        conn.commit()
        return True
    except Exception as e:
        print(f"Error updating URL success status: {str(e)}")
        return False

def get_url_type(url):
    """Determine the type of URL based on its pattern"""
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

def scrape_trainer_page(url):
    """
    Scrape a trainer profile page to extract trainer information
    
    Args:
        url (str): URL of the trainer profile page
        
    Returns:
        dict: Extracted trainer data or None if scraping failed
    """
    try:
        response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
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

def scrape_jockey_page(url):
    """
    Scrape a jockey profile page to extract jockey information
    
    Args:
        url (str): URL of the jockey profile page
        
    Returns:
        dict: Extracted jockey data or None if scraping failed
    """
    try:
        response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
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

def scrape_horse_page(url):
    """
    Scrape a horse profile page to extract horse information
    
    Args:
        url (str): URL of the horse profile page
        
    Returns:
        dict: Extracted horse data or None if scraping failed
    """
    try:
        response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0'})
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Extract horse ID from URL
        horse_id = int(url.split('/')[-1])
        
        # Extract horse name from the page header
        horse_name = soup.find('h1').text.strip() if soup.find('h1') else None
        
        if not horse_name:
            print(f"Could not find horse name at URL: {url}")
            return None
            
        # Initialize horse data dictionary
        horse_data = {
            'id': horse_id,
            'name': horse_name,
            'foaled': None,
            'sex': None,
            'trainer': None,
            'trainer_id': None,
            'sire': None,
            'sire_id': None,
            'dam': None,
            'dam_id': None,
            'owner': None
        }
        
        # Look for details in horse profile table
        # Most horse profile pages have a table with key details
        detail_tables = soup.find_all('table')
        for table in detail_tables:
            rows = table.find_all('tr')
            for row in rows:
                cols = row.find_all(['td', 'th'])
                if len(cols) >= 2:
                    label = cols[0].text.strip().lower()
                    value = cols[1].text.strip()
                    
                    if 'age' in label or 'foaled' in label:
                        # Extract foaled date from format like "12 (Foaled 17th September 2013)"
                        foaled_match = re.search(r'Foaled\s+(.+)\)', value)
                        if foaled_match:
                            horse_data['foaled'] = foaled_match.group(1).strip()
                    elif 'trainer' in label:
                        horse_data['trainer'] = value
                        # Try to extract trainer ID from any links
                        trainer_link = cols[1].find('a', href=True)
                        if trainer_link and 'profiles/trainer' in trainer_link['href']:
                            try:
                                trainer_id = int(trainer_link['href'].split('/')[-1])
                                horse_data['trainer_id'] = trainer_id
                            except (ValueError, IndexError):
                                pass
                    elif 'sex' in label:
                        horse_data['sex'] = value
                    elif 'sire' in label:
                        horse_data['sire'] = value
                        # Try to extract sire ID from any links
                        sire_link = cols[1].find('a', href=True)
                        if sire_link and 'profiles/horse' in sire_link['href']:
                            try:
                                sire_id = int(sire_link['href'].split('/')[-1])
                                horse_data['sire_id'] = sire_id
                            except (ValueError, IndexError):
                                pass
                    elif 'dam' in label:
                        horse_data['dam'] = value
                        # Try to extract dam ID from any links
                        dam_link = cols[1].find('a', href=True)
                        if dam_link and 'profiles/horse' in dam_link['href']:
                            try:
                                dam_id = int(dam_link['href'].split('/')[-1])
                                horse_data['dam_id'] = dam_id
                            except (ValueError, IndexError):
                                pass
                    elif 'owner' in label:
                        horse_data['owner'] = value
        
        # If we still don't have complete information, try alternative methods
        if not any([horse_data['foaled'], horse_data['sex'], horse_data['trainer']]):
            # Look for specific information in different page layouts
            print(f"Using fallback methods to extract horse details for {horse_name}")
            
            # Try to find details in sections with dt/dd or label/value pairs
            detail_sections = soup.find_all(['dl', 'div', 'ul'], class_=['details', 'info', 'profile'])
            for section in detail_sections:
                # Look for dt/dd pairs
                terms = section.find_all('dt')
                for term in terms:
                    label = term.text.strip().lower()
                    value_elem = term.find_next('dd')
                    if value_elem:
                        value = value_elem.text.strip()
                        
                        if 'age' in label or 'foaled' in label:
                            foaled_match = re.search(r'Foaled\s+(.+)\)', value)
                            if foaled_match:
                                horse_data['foaled'] = foaled_match.group(1).strip()
                        elif 'trainer' in label:
                            horse_data['trainer'] = value
                        elif 'sex' in label:
                            horse_data['sex'] = value
                        elif 'sire' in label:
                            horse_data['sire'] = value
                        elif 'dam' in label:
                            horse_data['dam'] = value
                        elif 'owner' in label:
                            horse_data['owner'] = value
        
        # Check if we have at least some basic information
        if not any([horse_data['foaled'], horse_data['sex'], horse_data['trainer'], horse_data['sire'], horse_data['dam']]):
            print(f"Warning: Unable to extract complete horse data for {horse_name}")
            
            # Last resort: scan the entire HTML for any mention of the critical fields
            html_text = soup.get_text()
            
            # Look for Age/Foaled pattern (common format)
            age_pattern = r'Age\s*\d+\s*\(Foaled\s+([^)]+)\)'
            age_match = re.search(age_pattern, html_text)
            if age_match:
                horse_data['foaled'] = age_match.group(1).strip()
            
            # Look for common field patterns
            field_patterns = {
                'sex': r'Sex\s*[:]\s*([^\n]+)',
                'trainer': r'Trainer\s*[:]\s*([^\n]+)',
                'sire': r'Sire\s*[:]\s*([^\n]+)',
                'dam': r'Dam\s*[:]\s*([^\n]+)',
                'owner': r'Owner\s*[:]\s*([^\n]+)',
            }
            
            for field, pattern in field_patterns.items():
                match = re.search(pattern, html_text)
                if match and not horse_data[field]:
                    horse_data[field] = match.group(1).strip()
        
        print(f"Extracted horse data: {horse_data}")
        return horse_data
        
    except Exception as e:
        print(f"Error scraping horse page {url}: {str(e)}")
        traceback.print_exc()
        return None

def scrape_urls_by_type(urls, url_type, limit, log_callback=None, progress_callback=None, conn=None):
    """
    Scrape a limited number of URLs of a specific type
    
    Args:
        urls (list): List of URLs to scrape
        url_type (str): Type of URLs to scrape ('races', 'horses', 'jockeys', 'trainers')
        limit (int): Maximum number of URLs to scrape
        log_callback (callable): Optional callback function to log messages in real-time
        progress_callback (callable): Optional callback function to update progress
        conn (sqlite3.Connection): Database connection to record URL status
        
    Returns:
        list: Scraped data
    """
    if not urls:
        return []
        
    # Limit the number of URLs to scrape
    urls_to_scrape = urls[:limit]
    
    # Initialize list to store scraped data
    scraped_data = []
    
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
            
    log(f"Starting scraping of {len(urls_to_scrape)} {url_type} URLs")
    
    for i, url in enumerate(urls_to_scrape):
        try:
            # Update progress
            update_progress(i, len(urls_to_scrape))
            
            success = False
            data = None
            
            # Scrape based on URL type
            if url_type == 'trainers':
                log(f"Scraping trainer data from: {url}")
                data = scrape_trainer_page(url)
            elif url_type == 'jockeys':
                log(f"Scraping jockey data from: {url}")
                data = scrape_jockey_page(url)
            elif url_type == 'horses':
                log(f"Scraping horse data from: {url}")
                data = scrape_horse_page(url)
            # Add other types here when implemented
            # elif url_type == 'races':
            #     data = scrape_race_page(url)
            
            if data:
                scraped_data.append(data)
                success = True
                log(f"Successfully scraped {url_type} data: {data['name']}")
            else:
                log(f"Failed to scrape data from {url}")
                
            # Record URL in database regardless of success
            if conn:
                # Get current timestamp
                timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                cursor = conn.cursor()
                
                # Check if URL already exists in the database
                cursor.execute("SELECT url FROM urls WHERE url = ?", (url,))
                if cursor.fetchone():
                    # Update existing URL
                    log(f"Updating existing URL record: {url}")
                    cursor.execute(
                        "UPDATE urls SET date_accessed = ?, success = ?, type = ? WHERE url = ?",
                        (timestamp, 1 if success else 0, url_type, url)
                    )
                else:
                    # Insert new URL
                    log(f"Adding new URL record: {url}")
                    cursor.execute(
                        "INSERT INTO urls (url, date_accessed, success, type) VALUES (?, ?, ?, ?)",
                        (url, timestamp, 1 if success else 0, url_type)
                    )
                conn.commit()
                
        except Exception as e:
            log(f"Error processing {url}: {str(e)}")
            if conn:
                try:
                    # Record error in URL table
                    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    cursor = conn.cursor()
                    
                    # Check if URL already exists
                    cursor.execute("SELECT url FROM urls WHERE url = ?", (url,))
                    if cursor.fetchone():
                        cursor.execute(
                            "UPDATE urls SET date_accessed = ?, success = ?, type = ? WHERE url = ?",
                            (timestamp, 0, url_type, url)
                        )
                    else:
                        cursor.execute(
                            "INSERT INTO urls (url, date_accessed, success, type) VALUES (?, ?, ?, ?)",
                            (url, timestamp, 0, url_type)
                        )
                    conn.commit()
                except Exception as db_error:
                    log(f"Error recording URL failure in database: {str(db_error)}")
    
    # Final progress update
    update_progress(len(urls_to_scrape), len(urls_to_scrape))
    
    log(f"Scraping complete. Scraped {len(scraped_data)} {url_type} URLs successfully.")
    return scraped_data

def process_scraped_data(data, data_type):
    """
    Process scraped data based on its type
    
    Args:
        data (list): Scraped data to process
        data_type (str): Type of data ('races', 'horses', 'jockeys', 'trainers')
        
    Returns:
        list: Processed data ready for database insertion
    """
    if not data:
        return []
        
    processed_data = []
    
    if data_type == 'trainers':
        # For trainers, we just need to format the data correctly
        for trainer in data:
            processed_data.append({
                'id': trainer['id'],
                'name': trainer['name']
            })
    elif data_type == 'jockeys':
        # For jockeys, we also just need to format the data correctly
        for jockey in data:
            processed_data.append({
                'id': jockey['id'],
                'name': jockey['name']
            })
    elif data_type == 'horses':
        # For horses, include all the extracted fields
        for horse in data:
            processed_data.append({
                'id': horse['id'],
                'name': horse['name'],
                'foaled': horse['foaled'],
                'sex': horse['sex'],
                'trainer': horse['trainer'],
                'trainer_id': horse['trainer_id'],
                'sire': horse['sire'],
                'sire_id': horse['sire_id'],
                'dam': horse['dam'],
                'dam_id': horse['dam_id'],
                'owner': horse['owner']
            })
    # Add other data types here when implemented
    # elif data_type == 'races':
    #    ...
    
    return processed_data

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
    if not data or not conn:
        return 0
        
    cursor = conn.cursor()
    records_saved = 0
    
    try:
        if data_type == 'trainers':
            # Save trainer data to the trainers table
            for trainer in data:
                try:
                    cursor.execute(
                        "INSERT OR IGNORE INTO trainers (ID, Name) VALUES (?, ?)",
                        (trainer['id'], trainer['name'])
                    )
                    if cursor.rowcount > 0:
                        records_saved += 1
                except Exception as e:
                    print(f"Error saving trainer {trainer['name']}: {str(e)}")
        elif data_type == 'jockeys':
            # Save jockey data to the jockeys table
            for jockey in data:
                try:
                    cursor.execute(
                        "INSERT OR IGNORE INTO jockeys (ID, Name) VALUES (?, ?)",
                        (jockey['id'], jockey['name'])
                    )
                    if cursor.rowcount > 0:
                        records_saved += 1
                except Exception as e:
                    print(f"Error saving jockey {jockey['name']}: {str(e)}")
        elif data_type == 'horses':
            # First ensure the horses table has all needed columns
            try:
                # Check if we need to alter the table to add new columns
                cursor.execute("PRAGMA table_info(horses)")
                existing_columns = [column[1].lower() for column in cursor.fetchall()]
                
                # Add columns if they don't exist
                if 'sex' not in existing_columns:
                    cursor.execute("ALTER TABLE horses ADD COLUMN Sex TEXT")
                if 'trainerid' not in existing_columns:
                    cursor.execute("ALTER TABLE horses ADD COLUMN TrainerID INTEGER")
                if 'sireid' not in existing_columns:
                    cursor.execute("ALTER TABLE horses ADD COLUMN SireID INTEGER")
                if 'damid' not in existing_columns:
                    cursor.execute("ALTER TABLE horses ADD COLUMN DamID INTEGER")
                if 'sire' not in existing_columns:
                    cursor.execute("ALTER TABLE horses ADD COLUMN Sire TEXT")
                if 'dam' not in existing_columns:
                    cursor.execute("ALTER TABLE horses ADD COLUMN Dam TEXT")
                if 'trainer' not in existing_columns:
                    cursor.execute("ALTER TABLE horses ADD COLUMN Trainer TEXT")
                
                conn.commit()
            except Exception as e:
                print(f"Error updating horses table schema: {str(e)}")
            
            # Save horse data to the horses table
            for horse in data:
                try:
                    # Print debug info
                    print(f"Attempting to save horse: {horse['id']} - {horse['name']}")
                    
                    cursor.execute(
                        """
                        INSERT OR REPLACE INTO horses 
                        (ID, Name, Foaled, Sex, Trainer, TrainerID, Sire, SireID, Dam, DamID, Owner) 
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            horse['id'], 
                            horse['name'], 
                            horse.get('foaled'),
                            horse.get('sex'),
                            horse.get('trainer'),
                            horse.get('trainer_id'),
                            horse.get('sire'), 
                            horse.get('sire_id'),
                            horse.get('dam'), 
                            horse.get('dam_id'),
                            horse.get('owner')
                        )
                    )
                    if cursor.rowcount > 0:
                        records_saved += 1
                        print(f"Successfully saved horse: {horse['name']}")
                    else:
                        print(f"No rows affected when saving horse: {horse['name']}")
                except Exception as e:
                    print(f"Error saving horse {horse['name']}: {str(e)}")
                    print(f"Horse data: {horse}")
        # Add other data types here when implemented
        # elif data_type == 'races':
        #    ...
        
        # Commit the changes
        conn.commit()
        return records_saved
    except Exception as e:
        print(f"Error saving {data_type} data: {str(e)}")
        conn.rollback()
        return 0

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
        
        # URL Search section
        url_search_frame = ttk.LabelFrame(main_frame, text="URL Search", padding="10")
        url_search_frame.pack(fill=tk.X, pady=5)
        
        # Create estimate variables
        self.horses_estimate_var = StringVar(value="Unknown")
        self.trainers_estimate_var = StringVar(value="Unknown")
        self.races_estimate_var = StringVar(value="Unknown")
        
        # Create database count variables
        self.horses_db_count_var = StringVar(value="0")
        self.trainers_db_count_var = StringVar(value="0")
        self.races_db_count_var = StringVar(value="0")
        
        # Row labels
        ttk.Label(url_search_frame, text="Type").grid(row=0, column=0, sticky=tk.W, padx=5, pady=5)
        ttk.Label(url_search_frame, text="Estimated URLs").grid(row=0, column=1, sticky=tk.W, padx=5, pady=5)
        ttk.Label(url_search_frame, text="In Database").grid(row=0, column=2, sticky=tk.W, padx=5, pady=5)
        ttk.Label(url_search_frame, text="Actions").grid(row=0, column=3, sticky=tk.W, padx=5, pady=5)
        
        # Horses row
        ttk.Label(url_search_frame, text="Horses:").grid(row=1, column=0, sticky=tk.W, padx=5, pady=5)
        ttk.Label(url_search_frame, textvariable=self.horses_estimate_var).grid(row=1, column=1, sticky=tk.W, padx=5, pady=5)
        ttk.Label(url_search_frame, textvariable=self.horses_db_count_var).grid(row=1, column=2, sticky=tk.W, padx=5, pady=5)
        ttk.Button(url_search_frame, text="Estimate", 
                  command=lambda: self.estimate_url_count('horses')).grid(row=1, column=3, padx=5, pady=5)
        
        # Trainers row
        ttk.Label(url_search_frame, text="Trainers:").grid(row=2, column=0, sticky=tk.W, padx=5, pady=5)
        ttk.Label(url_search_frame, textvariable=self.trainers_estimate_var).grid(row=2, column=1, sticky=tk.W, padx=5, pady=5)
        ttk.Label(url_search_frame, textvariable=self.trainers_db_count_var).grid(row=2, column=2, sticky=tk.W, padx=5, pady=5)
        ttk.Button(url_search_frame, text="Estimate", 
                  command=lambda: self.estimate_url_count('trainers')).grid(row=2, column=3, padx=5, pady=5)
        
        # Races row
        ttk.Label(url_search_frame, text="Races:").grid(row=3, column=0, sticky=tk.W, padx=5, pady=5)
        ttk.Label(url_search_frame, textvariable=self.races_estimate_var).grid(row=3, column=1, sticky=tk.W, padx=5, pady=5)
        ttk.Label(url_search_frame, textvariable=self.races_db_count_var).grid(row=3, column=2, sticky=tk.W, padx=5, pady=5)
        ttk.Button(url_search_frame, text="Estimate", 
                  command=lambda: self.estimate_url_count('races')).grid(row=3, column=3, padx=5, pady=5)
        
        # Refresh counts button
        ttk.Button(url_search_frame, text="Refresh Database Counts", 
                  command=self.update_db_counts).grid(row=4, column=0, columnspan=4, padx=5, pady=5)
        
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
        
        ttk.Button(scraper_frame, text="Scrape Discovered URLs", command=self.scrape_selected_type).grid(row=0, column=0, padx=5, pady=5)
        
        self.scrape_stats_var = StringVar(value="Not started")
        ttk.Label(scraper_frame, textvariable=self.scrape_stats_var).grid(row=0, column=1, sticky=tk.W, padx=5, pady=5)
        
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
                # Update the database counts
                self.update_db_counts()
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
            
            # Function to update progress bar during crawling
            def update_progress(percent):
                self.progress_var.set(percent)
                self.root.update_idletasks()
            
            # Start the crawl with callbacks for logging and progress updates
            self.log_message("Beginning web crawl...")
            start_time = time.time()
            self.discovered_urls = crawl_website(
                base_url, 
                max_urls, 
                data_type, 
                log_callback=self.log_message,
                progress_callback=update_progress,
                conn=self.conn  # Pass database connection to check existing URLs
            )
            crawl_time = time.time() - start_time
            
            # Update statistics in UI
            self.log_message(f"Crawl finished in {crawl_time:.1f} seconds")
            
            # Get database URLs for comparison (now includes the newly discovered ones)
            self.captured_urls = get_captured_urls(self.conn)
            
            # Update UI with summary
            total_discovered = len(self.discovered_urls)
            total_captured = len(self.captured_urls)
            
            self.crawl_stats_var.set(f"Found: {total_discovered}, Total in DB: {total_captured}")
            
            # Log detailed statistics
            self.log_message(f"Crawl complete. Found {total_discovered} new URLs, database has {total_captured} total URLs")
            
            # Update stats variable
            self.stats_var.set(f"Ready - {total_discovered} URLs discovered")
            
        except Exception as e:
            self.crawl_stats_var.set("Error")
            self.log_message(f"Error during crawl: {str(e)}")
            messagebox.showerror("Crawl Error", f"Failed to crawl website: {str(e)}")
            self.progress_var.set(0)
            self.stats_var.set("Error occurred")
    
    def scrape_selected_type(self):
        """Scrape all discovered URLs of the selected type"""
        if self.conn is None:
            messagebox.showerror("Not Connected", "Please connect to the database first.")
            return
        
        if not self.discovered_urls:
            messagebox.showwarning("No URLs", "Please crawl the website first to discover URLs or load URLs from database.")
            return
        
        data_type = self.data_type_var.get()
        if data_type == "all":
            messagebox.showwarning("Type Selection", "Please select a specific data type to scrape in the crawler section.")
            return
            
        self.log_message(f"Starting scrape of all {len(self.discovered_urls)} discovered {data_type} URLs")
        self.progress_var.set(0)
        
        try:
            # Update UI
            self.scrape_stats_var.set("Scraping...")
            self.root.update_idletasks()
            
            # Filter URLs by type - no need to filter again if already filtered during crawl
            urls_to_scrape = self.discovered_urls
            
            if not urls_to_scrape:
                self.scrape_stats_var.set("No matching URLs")
                self.log_message(f"No URLs found to scrape.")
                return
            
            # Perform the actual scraping with progress updates
            self.log_message(f"Scraping {len(urls_to_scrape)} {data_type} URLs...")
            
            # Function to update progress bar during scraping
            def update_progress(percent):
                self.progress_var.set(percent)
                self.root.update_idletasks()
            
            # Start the scraping with callbacks for logging and progress updates
            start_time = time.time()
            scraped_data = scrape_urls_by_type(
                urls_to_scrape,
                data_type,
                len(urls_to_scrape),  # Use all discovered URLs
                log_callback=self.log_message,
                progress_callback=update_progress,
                conn=self.conn
            )
            scrape_time = time.time() - start_time
            
            if scraped_data:
                # Process the scraped data
                self.log_message(f"Processing {len(scraped_data)} {data_type} records...")
                processed_data = process_scraped_data(scraped_data, data_type)
                
                # Save the processed data to the database
                self.log_message(f"Saving {len(processed_data)} {data_type} records to database...")
                saved_count = save_data_to_database(self.conn, processed_data, data_type)
                
                # Update UI with results
                self.scrape_stats_var.set(f"Scraped: {len(scraped_data)}, Saved: {saved_count}")
                self.log_message(f"Scrape complete in {scrape_time:.1f} seconds. Saved {saved_count} {data_type} records to database.")
            else:
                self.scrape_stats_var.set("No data scraped")
                self.log_message(f"No {data_type} data was successfully scraped.")
            
            # Set final progress
            self.progress_var.set(100)
            
        except Exception as e:
            self.scrape_stats_var.set("Error")
            self.log_message(f"Error during scrape: {str(e)}")
            messagebox.showerror("Scrape Error", f"Failed to scrape URLs: {str(e)}")
    
    def log_message(self, message):
        """Log a message to the UI"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.log_text.insert(tk.END, f"[{timestamp}] {message}\n")
        self.log_text.see(tk.END)  # Scroll to the end
        # Update the UI immediately to show the new log entry
        self.root.update()  # Forces an immediate update of the UI

    def update_default_url(self, event=None):
        """Update the default URL based on the selected data type"""
        data_type = self.data_type_var.get()
        
        default_urls = {
            "races": "https://www.sportinglife.com/racing/results/",
            "horses": "https://www.sportinglife.com/racing/profiles/horse/",
            "jockeys": "https://www.sportinglife.com/racing/profiles/jockey/",
            "trainers": "https://www.sportinglife.com/racing/profiles/trainer/"
        }
        
        self.base_url_var.set(default_urls.get(data_type, "https://www.sportinglife.com/racing/"))
        self.log_message(f"Updated base URL for {data_type} data type")
        
    def estimate_url_count(self, url_type):
        """
        Estimate the upper bound of valid URLs for a given type
        using a binary search approach
        
        Args:
            url_type (str): Type of URLs to estimate ('races', 'horses', 'trainers')
        """
        if url_type not in ['horses', 'trainers', 'races']:
            messagebox.showerror("Invalid Type", f"URL type '{url_type}' is not supported for estimation.")
            return
            
        self.log_message(f"Starting URL count estimation for {url_type}...")
        
        # Set variable to "Estimating..." during the search
        if url_type == 'horses':
            self.horses_estimate_var.set("Estimating...")
        elif url_type == 'trainers':
            self.trainers_estimate_var.set("Estimating...")
        elif url_type == 'races':
            self.races_estimate_var.set("Estimating...")
        
        self.root.update_idletasks()
        
        # Define base URLs for each type
        base_urls = {
            "horses": "https://www.sportinglife.com/racing/profiles/horse/",
            "trainers": "https://www.sportinglife.com/racing/profiles/trainer/",
            # For races, we'll use a different approach
            "races": "https://www.sportinglife.com/racing/results/"
        }
        
        base_url = base_urls.get(url_type)
        
        try:
            if url_type in ['horses', 'trainers']:
                # These have numerical IDs, so we can use binary search
                self.log_message(f"Performing binary search to find maximum valid {url_type} ID...")
                
                # Starting binary search parameters
                low = 1  # Start with ID 1
                high = 1000000  # Initial high guess
                last_valid = 0
                
                # First check if the high value is valid - if it is, we need a higher initial range
                response = requests.head(f"{base_url}{high}", 
                                        headers={'User-Agent': 'Mozilla/5.0'}, 
                                        allow_redirects=True)
                if response.status_code == 200:
                    # If high guess is valid, we need to increase our range
                    while response.status_code == 200:
                        last_valid = high
                        high *= 2
                        self.log_message(f"Initial high value {last_valid} is valid, trying {high}...")
                        response = requests.head(f"{base_url}{high}", 
                                                headers={'User-Agent': 'Mozilla/5.0'}, 
                                                allow_redirects=True)
                
                # Perform binary search
                while low <= high:
                    mid = (low + high) // 2
                    
                    # Add a small delay to avoid overwhelming the server
                    time.sleep(0.2)
                    
                    # Check if mid is valid
                    response = requests.head(f"{base_url}{mid}", 
                                           headers={'User-Agent': 'Mozilla/5.0'}, 
                                           allow_redirects=True)
                    
                    # Update UI periodically
                    if (mid % 1000 == 0) or (high - low < 100):
                        self.log_message(f"Testing {url_type} ID: {mid} (range: {low}-{high})")
                        self.root.update_idletasks()
                    
                    # If valid, try higher
                    if response.status_code == 200:
                        last_valid = mid
                        low = mid + 1
                    # If invalid, try lower
                    else:
                        high = mid - 1
                
                # After search, last_valid contains our estimate
                estimate = last_valid
                self.log_message(f"Estimated upper bound for {url_type} IDs: approximately {estimate}")
                
                # Update the appropriate variable
                if url_type == 'horses':
                    self.horses_estimate_var.set(f"~{estimate:,}")
                elif url_type == 'trainers':
                    self.trainers_estimate_var.set(f"~{estimate:,}")
                
            elif url_type == 'races':
                # For races, we'll try a different approach - count races per month over last 5 years
                self.log_message("Estimating race count by sampling months...")
                
                # Get the current date
                now = datetime.now()
                total_count = 0
                samples = 0
                
                # Go back 5 years, sample 1 month from each quarter
                for year in range(now.year - 5, now.year + 1):
                    for month in [1, 4, 7, 10]:  # January, April, July, October
                        # Skip future months
                        if year == now.year and month > now.month:
                            continue
                            
                        # Format the URL for the month
                        month_url = f"{base_url}{year}/{month:02d}"
                        self.log_message(f"Sampling races from {year}-{month:02d}...")
                        
                        try:
                            # Get the content of the monthly page
                            response = requests.get(month_url, 
                                                  headers={'User-Agent': 'Mozilla/5.0'})
                            if response.status_code == 200:
                                soup = BeautifulSoup(response.text, 'html.parser')
                                
                                # Find all race links
                                race_links = soup.find_all('a', href=re.compile(r'/racing/results/\d{4}-\d{2}-\d{2}/'))
                                month_count = len(set(link['href'] for link in race_links))
                                
                                self.log_message(f"Found {month_count} races in {year}-{month:02d}")
                                total_count += month_count
                                samples += 1
                            else:
                                self.log_message(f"Could not access {month_url}, status: {response.status_code}")
                        except Exception as e:
                            self.log_message(f"Error sampling month {year}-{month:02d}: {str(e)}")
                        
                        # Short delay between requests
                        time.sleep(0.5)
                
                # Calculate estimate based on average per month * 12 months * 5 years
                if samples > 0:
                    avg_per_month = total_count / samples
                    total_estimate = int(avg_per_month * 12 * 5)  # Estimate for 5 years
                    self.log_message(f"Estimated race count: ~{total_estimate:,} (based on {samples} sample months)")
                    self.races_estimate_var.set(f"~{total_estimate:,}")
                else:
                    self.log_message("Could not estimate race count, no valid samples found")
                    self.races_estimate_var.set("Unknown")
                
        except Exception as e:
            self.log_message(f"Error estimating {url_type} count: {str(e)}")
            # Reset the variable on error
            if url_type == 'horses':
                self.horses_estimate_var.set("Error")
            elif url_type == 'trainers':
                self.trainers_estimate_var.set("Error")
            elif url_type == 'races':
                self.races_estimate_var.set("Error")
            
            messagebox.showerror("Estimation Error", f"Failed to estimate {url_type} count: {str(e)}")
    
    def update_db_counts(self):
        """Update database count displays for all data types"""
        if self.conn is None:
            messagebox.showerror("Not Connected", "Please connect to the database first.")
            return
            
        self.log_message("Updating database counts...")
        
        try:
            cursor = self.conn.cursor()
            
            # Count horses
            cursor.execute("SELECT COUNT(*) FROM horses")
            horse_count = cursor.fetchone()[0]
            self.horses_db_count_var.set(f"{horse_count:,}")
            
            # Count trainers
            cursor.execute("SELECT COUNT(*) FROM trainers")
            trainer_count = cursor.fetchone()[0]
            self.trainers_db_count_var.set(f"{trainer_count:,}")
            
            # Count races
            cursor.execute("SELECT COUNT(*) FROM races")
            race_count = cursor.fetchone()[0]
            self.races_db_count_var.set(f"{race_count:,}")
            
            self.log_message(f"Database counts updated: {horse_count} horses, {trainer_count} trainers, {race_count} races")
            
        except Exception as e:
            self.log_message(f"Error updating database counts: {str(e)}")
            messagebox.showerror("Count Error", f"Failed to update database counts: {str(e)}")

# === Main Entry Point ===

if __name__ == "__main__":
    root = tk.Tk()
    app = ScraperUI(root)
    root.mainloop()
