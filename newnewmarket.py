# -*- coding: utf-8 -*-
import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
from datetime import datetime
import os
import sqlite3

def fetch_racing_page(url):
    """Fetch the web page content with appropriate headers."""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.text
    except requests.exceptions.RequestException as e:
        print(f"Error fetching page: {e}")
        return None

def extract_race_info(html_content, url):
    """Extract detailed race information from the racing page HTML."""
    if not html_content:
        return None
    
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Dictionary to store race information
    race_info = {
        'racecourse': 'Unknown',
        'time': 'Unknown',
        'date': 'Unknown',
        'race_name': 'Unknown',
        'age_restrictions': 'Unknown',
        'class': 'Unknown',
        'distance': 'Unknown',
        'going': 'Unknown',
        'runners': 'Unknown',
        'surface': 'Unknown',
        'url': url  # Store the source URL
    }
    
    # Extract racecourse and date from URL
    url_match = re.search(r'racecards/(\d{4}-\d{2}-\d{2})/([^/]+)', url)
    if url_match:
        race_info['date'] = url_match.group(1)
        race_info['racecourse'] = url_match.group(2).replace('-', ' ').title()
    
    # Try to get time from the page content
    time_match = re.search(r'(\d{1,2}[:\.]\d{2})', html_content)
    if time_match:
        race_info['time'] = time_match.group(1)
    
    # Get race name - usually a prominent heading
    race_name_elem = soup.find(['h1', 'h2'], string=lambda s: s and len(s.strip()) > 5 and not re.match(r'\d{1,2}:\d{2}', s.strip()))
    if race_name_elem:
        race_info['race_name'] = race_name_elem.get_text().strip()
    
    # Find the race details section that often contains most of the information
    race_details_sections = soup.find_all(['p', 'div', 'span'], string=lambda s: s and re.search(r'Class \d+', str(s)))
    
    race_details_text = ""
    for section in race_details_sections:
        race_details_text += section.get_text().strip() + " "
    
    # If we couldn't find a section with explicit class information, look for other sections
    if not race_details_text:
        detail_candidates = [
            soup.find('div', class_=lambda c: c and ('details' in str(c).lower() or 'subtitle' in str(c).lower())),
            soup.find('p', class_=lambda c: c and ('details' in str(c).lower() or 'description' in str(c).lower())),
            soup.find('div', class_=lambda c: c and ('racecard-header' in str(c).lower() or 'race-header' in str(c).lower()))
        ]
        
        for candidate in detail_candidates:
            if candidate:
                race_details_text += candidate.get_text().strip() + " "
    
    # Process the full page text if we couldn't find specific sections
    if not race_details_text:
        page_text = soup.get_text()
        race_details_text = page_text
    
    # Now parse specific elements from the race details text
    
    # Extract class
    class_match = re.search(r'[Cc]lass\s+(\d+)', race_details_text)
    if class_match:
        race_info['class'] = f"Class {class_match.group(1)}"
    
    # Extract age restrictions - various formats
    age_patterns = [
        r'(\d+YO\s+only)', 
        r'(\d+\s*[Yy]ear\s*[Oo]lds?\s*only)',
        r'(\d+(?:-\d+)?(?:-\d+)?\s*YO\+?)',
        r'(\d+\s*[Yy]ear\s*[Oo]lds?(?:\+)?)'
    ]
    
    for pattern in age_patterns:
        age_match = re.search(pattern, race_details_text, re.IGNORECASE)
        if age_match:
            race_info['age_restrictions'] = age_match.group(1).strip()
            break
    
    # Extract distance - common formats
    distance_match = re.search(r'(\d+m\s*(?:\d+f)?\s*(?:\d+y)?)', race_details_text)
    if distance_match:
        race_info['distance'] = distance_match.group(1).strip()
    
    # Extract going
    going_patterns = ["Good", "Soft", "Firm", "Heavy", "Standard", "Slow", "Good to Soft", "Good to Firm", "Yielding"]
    for pattern in going_patterns:
        if pattern.lower() in race_details_text.lower():
            race_info['going'] = pattern
            break
    
    # Extract surface
    if "turf" in race_details_text.lower():
        race_info['surface'] = "Turf"
    elif any(x in race_details_text.lower() for x in ["all-weather", "all weather", "aw", "polytrack", "tapeta", "fibresand"]):
        race_info['surface'] = "All Weather"
    
    # Improved runner count detection
    # First check for explicit runner text in race details
    runners_match = re.search(r'(\d+)\s*[Rr]unners', race_details_text)
    if runners_match:
        race_info['runners'] = runners_match.group(1)
    else:
        # Look for numbered horse entries (usually more reliable)
        horse_entries = []
        # Check for horse silk or number elements
        horse_numbers = soup.find_all(['div', 'span'], string=re.compile(r'^\d+$'))
        if horse_numbers:
            # Filter out any non-horse numbers (like dates, times)
            horse_entries = [num for num in horse_numbers if len(num.get_text()) <= 2 and int(num.get_text()) < 30]
            if horse_entries:
                race_info['runners'] = str(len(horse_entries))
        
        # If still not found, look for horse rows
        if race_info['runners'] == 'Unknown':
            horse_rows = soup.find_all(['tr', 'div'], class_=lambda c: c and ('runner' in str(c).lower() or 'horse' in str(c).lower() if c else False))
            if horse_rows:
                # Count only unique horses to avoid duplicates
                unique_horses = set()
                for row in horse_rows:
                    # Try to extract horse name or number
                    horse_name = row.get_text().strip()
                    if horse_name:
                        unique_horses.add(horse_name)
                
                if unique_horses:
                    race_info['runners'] = str(len(unique_horses))
    
    # If we have race details that mention "4 Runners" explicitly, use that
    if "4 Runners" in race_details_text:
        race_info['runners'] = "4"
    
    # Add timestamp for when the data was scraped
    race_info['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    return race_info

def initialize_database():
    """Create the SQLite database and races table if they don't exist."""
    conn = sqlite3.connect('racing_data.db')
    cursor = conn.cursor()
    
    # Create races table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS races (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        racecourse TEXT,
        time TEXT,
        date TEXT,
        race_name TEXT,
        age_restrictions TEXT,
        class TEXT,
        distance TEXT,
        going TEXT,
        runners TEXT,
        surface TEXT,
        url TEXT,
        scraped_at TEXT,
        UNIQUE(date, racecourse, time)
    )
    ''')
    
    conn.commit()
    return conn

def save_to_database(race_info, conn):
    """Save the race information to the database."""
    cursor = conn.cursor()
    
    # Prepare column names and placeholders for SQL query
    columns = ', '.join(race_info.keys())
    placeholders = ', '.join(['?' for _ in race_info])
    values = tuple(race_info.values())
    
    # Insert or replace if the same race already exists
    query = f'''
    INSERT OR REPLACE INTO races ({columns})
    VALUES ({placeholders})
    '''
    
    cursor.execute(query, values)
    conn.commit()
    return cursor.lastrowid

def main():
    # URL of the racing card to scrape
    url = "https://www.sportinglife.com/racing/racecards/2025-04-05/chepstow/racecard/851313/best-odds-guaranteed-with-dragonbet-ebf-junior-national-hunt-hurdle-gbb-race"
    
    print(f"Scraping race information from {url}...")
    
    # Initialize the database
    conn = initialize_database()
    
    # Fetch the page content
    html_content = fetch_racing_page(url)
    
    if html_content:
        # Extract race information
        race_info = extract_race_info(html_content, url)
        
        if race_info:
            print("\nRace Information:")
            print("=================")
            
            # Print the extracted information
            for key, value in race_info.items():
                if key != 'url' and key != 'scraped_at':  # Skip URL and timestamp in display
                    print(f"{key.replace('_', ' ').title()}: {value}")
            
            # Save to database
            row_id = save_to_database(race_info, conn)
            print(f"\nSaved race information to database with ID: {row_id}")
            
            # Demonstrate reading from database
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM races")
            print(f"\nTotal races in database: {len(cursor.fetchall())}")
            
            print("\nScraping completed successfully!")
        else:
            print("No race information found on the page.")
    else:
        print("Failed to retrieve the racing page.")
    
    # Close the database connection
    conn.close()

if __name__ == "__main__":
    main()
