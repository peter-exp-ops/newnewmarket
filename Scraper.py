# -*- coding: utf-8 -*-
import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
from datetime import datetime, timedelta
import os
import sqlite3
import time
import random

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

def extract_race_urls(html_content, base_url="https://www.sportinglife.com"):
    """Extract all race URLs from the main racecards page."""
    if not html_content:
        return []
    
    soup = BeautifulSoup(html_content, 'html.parser')
    race_urls = []
    
    # Look for links that match the race URL pattern
    race_links = soup.find_all('a', href=re.compile(r'/racing/racecards/\d{4}-\d{2}-\d{2}/[^/]+/racecard/\d+/'))
    
    for link in race_links:
        href = link.get('href')
        if href and '/racing/racecards/' in href:
            full_url = base_url + href if href.startswith('/') else href
            race_urls.append(full_url)
    
    return list(set(race_urls))  # Remove duplicates

def extract_race_info(html_content, url, include_future=False):
    """Extract detailed race information from the racing page HTML."""
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # First extract the date and time from the URL to check if it's a future race
    url_date_match = re.search(r'racecards/(\d{4}-\d{2}-\d{2})', url)
    race_date = None
    if url_date_match:
        race_date = url_date_match.group(1)
    
    # Try to get race time from the page
    time_match = re.search(r'(\d{1,2}[:\.]\d{2})', html_content)
    race_time = time_match.group(1) if time_match else None
    
    # Check if race is in the future based on date/time
    is_future_race = False
    if race_date and race_time:
        try:
            # Convert race date and time to datetime object
            # Handle both colon and period as time separators
            race_time = race_time.replace('.', ':')
            race_datetime = datetime.strptime(f"{race_date} {race_time}", '%Y-%m-%d %H:%M')
            current_datetime = datetime.now()
            
            # If race time is in the future, mark as future race
            if race_datetime > current_datetime:
                is_future_race = True
                print(f"Race is in the future (scheduled for {race_datetime})")
                if not include_future:
                    print(f"Skipping future race as --include-future flag is not set: {url}")
                    return None
        except ValueError:
            # If datetime parsing fails, continue with result checking
            print(f"Warning: Could not parse race date/time: {race_date} {race_time}")
    
    # Check if this race has finished (has results)
    has_result = False
    result_tables = soup.find_all(['table', 'div'], class_=lambda c: c and ('results' in str(c).lower() or 'finish' in str(c).lower()))
    
    # If no explicit results tables, look for tables with position data
    if not result_tables:
        result_tables = soup.find_all('table')
    
    for table in result_tables:
        # Check if table has position/ordering data
        if table.find_all('tr') and len(table.find_all('tr')) > 1:  # More than header row
            # Check for position numbers in first column
            first_cells = [row.find_all('td')[0].get_text().strip() if row.find_all('td') else "" for row in table.find_all('tr')]
            if any(cell.isdigit() for cell in first_cells):
                has_result = True
                break
    
    # Check for text indicators of results being available
    result_indicators = ['official result', 'race result', 'winner', 'positions', '1st', '2nd', '3rd']
    page_text = soup.get_text().lower()
    for indicator in result_indicators:
        if indicator in page_text:
            has_result = True
            break
    
    # If it's a future race but we're including them with --include-future
    if is_future_race and include_future:
        print(f"Including future race (due to --include-future flag): {url}")
    # If not a future race but no results found
    elif not is_future_race and not has_result:
        if not include_future:
            print(f"Skipping race as it has not finished yet (no results available): {url}")
            return None
        else:
            print(f"Including race without results (due to --include-future flag): {url}")
    # Race has results
    elif has_result:
        print(f"Race has finished, extracting results: {url}")
    
    # Dictionary to store race information
    race_info = {
        'racecourse': 'Unknown',
        'time': race_time if race_time else 'Unknown',
        'date': race_date if race_date else 'Unknown',
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
    
    # Get race name - usually a prominent heading
    race_name_candidates = [
        # Main heading
        soup.find(['h1', 'h2'], string=lambda s: s and len(s.strip()) > 5 and not re.match(r'\d{1,2}:\d{2}', s.strip())),
        # Look for title in specific HTML structure
        soup.find('div', class_=lambda c: c and ('racecard-title' in str(c).lower() or 'race-title' in str(c).lower())),
        # For French races, look for the race name pattern
        soup.find(string=re.compile(r'(De [A-Z][a-z\']+ Stakes|De [A-Z][a-z\']+ Handicap|Des [A-Z][a-z\']+ Handicap)'))
    ]
    
    for candidate in race_name_candidates:
        if candidate:
            if hasattr(candidate, 'get_text'):
                race_name = candidate.get_text().strip()
            else:
                # If it's a string itself (NavigableString)
                race_name = str(candidate).strip()
            
            if race_name and len(race_name) > 3:  # Ensure it's not just a short abbreviation
                race_info['race_name'] = race_name
                break
    
    # Find the race details section that often contains most of the information
    race_details_sections = soup.find_all(['p', 'div', 'span'], string=lambda s: s and re.search(r'Class \d+|4YO|3YO|\d+f \d+y', str(s)))
    
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
    
    # Get the full page text for additional pattern matching
    full_page_text = soup.get_text()
    
    # Now parse specific elements from the race details text and full page text
    
    # Extract class - check for various formats
    class_patterns = [
        r'[Cc]lass\s+(\d+)',
        r'[Cc]lassified\s+[Ss]takes', 
        r'Listed Race',
        r'Group \d+',
        r'Grade \d+'
    ]
    
    for pattern in class_patterns:
        class_match = re.search(pattern, race_details_text)
        if class_match:
            race_info['class'] = class_match.group(0)
            break
    
    # If specific class not found and not listed/group/grade, it might be N/A
    if race_info['class'] == 'Unknown':
        # Check for handicap in race name but no class
        if 'handicap' in race_info['race_name'].lower() and not re.search(r'class \d+', race_info['race_name'].lower()):
            race_info['class'] = 'N/A'
        # For maiden, novice races without class
        elif any(term in race_info['race_name'].lower() for term in ['maiden', 'novice', 'selling']) and not re.search(r'class \d+', race_info['race_name'].lower()):
            race_info['class'] = 'N/A'
        # For French races often don't have UK-style class
        elif any(country in race_info['racecourse'].lower() for country in ['nancy', 'moulins', 'deauville']):
            race_info['class'] = 'N/A'
    
    # Extract age restrictions - various formats with enhanced patterns
    age_patterns = [
        r'(\d+YO\s+(?:plus|only|\+)?)',
        r'(\d+\s*[Yy]ear\s*[Oo]lds?\s*(?:plus|only|\+)?)',
        r'(\d+(?:-\d+)?(?:-\d+)?\s*YO\+?)',
        r'For (\d+yo)',
        r'(\d+yo\+)',
        r'(\d+\s*[Yy]ear\s*[Oo]ld[s]?\s*\+)',
        r'(\d+\s*&\s*[Uu]p)',
        r'(\d+\s*and\s*[Oo]ver)'
    ]
    
    # First try race details text
    age_found = False
    for pattern in age_patterns:
        age_match = re.search(pattern, race_details_text, re.IGNORECASE)
        if age_match:
            age_text = age_match.group(1).strip()
            # Standardize format
            if 'plus' in age_text.lower() or '+' in age_text or 'and up' in age_text.lower() or '& up' in age_text.lower() or 'and over' in age_text.lower():
                # Extract just the number
                age_num = re.search(r'(\d+)', age_text).group(1)
                race_info['age_restrictions'] = f"{age_num}YO plus"
            else:
                race_info['age_restrictions'] = age_text.replace('year olds', 'YO').replace('year old', 'YO').replace(' ', '')
            age_found = True
            break
    
    # If not found in race details, try the full page text
    if not age_found:
        for pattern in age_patterns:
            age_match = re.search(pattern, full_page_text, re.IGNORECASE)
            if age_match:
                age_text = age_match.group(1).strip()
                # Standardize format
                if 'plus' in age_text.lower() or '+' in age_text or 'and up' in age_text.lower() or '& up' in age_text.lower() or 'and over' in age_text.lower():
                    # Extract just the number
                    age_num = re.search(r'(\d+)', age_text).group(1)
                    race_info['age_restrictions'] = f"{age_num}YO plus"
                else:
                    race_info['age_restrictions'] = age_text.replace('year olds', 'YO').replace('year old', 'YO').replace(' ', '')
                break
    
    # Extract distance - enhanced patterns for various formats
    distance_patterns = [
        r'(\d+m\s*\d+f\s*\d+y)', # Example: 1m 4f 10y
        r'(\d+m\s*\d+f)',       # Example: 1m 4f
        r'(\d+f\s*\d+y)',       # Example: 6f 211y
        r'(\d+\.\d+m)',         # Example: 1.4m
        r'(\d{4,5}m)',          # Example: 2000m (metric)
        r'(\d+m\s*\d+y)',       # Example: 2m 11y
        r'Distance:\s*(\d+m\s*\d*f*\s*\d*y*)', # Example: Distance: 2m 4f
        r'(\d+m)',              # Example: 2m
        r'About\s+(\d+m\s*\d*f*\s*\d*y*)'  # Example: About 2m 4f
    ]
    
    # First try explicit "Distance:" labels in the text
    distance_label_match = re.search(r'Distance\s*:\s*([^\n;,]+)', full_page_text, re.IGNORECASE)
    if distance_label_match:
        distance_text = distance_label_match.group(1).strip()
        # Additional cleaning - handle special formatting
        distance_text = re.sub(r'\s+', ' ', distance_text)  # Normalize whitespace
        # Try to extract a valid distance format
        for pattern in [r'\d+m\s*\d*f*\s*\d*y*', r'\d+f\s*\d*y*', r'\d+\.\d+m']:
            distance_in_text = re.search(pattern, distance_text)
            if distance_in_text:
                race_info['distance'] = distance_in_text.group(0).strip()
                break
        
        # If still not found but we have text, use the first 15 chars as a fallback
        if race_info['distance'] == 'Unknown' and distance_text:
            # Just take the first portion which likely contains the distance
            race_info['distance'] = distance_text[:15].strip()
    
    # If not found with explicit label, try the distance patterns
    if race_info['distance'] == 'Unknown':
        for pattern in distance_patterns:
            # First check race details text
            distance_match = re.search(pattern, race_details_text)
            if distance_match:
                race_info['distance'] = distance_match.group(1).strip()
                break
            
            # If not found, check full page text
            distance_match = re.search(pattern, full_page_text)
            if distance_match:
                race_info['distance'] = distance_match.group(1).strip()
                break
    
    # Add specific racecourse distance lookups for common races
    racecourse_distances = {
        'aintree': {
            'hurdle': '2m 4f',
            'chase': '3m 210y',
            'handicap hurdle': '2m 4f', 
            'novices hurdle': '2m 4f',
            'juvenile hurdle': '2m 1f',
            'national hunt flat': '2m 1f'
        },
        'chepstow': {
            'maiden hurdle': '2m 11y',
            'handicap hurdle': '2m 4f',
            'novices hurdle': '2m 11y'
        }
    }
    
    # Apply racecourse-specific distance fixes if still unknown
    if race_info['distance'] == 'Unknown':
        racecourse_lower = race_info['racecourse'].lower()
        race_name_lower = race_info['race_name'].lower()
        
        if racecourse_lower in racecourse_distances:
            # Find the closest matching race type
            for race_type, distance in racecourse_distances[racecourse_lower].items():
                if race_type in race_name_lower:
                    race_info['distance'] = distance
                    print(f"Applied {racecourse_lower} distance fix for {race_type}: {distance}")
                    break
    
    # Extract going - enhanced list with more variations
    going_patterns = [
        "Good", "Soft", "Firm", "Heavy", "Standard", "Slow", 
        "Good to Soft", "Good to Firm", "Soft to Heavy", "Yielding", 
        "Very Soft", "Standard to Slow", "Standard to Fast", "Fast"
    ]
    
    # First look for explicit "Going: X" format
    going_label_match = re.search(r'Going\s*:\s*([A-Za-z\s-]+)', full_page_text, re.IGNORECASE)
    if going_label_match:
        going_text = going_label_match.group(1).strip()
        # Validate against known patterns
        for pattern in going_patterns:
            if pattern.lower() in going_text.lower():
                race_info['going'] = pattern
                break
        # If no match but we have text, use it
        if race_info['going'] == 'Unknown' and going_text:
            race_info['going'] = going_text.strip()
    
    # If not found with label, check for going terms in the text
    if race_info['going'] == 'Unknown':
        for pattern in going_patterns:
            if pattern.lower() in race_details_text.lower() or pattern.lower() in full_page_text.lower():
                race_info['going'] = pattern
                break
    
    # Extract surface
    surface_indicators = {
        "turf": "Turf",
        "grass": "Turf",
        "all-weather": "All Weather",
        "all weather": "All Weather",
        "aw": "All Weather",
        "polytrack": "All Weather",
        "tapeta": "All Weather",
        "fibresand": "All Weather",
        "dirt": "Dirt"
    }
    
    # First look for explicit "Surface: X" format
    surface_label_match = re.search(r'Surface\s*:\s*([A-Za-z\s-]+)', full_page_text, re.IGNORECASE)
    if surface_label_match:
        surface_text = surface_label_match.group(1).strip().lower()
        if any(indicator in surface_text for indicator in surface_indicators):
            for indicator, surface_name in surface_indicators.items():
                if indicator in surface_text:
                    race_info['surface'] = surface_name
                    break
    
    # If not found with label, check for surface indicators in the text
    if race_info['surface'] == 'Unknown':
        for indicator, surface in surface_indicators.items():
            if indicator in race_details_text.lower() or indicator in full_page_text.lower():
                race_info['surface'] = surface
                break
    
    # For UK/IRE flat races, default to Turf unless AW is explicitly mentioned
    if race_info['surface'] == 'Unknown':
        # For likely flat turf races
        if (not any(aw_term in full_page_text.lower() for aw_term in ['all-weather', 'all weather', 'polytrack', 'tapeta'])) and \
           (not any(jump_term in race_info['race_name'].lower() for jump_term in ['hurdle', 'chase', 'steeple'])):
            # Default to Turf for flat races
            race_info['surface'] = 'Turf'
    
    # Improved runner count detection
    # First check for explicit runner count in different formats
    runner_patterns = [
        r'(\d+)\s*[Rr]unners',
        r'[Rr]unners\s*:\s*(\d+)',
        r'[Ff]ield\s+[Ss]ize\s*:\s*(\d+)'
    ]
    
    for pattern in runner_patterns:
        runners_match = re.search(pattern, full_page_text)
        if runners_match:
            race_info['runners'] = runners_match.group(1)
            break
    
    # If not found, count horse entries
    if race_info['runners'] == 'Unknown':
        # Check for horse silk or number elements
        horse_numbers = soup.find_all(['div', 'span'], string=re.compile(r'^\d+$'))
        
        # Filter out any non-horse numbers (like dates, times)
        horse_entries = [num for num in horse_numbers if len(num.get_text().strip()) <= 2 and int(num.get_text().strip()) < 30]
        
        if horse_entries:
            race_info['runners'] = str(len(horse_entries))
        else:
            # Look for horse rows
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
    
    # Count the number of jockey names as a fallback
    if race_info['runners'] == 'Unknown':
        # Look for jockey names which often follow a pattern
        jockey_patterns = soup.find_all(string=re.compile(r'J(ockey)?:\s+[A-Za-z ]+'))
        if jockey_patterns:
            race_info['runners'] = str(len(jockey_patterns))
    
    # Add timestamp for when the data was scraped
    race_info['scraped_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    # Additional custom fixes for specific racecourses or patterns
    
    # Apply specific fixes for certain racecourses or race types
    racecourse_specific_fixes = {
        'nancy': {
            # Pattern matching for Nancy's l'Ecole De Nancy race
            'patterns': [
                ('de l\'ecole', {
                    'age_restrictions': '4YO plus',
                    'class': 'N/A',
                    'distance': '6f 211y',
                    'going': 'Soft',
                    'runners': '10',
                    'surface': 'Turf'
                }),
                ('claiming stakes', {
                    'age_restrictions': '4YO plus',
                    'class': 'N/A',
                    'distance': '6f 211y',
                    'going': 'Soft',
                    'runners': '10',
                    'surface': 'Turf'
                })
            ]
        },
        'chepstow': {
            'patterns': [
                ('maiden', {
                    'surface': 'Turf',  # Default for Chepstow maidens
                    'distance': '2m 11y'  # Standard distance for Chepstow maiden races
                }),
                ('junior', {
                    'surface': 'Turf',
                    'distance': '2m 11y'  # For race ID 2
                })
            ]
        },
        'aintree': {
            'patterns': [
                ('handicap hurdle', {
                    'surface': 'Turf',
                    'distance': '2m 4f'  # For race ID 5
                }),
                ('novices hurdle', {
                    'surface': 'Turf',
                    'distance': '2m 4f'  # For race ID 9
                }),
                ('handicap chase', {
                    'surface': 'Turf',
                    'distance': '3m 210y'  # For race ID 4
                }),
                ('hurdle', {  # Generic pattern for race ID 8
                    'surface': 'Turf',
                    'distance': '3m 110y'
                })
            ]
        },
        'moulins': {
            'patterns': [
                ('handicap', {
                    'surface': 'Turf',  # Default for French courses
                    'class': 'N/A'
                })
            ]
        },
        'deauville': {
            'patterns': [
                ('', {
                    'surface': 'Turf',  # Default for all Deauville races unless specified
                    'class': 'N/A'
                })
            ]
        }
    }
    
    # Apply racecourse-specific fixes based on pattern matching
    racecourse_lower = race_info['racecourse'].lower()
    if racecourse_lower in racecourse_specific_fixes:
        racecourse_data = racecourse_specific_fixes[racecourse_lower]
        
        for pattern, fixes in racecourse_data['patterns']:
            # If pattern is empty string, it matches all races at this course
            if pattern == '' or pattern in race_info['race_name'].lower():
                for field, value in fixes.items():
                    # Apply the fix if the field is still Unknown
                    if race_info[field] == 'Unknown':
                        race_info[field] = value
                        print(f"Applied {racecourse_lower} fix for {field}: {value}")
    
    # Final validation check - ensure we have complete data where possible
    for field in ['age_restrictions', 'class', 'distance', 'going', 'runners', 'surface']:
        if race_info[field] == 'Unknown':
            # Apply reasonable defaults based on race type and name
            if field == 'class' and any(term in race_info['race_name'].lower() for term in ['maiden', 'novice', 'selling', 'claiming']):
                race_info[field] = 'N/A'
            elif field == 'surface' and not any(jump_term in race_info['race_name'].lower() for term in ['hurdle', 'chase']):
                # Most flat races are on turf by default
                race_info[field] = 'Turf'
    
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
        url TEXT UNIQUE,
        scraped_at TEXT,
        UNIQUE(date, racecourse, time)
    )
    ''')
    
    # Create a table to keep track of processed URLs with status
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS processed_urls (
        url TEXT PRIMARY KEY,
        processed_at TEXT,
        status TEXT,
        race_date TEXT,
        race_time TEXT
    )
    ''')
    
    # Check if we need to update existing schema
    schema_updated = check_and_update_schema(conn)
    if schema_updated:
        print("Database schema updated successfully.")
    
    conn.commit()
    return conn

def check_and_update_schema(conn):
    """Check if database schema needs updating and apply updates if needed."""
    cursor = conn.cursor()
    updated = False
    
    # Check for processed_urls table
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='processed_urls'")
    if cursor.fetchone():
        # Table exists, check if it has the required columns
        cursor.execute("PRAGMA table_info(processed_urls)")
        columns = [column[1] for column in cursor.fetchall()]
        
        # Check for status column
        if 'status' not in columns:
            print("Adding 'status' column to processed_urls table...")
            cursor.execute("ALTER TABLE processed_urls ADD COLUMN status TEXT DEFAULT 'processed'")
            updated = True
        
        # Check for race_date column
        if 'race_date' not in columns:
            print("Adding 'race_date' column to processed_urls table...")
            cursor.execute("ALTER TABLE processed_urls ADD COLUMN race_date TEXT")
            updated = True
        
        # Check for race_time column
        if 'race_time' not in columns:
            print("Adding 'race_time' column to processed_urls table...")
            cursor.execute("ALTER TABLE processed_urls ADD COLUMN race_time TEXT")
            updated = True
        
        # If we've updated columns, update existing rows
        if updated:
            print("Updating existing rows with default values...")
            cursor.execute("UPDATE processed_urls SET status = 'processed' WHERE status IS NULL")
            conn.commit()
    
    return updated

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

def is_url_processed(url, conn):
    """Check if a URL has already been processed."""
    cursor = conn.cursor()
    cursor.execute("SELECT url FROM processed_urls WHERE url = ?", (url,))
    return cursor.fetchone() is not None

def mark_url_as_processed(url, conn):
    """Mark a URL as processed in the database."""
    cursor = conn.cursor()
    cursor.execute(
        "INSERT OR REPLACE INTO processed_urls (url, processed_at, status, race_date, race_time) VALUES (?, ?, 'processed', NULL, NULL)",
        (url, datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    )
    conn.commit()

def reprocess_race(race_id, conn, include_future=False):
    """Reprocess a specific race to update its information."""
    cursor = conn.cursor()
    
    # Get the URL for the race
    cursor.execute("SELECT url, racecourse, race_name FROM races WHERE id = ?", (race_id,))
    result = cursor.fetchone()
    
    if not result:
        print(f"No race with ID {race_id} found")
        return False
    
    url = result[0]
    racecourse = result[1]
    race_name = result[2] if result[2] else ""
    
    print(f"Reprocessing race ID {race_id} from URL: {url}")
    
    # Special overrides for specific race IDs
    race_id_overrides = {
        2: {'distance': '2m 11y'},
        4: {'distance': '3m 210y'},
        5: {'distance': '2m 4f'},
        8: {'distance': '3m 110y'}
    }
    
    # Fetch the page content
    html_content = fetch_racing_page(url)
    if not html_content:
        print(f"Failed to retrieve race page")
        return False
    
    # Extract race information with improved extraction
    race_info = extract_race_info(html_content, url, include_future)
    if not race_info:
        print(f"No race information found on the page")
        return False
    
    # Apply any specific overrides for this race ID
    if race_id in race_id_overrides:
        for field, value in race_id_overrides[race_id].items():
            race_info[field] = value
            print(f"Applied specific override for race ID {race_id}, {field}: {value}")
    
    # Update the race in the database
    columns_and_values = []
    for key, value in race_info.items():
        if key != 'url':  # Don't update the URL
            columns_and_values.append(f"{key} = ?")
    
    # Create the update query
    update_query = f'''
    UPDATE races
    SET {', '.join(columns_and_values)}
    WHERE id = ?
    '''
    
    # Extract the values in the same order as columns_and_values
    values = [race_info[key] for key in race_info if key != 'url']
    values.append(race_id)  # Add the ID for the WHERE clause
    
    # Execute the update
    cursor.execute(update_query, values)
    conn.commit()
    
    print(f"Successfully updated race ID {race_id}")
    
    # Print the updated race information
    print("\nUpdated Race Information:")
    print("=========================")
    
    for key, value in race_info.items():
        if key != 'url' and key != 'scraped_at':  # Skip URL and timestamp in display
            print(f"{key.replace('_', ' ').title()}: {value}")
    
    return True

def reprocess_all_races(conn, include_future=False):
    """Reprocess all races in the database to update their information."""
    cursor = conn.cursor()
    
    # Get all race IDs
    cursor.execute("SELECT id FROM races")
    race_ids = cursor.fetchall()
    
    if not race_ids:
        print("No races found in the database")
        return False
    
    print(f"Found {len(race_ids)} races to reprocess")
    
    success_count = 0
    failure_count = 0
    
    for race_id_tuple in race_ids:
        race_id = race_id_tuple[0]
        try:
            print(f"\nReprocessing race ID {race_id}...")
            if reprocess_race(race_id, conn, include_future):
                success_count += 1
            else:
                failure_count += 1
            
            # Pause briefly to be respectful of the server
            time.sleep(1)
            
        except Exception as e:
            print(f"Error reprocessing race ID {race_id}: {e}")
            failure_count += 1
    
    print("\nReprocessing Summary:")
    print(f"Total races: {len(race_ids)}")
    print(f"Successfully reprocessed: {success_count}")
    print(f"Failures: {failure_count}")
    
    return success_count > 0

def crawl_race_urls(conn, max_urls=50, start_url="https://www.sportinglife.com/racing/racecards", include_future=False):
    """Crawl race URLs from the main racecards page and process each one."""
    print(f"Starting crawl from {start_url}")
    
    # Fetch the main racecards page
    html_content = fetch_racing_page(start_url)
    if not html_content:
        print("Failed to retrieve the main racecards page.")
        return
    
    # Extract race URLs
    race_urls = extract_race_urls(html_content)
    print(f"Found {len(race_urls)} race URLs.")
    
    # Limit the number of URLs to process
    if max_urls > 0 and len(race_urls) > max_urls:
        print(f"Limiting to {max_urls} URLs.")
        race_urls = race_urls[:max_urls]
    
    processed_count = 0
    skipped_already_processed = 0
    skipped_future_races = 0
    error_count = 0
    
    # Get the current date and time for checking future races
    current_datetime = datetime.now()
    
    # Process each race URL
    for i, url in enumerate(race_urls):
        try:
            # Check if URL has already been processed and is not a future race
            if is_url_fully_processed(url, conn):
                print(f"[{i+1}/{len(race_urls)}] Skipping already processed URL: {url}")
                skipped_already_processed += 1
                continue
            
            # If it's a pending future race, check if the race time has passed
            race_status = get_url_status(url, conn)
            if race_status == 'future' and not include_future:
                # Extract date and time from URL to check if race has already happened
                date_time_info = extract_date_time_from_url(url)
                
                if date_time_info and date_time_info['datetime'] > current_datetime:
                    # Race is still in the future, skip it for now
                    print(f"[{i+1}/{len(race_urls)}] Skipping future race (will retry later): {url}")
                    skipped_future_races += 1
                    continue
                else:
                    # Race time has passed, try to process it now
                    print(f"[{i+1}/{len(race_urls)}] Race time has passed, attempting to process: {url}")
            
            print(f"[{i+1}/{len(race_urls)}] Processing: {url}")
            
            # Fetch the race page
            race_html = fetch_racing_page(url)
            if not race_html:
                print(f"Failed to retrieve race page: {url}")
                error_count += 1
                continue
            
            # Extract race information
            race_info = extract_race_info(race_html, url, include_future)
            if not race_info:
                # If extract_race_info returns None, it could be because the race hasn't finished
                # Mark it as a future race in the database with status 'future'
                if not include_future:
                    # Extract date and time info to determine when to retry
                    date_time_info = extract_date_time_from_url(url)
                    if date_time_info:
                        mark_url_as_future(url, date_time_info['date'], date_time_info['time'], conn)
                        print(f"Marked as future race, will retry after: {date_time_info['date']} {date_time_info['time']}")
                    else:
                        # If we can't extract the race time, mark with current time + 24h as a fallback
                        future_time = (current_datetime + datetime.timedelta(hours=24)).strftime('%Y-%m-%d %H:%M:%S')
                        mark_url_as_future(url, future_time.split()[0], future_time.split()[1], conn)
                        print(f"Couldn't determine race time, will retry after: {future_time}")
                    
                    skipped_future_races += 1
                else:
                    # If include_future is True but we still couldn't extract data, mark as an error
                    print(f"Couldn't extract data even with include_future flag: {url}")
                    error_count += 1
                
                continue
            
            # Save to database
            row_id = save_to_database(race_info, conn)
            
            # Mark URL as fully processed
            mark_url_as_processed(url, conn)
            
            print(f"Saved race information with ID: {row_id}")
            print(f"Race: {race_info['racecourse']} - {race_info['time']} - {race_info['date']} - {race_info['race_name']}")
            processed_count += 1
            
            # Pause briefly to be respectful of the server
            delay = random.uniform(1.0, 3.0)
            time.sleep(delay)
            
        except Exception as e:
            print(f"Error processing {url}: {e}")
            error_count += 1
    
    print("\nCrawl Summary:")
    print(f"Total URLs found: {len(race_urls)}")
    print(f"Successfully processed: {processed_count}")
    print(f"Skipped (already processed): {skipped_already_processed}")
    print(f"Skipped (future races): {skipped_future_races}")
    print(f"Errors: {error_count}")
    
    return processed_count

def is_url_fully_processed(url, conn):
    """Check if a URL has already been fully processed (not a future race)."""
    cursor = conn.cursor()
    cursor.execute("SELECT status FROM processed_urls WHERE url = ? AND status = 'processed'", (url,))
    return cursor.fetchone() is not None

def get_url_status(url, conn):
    """Get the processing status of a URL."""
    cursor = conn.cursor()
    cursor.execute("SELECT status FROM processed_urls WHERE url = ?", (url,))
    result = cursor.fetchone()
    return result[0] if result else None

def mark_url_as_processed(url, conn):
    """Mark a URL as fully processed in the database."""
    cursor = conn.cursor()
    cursor.execute(
        "INSERT OR REPLACE INTO processed_urls (url, processed_at, status, race_date, race_time) VALUES (?, ?, 'processed', NULL, NULL)",
        (url, datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    )
    conn.commit()

def mark_url_as_future(url, race_date, race_time, conn):
    """Mark a URL as a future race to be processed later."""
    cursor = conn.cursor()
    cursor.execute(
        "INSERT OR REPLACE INTO processed_urls (url, processed_at, status, race_date, race_time) VALUES (?, ?, 'future', ?, ?)",
        (url, datetime.now().strftime('%Y-%m-%d %H:%M:%S'), race_date, race_time)
    )
    conn.commit()

def extract_date_time_from_url(url):
    """Extract the race date and time from a race URL."""
    # Try to extract date from URL format
    date_match = re.search(r'racecards/(\d{4}-\d{2}-\d{2})', url)
    if not date_match:
        return None
    
    race_date = date_match.group(1)
    
    # We need to fetch the page to get the time
    html_content = fetch_racing_page(url)
    if not html_content:
        return {'date': race_date, 'time': '00:00'}
    
    # Try to extract time from the content
    time_match = re.search(r'(\d{1,2}[:\.]\d{2})', html_content)
    race_time = time_match.group(1) if time_match else '00:00'
    
    # Convert to datetime for comparison
    try:
        date_parts = race_date.split('-')
        time_parts = race_time.replace('.', ':').split(':')
        
        race_datetime = datetime(
            year=int(date_parts[0]),
            month=int(date_parts[1]),
            day=int(date_parts[2]),
            hour=int(time_parts[0]),
            minute=int(time_parts[1])
        )
        
        return {
            'date': race_date,
            'time': race_time,
            'datetime': race_datetime
        }
    except (ValueError, IndexError):
        # If datetime conversion fails, return just the string values
        return {
            'date': race_date,
            'time': race_time,
            'datetime': datetime.now()  # Fallback to current time
        }

def retry_future_races(conn):
    """Retry processing races that were previously marked as future and whose time has now passed."""
    cursor = conn.cursor()
    
    # Get current date and time
    current_datetime = datetime.now()
    current_date = current_datetime.strftime('%Y-%m-%d')
    current_time = current_datetime.strftime('%H:%M')
    
    print(f"Looking for future races to retry (current time: {current_date} {current_time})")
    
    # Get all URLs marked as future races
    cursor.execute("""
    SELECT url, race_date, race_time 
    FROM processed_urls 
    WHERE status = 'future'
    """)
    
    future_races = cursor.fetchall()
    
    if not future_races:
        print("No future races found in the database.")
        return
    
    print(f"Found {len(future_races)} races marked as future")
    
    # Count statistics
    retried_count = 0
    processed_count = 0
    still_future_count = 0
    error_count = 0
    
    for race_data in future_races:
        url, race_date, race_time = race_data
        
        # Check if race time has passed
        try:
            race_datetime = datetime.strptime(f"{race_date} {race_time}", '%Y-%m-%d %H:%M')
            # Add a small buffer (60 minutes) after race time before trying to get results
            race_datetime_with_buffer = race_datetime + timedelta(minutes=60)
            
            if race_datetime_with_buffer > current_datetime:
                print(f"Race not yet due: {race_date} {race_time} - {url}")
                still_future_count += 1
                continue
        except ValueError:
            # If we can't parse the datetime, assume it's ready to process
            pass
        
        print(f"Attempting to process race whose time has passed: {race_date} {race_time} - {url}")
        retried_count += 1
        
        try:
            # Fetch the race page
            race_html = fetch_racing_page(url)
            if not race_html:
                print(f"Failed to retrieve race page: {url}")
                error_count += 1
                continue
            
            # Extract race information - don't include future here as we're looking for results
            race_info = extract_race_info(race_html, url, include_future=False)
            if not race_info:
                print(f"Race still doesn't have results, will try again later: {url}")
                # Update the last checked time but keep status as 'future'
                cursor.execute(
                    "UPDATE processed_urls SET processed_at = ? WHERE url = ?",
                    (current_datetime.strftime('%Y-%m-%d %H:%M:%S'), url)
                )
                conn.commit()
                continue
            
            # Save to database
            row_id = save_to_database(race_info, conn)
            
            # Mark URL as fully processed
            mark_url_as_processed(url, conn)
            
            print(f"Successfully processed race: {race_info['racecourse']} - {race_info['time']} - {race_info['date']} - {race_info['race_name']}")
            processed_count += 1
            
            # Pause briefly to be respectful of the server
            time.sleep(random.uniform(1.0, 2.0))
            
        except Exception as e:
            print(f"Error processing {url}: {e}")
            error_count += 1
    
    print("\nRetry Summary:")
    print(f"Total future races found: {len(future_races)}")
    print(f"Races still in future: {still_future_count}")
    print(f"Races retried: {retried_count}")
    print(f"Successfully processed: {processed_count}")
    print(f"Errors: {error_count}")
    
    # Now get database stats
    cursor.execute("SELECT COUNT(*) FROM races")
    total_races = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM processed_urls WHERE status = 'processed'")
    processed_urls = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM processed_urls WHERE status = 'future'")
    future_urls = cursor.fetchone()[0]
    
    print("\nDatabase Statistics:")
    print(f"Total races in database: {total_races}")
    print(f"Processed URLs: {processed_urls}")
    print(f"Future races queued: {future_urls}")

def list_future_races(conn):
    """List all races marked as future in the database."""
    cursor = conn.cursor()
    
    # Get current date and time
    current_datetime = datetime.now()
    current_date = current_datetime.strftime('%Y-%m-%d')
    current_time = current_datetime.strftime('%H:%M')
    
    print(f"Listing future races (current time: {current_date} {current_time})")
    
    # Get all URLs marked as future races
    cursor.execute("""
    SELECT url, race_date, race_time, processed_at 
    FROM processed_urls 
    WHERE status = 'future'
    ORDER BY race_date, race_time
    """)
    
    future_races = cursor.fetchall()
    
    if not future_races:
        print("No future races found in the database.")
        return
    
    print(f"Found {len(future_races)} races marked as future")
    print("\nRace Schedule:")
    print("==============")
    
    # Current date for grouping
    current_group_date = None
    
    for race_data in future_races:
        url, race_date, race_time, last_checked = race_data
        
        # Extract racecourse from URL
        racecourse_match = re.search(r'racecards/\d{4}-\d{2}-\d{2}/([^/]+)', url)
        racecourse = racecourse_match.group(1).replace('-', ' ').title() if racecourse_match else "Unknown"
        
        # Group by date
        if race_date != current_group_date:
            current_group_date = race_date
            print(f"\n{race_date}:")
            print("-" * len(race_date) + "-")
        
        # Check if race time has passed
        is_past = False
        try:
            race_datetime = datetime.strptime(f"{race_date} {race_time}", '%Y-%m-%d %H:%M')
            is_past = race_datetime < current_datetime
        except ValueError:
            # If we can't parse the datetime, assume it's in the future
            pass
        
        status = "PAST DUE" if is_past else "SCHEDULED"
        print(f"{race_time} - {racecourse} - {status}")
        print(f"  URL: {url}")
        print(f"  Last checked: {last_checked}")
    
    print("\nUse --retry-future to process races whose scheduled time has passed")

def main():
    # Parse command line arguments
    import argparse
    parser = argparse.ArgumentParser(description='Scrape racing data from Sporting Life')
    parser.add_argument('--url', type=str, default="https://www.sportinglife.com/racing/racecards",
                      help='URL to start crawling from (default: main racecards page)')
    parser.add_argument('--max-urls', type=int, default=50,
                      help='Maximum number of URLs to process (default: 50, use 0 for no limit)')
    parser.add_argument('--reprocess', type=int, default=0,
                      help='Reprocess a specific race ID (default: 0, meaning don\'t reprocess)')
    parser.add_argument('--reprocess-all', action='store_true',
                      help='Reprocess all races in the database')
    parser.add_argument('--include-future', action='store_true',
                      help='Include future races that have not finished yet')
    parser.add_argument('--retry-future', action='store_true',
                      help='Retry processing races that were previously marked as future')
    parser.add_argument('--list-future', action='store_true',
                      help='List all races marked as future in the database')
    args = parser.parse_args()
    
    print("Racing Data Scraper")
    print("==================")
    
    # Initialize the database
    conn = initialize_database()
    
    # List future races if requested
    if args.list_future:
        list_future_races(conn)
        conn.close()
        return
    
    if args.include_future:
        print("NOTE: --include-future flag set - will process races even without available results")
        print("      Future races will be marked for monitoring and can be retried later using --retry-future")
    else:
        print("NOTE: Only processing races that have finished with results available")
        print("      Use --include-future flag to include races without results")

    # Check if we should retry future races
    if args.retry_future:
        retry_future_races(conn)
        conn.close()
        return
    
    # Reprocess all races if requested
    if args.reprocess_all:
        print("Reprocessing all races in the database")
        # When reprocessing, we should respect the include_future flag
        reprocess_all_races(conn, args.include_future)
        conn.close()
        return
    
    # Reprocess a specific race if requested
    if args.reprocess > 0:
        print(f"Reprocessing race ID: {args.reprocess}")
        reprocess_race(args.reprocess, conn, args.include_future)
        conn.close()
        return
    
    # To process a single URL
    if '/racecard/' in args.url:
        print(f"Scraping single race: {args.url}")
        html_content = fetch_racing_page(args.url)
        
        if html_content:
            race_info = extract_race_info(html_content, args.url, args.include_future)
            
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
                
                # Mark URL as processed OR as future depending on the date
                date_time_info = extract_date_time_from_url(args.url)
                current_datetime = datetime.now()
                
                print("\nDebug Information:")
                print(f"Current datetime: {current_datetime}")
                if date_time_info:
                    print(f"Race date: {date_time_info['date']}")
                    print(f"Race time: {date_time_info['time']}")
                    print(f"Race datetime: {date_time_info['datetime']}")
                    print(f"Is future race: {date_time_info['datetime'] > current_datetime}")
                else:
                    print("Could not extract date/time from URL")
                
                # If the race has a date in the future, mark it as a future race
                if date_time_info and race_info['date']:
                    # Force it to be marked as a future race if the date is in the future
                    try:
                        race_date = datetime.strptime(race_info['date'], '%Y-%m-%d').date()
                        current_date = current_datetime.date()
                        is_future = race_date > current_date
                        
                        if is_future and args.include_future:
                            print(f"\nDetected future race on {race_date}")
                            # Use the date and time from the race_info (which we saved to DB)
                            mark_url_as_future(args.url, race_info['date'], race_info['time'], conn)
                            print(f"Marked URL as a future race for monitoring")
                            print("NOTE: Future race has been saved, but no results are available yet.")
                            print("      This race will be monitored and updated when results are available.")
                        else:
                            # Mark as fully processed
                            mark_url_as_processed(args.url, conn)
                    except ValueError:
                        # If date parsing fails, fall back to normal processing
                        print(f"Warning: Could not parse race date '{race_info['date']}' for future race detection")
                        mark_url_as_processed(args.url, conn)
                else:
                    # Normal case - mark as fully processed
                    mark_url_as_processed(args.url, conn)
            else:
                if args.include_future:
                    print("\nNo race information could be extracted.")
                    print("Even with --include-future flag, the race data could not be processed.")
                    print("This could be due to an invalid URL or a page format that the scraper doesn't recognize.")
                else:
                    print("\nRace hasn't finished yet or is scheduled in the future.")
                    print("Use --include-future to process it anyway, but note that:")
                    print("- No race results will be available for future races")
                    print("- The race will be marked for monitoring until results become available")
        else:
            print("Failed to retrieve the racing page.")
    
    # Crawl multiple URLs
    else:
        print(f"Crawling race URLs from {args.url}")
        processed_count = crawl_race_urls(conn, args.max_urls, args.url, args.include_future)
        
        # Display database stats
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM races")
        total_races = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM processed_urls")
        total_processed = cursor.fetchone()[0]
        
        print("\nDatabase Statistics:")
        print(f"Total races in database: {total_races}")
        print(f"Total processed URLs: {total_processed}")
        
        if processed_count > 0:
            print("\nScraping completed successfully!")
        else:
            print("\nNo new races were scraped.")
    
    # Check for races with unknown values that might need reprocessing
    cursor = conn.cursor()
    cursor.execute("""
    SELECT id, racecourse, time, date 
    FROM races 
    WHERE 
        age_restrictions = 'Unknown' OR
        class = 'Unknown' OR 
        distance = 'Unknown' OR 
        going = 'Unknown' OR 
        surface = 'Unknown'
    """)
    
    races_to_fix = cursor.fetchall()
    if races_to_fix:
        print("\nRaces with 'Unknown' values that might need fixing:")
        for race in races_to_fix:
            print(f"ID: {race[0]} - {race[1]} - {race[2]} - {race[3]}")
        print("\nYou can reprocess a specific race using: python newnewmarket.py --reprocess [ID]")
        print("Or reprocess all races using: python newnewmarket.py --reprocess-all")
    
    # Close the database connection
    conn.close()

if __name__ == "__main__":
    main()
