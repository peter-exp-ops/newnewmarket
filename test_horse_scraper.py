import sqlite3
import os
from Scraper import scrape_horse_page, save_data_to_database, process_scraped_data
from Database_Operations import initialize_database, connect_to_database

def test_horse_scraping():
    print("Testing horse scraping functionality...")
    
    # Test URL - Osculation (Horse ID: 1)
    test_url = "https://www.sportinglife.com/racing/profiles/horse/1"
    
    # Step 1: Scrape the horse data
    print("\nStep 1: Scraping horse data from", test_url)
    horse_data = scrape_horse_page(test_url)
    
    if not horse_data:
        print("Failed to scrape horse data!")
        return False
    
    print("\nRaw horse data:")
    for key, value in horse_data.items():
        print(f"{key}: {value}")
    
    # Step 2: Process the scraped data
    print("\nStep 2: Processing scraped data")
    processed_data = process_scraped_data([horse_data], 'horses')
    
    # Step 3: Connect to database
    print("\nStep 3: Connecting to database")
    # Initialize the database if it doesn't exist
    if not os.path.exists('racing_data.db'):
        print("Creating new database...")
        conn = initialize_database('racing_data.db')
    else:
        print("Connecting to existing database...")
        conn = connect_to_database('racing_data.db')
    
    if not conn:
        print("Failed to connect to database!")
        return False
    
    # Step 4: Save data to database
    print("\nStep 4: Saving horse data to database")
    records_saved = save_data_to_database(conn, processed_data, 'horses')
    
    print(f"Saved {records_saved} records to database")
    
    # Step 5: Verify data was saved correctly
    print("\nStep 5: Verifying data in database")
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM horses WHERE ID = ?", (horse_data['id'],))
    db_record = cursor.fetchone()
    
    if db_record:
        print("\nHorse record in database:")
        cursor.execute("PRAGMA table_info(horses)")
        columns = [column[1] for column in cursor.fetchall()]
        
        for i, col_name in enumerate(columns):
            print(f"{col_name}: {db_record[i]}")
        
        # Check that the important fields match what we expect for Osculation
        expected_values = {
            'Name': 'Osculation',
            'Foaled': '17th September 2013',
            'Sex': 'Filly',
            'Trainer': 'Roy Magner',
            'TrainerID': 1,
            'Sire': 'Argonaut',
            'Dam': 'Kissing Cousin',
            'Owner': 'Mr G R Sadleir'
        }
        
        column_indices = {col_name: i for i, col_name in enumerate(columns)}
        
        success = True
        for field, expected in expected_values.items():
            if field in column_indices:
                actual = db_record[column_indices[field]]
                if str(actual) != str(expected):
                    print(f"Mismatch in {field}. Expected: {expected}, Got: {actual}")
                    success = False
        
        if success:
            print("\nSUCCESS: All horse data fields match expected values!")
        else:
            print("\nFAIL: Some horse data fields did not match expected values.")
        
        return success
    else:
        print("Failed to find the horse record in the database!")
        return False
    
if __name__ == "__main__":
    success = test_horse_scraping()
    print("\nTest completed with result:", "SUCCESS" if success else "FAILURE") 