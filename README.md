# Racing Data Scraper

A Python script that extracts horse racing data from Sporting Life racing cards.

## Features

- Extracts horse names from racing cards
- Organizes data by race
- Saves extracted data to CSV file
- Robust scraping with multiple fallback methods for different page structures

## Requirements

- Python 3.6 or higher
- Required packages:
  - requests
  - beautifulsoup4
  - pandas

## Installation

1. Clone this repository
2. Install required packages:
   ```
   pip install -r requirements.txt
   ```

## Usage

Run the script directly:

```
python newnewmarket.py
```

The script will:
1. Scrape the racing page at `https://www.sportinglife.com/racing/fast-cards/82929/2021-04-02/unknown`
2. Extract all horse names from each race
3. Print the horse names to the console
4. Save all data to a CSV file named `horses_data.csv`

## Output

The script generates a CSV file with the following columns:
- Race: The race name/number
- Horse: The horse name

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- Contributors and data sources will be acknowledged here 