# -*- coding: utf-8 -*-
import sqlite3
import pandas as pd
import os
import webbrowser
from datetime import datetime
import base64

def generate_html_report():
    """Generate an HTML report from the racing_data.db file"""
    try:
        # Connect to the database
        conn = sqlite3.connect('racing_data.db')
        
        # Get all race data
        query = "SELECT * FROM races"
        df = pd.read_sql_query(query, conn)
        
        # Get table names
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        
        # Get column information for the races table
        cursor.execute("PRAGMA table_info(races)")
        columns = cursor.fetchall()
        
        # Count rows in the races table
        cursor.execute("SELECT COUNT(*) FROM races")
        count = cursor.fetchone()[0]
        
        # Get paths to icon files
        current_dir = os.path.dirname(os.path.abspath(__file__))
        favicon_path = os.path.join(current_dir, 'Icon 32px.png')
        hero_image_path = os.path.join(current_dir, 'Icon 512px.png')
        
        # Read the favicon and encode it as base64
        favicon_base64 = ""
        if os.path.exists(favicon_path):
            with open(favicon_path, 'rb') as f:
                favicon_data = f.read()
                favicon_base64 = base64.b64encode(favicon_data).decode('utf-8')
        
        # Generate HTML content
        html_content = f"""
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>Racing Database Viewer</title>
            <!-- Favicon -->
            <link rel="icon" href="Icon 32px.png" type="image/png">
            <style>
                body {{
                    font-family: Arial, sans-serif;
                    margin: 0;
                    padding: 0;
                    background-color: #f5f5f5;
                    color: #333;
                }}
                h1, h2, h3 {{
                    color: #2c3e50;
                }}
                .hero {{
                    position: relative;
                    background-color: #4c6ef5;
                    color: white;
                    padding: 80px 0;
                    text-align: center;
                    margin-bottom: 40px;
                }}
                .hero-image {{
                    max-width: 150px;
                    display: block;
                    margin: 0 auto 20px;
                }}
                .hero h1 {{
                    color: white;
                    margin: 0;
                    font-size: 2.5em;
                }}
                .hero p {{
                    font-size: 1.2em;
                    margin: 10px 0 0;
                    opacity: 0.9;
                }}
                .container {{
                    max-width: 1200px;
                    margin: 0 auto;
                    padding: 0 20px 40px;
                }}
                .info-box {{
                    background-color: #f8f9fa;
                    border: 1px solid #dee2e6;
                    border-radius: 4px;
                    padding: 15px;
                    margin-bottom: 20px;
                }}
                table {{
                    width: 100%;
                    border-collapse: collapse;
                    margin: 20px 0;
                }}
                th, td {{
                    padding: 12px 15px;
                    border-bottom: 1px solid #ddd;
                    text-align: left;
                }}
                th {{
                    background-color: #4c6ef5;
                    color: white;
                }}
                tr:nth-child(even) {{
                    background-color: #f2f2f2;
                }}
                tr:hover {{
                    background-color: #e9ecef;
                }}
                .footer {{
                    margin-top: 30px;
                    text-align: center;
                    font-size: 0.8em;
                    color: #6c757d;
                    padding-bottom: 20px;
                }}
            </style>
        </head>
        <body>
            <div class="hero">
                <img src="Icon 512px.png" alt="Racing Database" class="hero-image">
                <h1>Racing Database Viewer</h1>
                <p>View and explore your horse racing data</p>
            </div>
            
            <div class="container">
                <p>Report generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                
                <div class="info-box">
                    <h2>Database Information</h2>
                    <p>Tables in database: {', '.join([table[0] for table in tables])}</p>
                    <p>Total races: {count}</p>
                </div>
                
                <h2>Race Information</h2>
        """
        
        if df.empty:
            html_content += "<p>No race data found in the database.</p>"
        else:
            # First add a summary table with key race info
            html_content += """
                <h3>Race Summary</h3>
                <table>
                    <tr>
                        <th>ID</th>
                        <th>Racecourse</th>
                        <th>Date</th>
                        <th>Time</th>
                        <th>Race Name</th>
                        <th>Class</th>
                        <th>Runners</th>
                    </tr>
            """
            
            for _, row in df.iterrows():
                html_content += f"""
                    <tr>
                        <td>{row['id']}</td>
                        <td>{row['racecourse']}</td>
                        <td>{row['date']}</td>
                        <td>{row['time']}</td>
                        <td>{row['race_name']}</td>
                        <td>{row['class']}</td>
                        <td>{row['runners']}</td>
                    </tr>
                """
            
            html_content += "</table>"
            
            # Then add detailed race cards
            html_content += "<h3>Detailed Race Cards</h3>"
            
            for _, row in df.iterrows():
                html_content += f"""
                <div class="info-box">
                    <h3>{row['racecourse']} - {row['time']} - {row['date']}</h3>
                    <h4>{row['race_name']}</h4>
                    <table>
                        <tr>
                            <th>Field</th>
                            <th>Value</th>
                        </tr>
                        <tr><td>Racecourse</td><td>{row['racecourse']}</td></tr>
                        <tr><td>Date</td><td>{row['date']}</td></tr>
                        <tr><td>Time</td><td>{row['time']}</td></tr>
                        <tr><td>Race Name</td><td>{row['race_name']}</td></tr>
                        <tr><td>Age Restrictions</td><td>{row['age_restrictions']}</td></tr>
                        <tr><td>Class</td><td>{row['class']}</td></tr>
                        <tr><td>Distance</td><td>{row['distance']}</td></tr>
                        <tr><td>Going</td><td>{row['going']}</td></tr>
                        <tr><td>Runners</td><td>{row['runners']}</td></tr>
                        <tr><td>Surface</td><td>{row['surface']}</td></tr>
                        <tr><td>Source URL</td><td><a href="{row['url']}" target="_blank">View at Sporting Life</a></td></tr>
                        <tr><td>Scraped at</td><td>{row['scraped_at']}</td></tr>
                    </table>
                </div>
                """
        
        html_content += """
                <div class="footer">
                    <p>Generated by Racing Database Viewer</p>
                </div>
            </div>
        </body>
        </html>
        """
        
        # Close the connection
        conn.close()
        
        # Get current directory and create file path
        current_dir = os.path.dirname(os.path.abspath(__file__))
        html_file = os.path.join(current_dir, 'viewer.html')
        
        # Write HTML to file
        with open(html_file, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        print(f"HTML report generated: {html_file}")
        
        # Open in default browser
        file_url = 'file://' + os.path.abspath(html_file)
        print(f"Opening in browser: {file_url}")
        webbrowser.open(file_url)
        
        return True
        
    except sqlite3.Error as e:
        print(f"Database error: {e}")
        return False
    except Exception as e:
        print(f"Error: {e}")
        return False

def check_database():
    """Check the contents of the racing_data.db file and print to console"""
    try:
        # Connect to the database
        conn = sqlite3.connect('racing_data.db')
        
        # Create a cursor
        cursor = conn.cursor()
        
        # Get table names
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        print(f"Tables in database: {[table[0] for table in tables]}")
        
        # Get column information for the races table
        cursor.execute("PRAGMA table_info(races)")
        columns = cursor.fetchall()
        print("\nColumns in races table:")
        for col in columns:
            print(f"  {col[1]} ({col[2]})")
        
        # Count rows in the races table
        cursor.execute("SELECT COUNT(*) FROM races")
        count = cursor.fetchone()[0]
        print(f"\nTotal races in database: {count}")
        
        # Get all race data and display it using pandas
        query = "SELECT * FROM races"
        df = pd.read_sql_query(query, conn)
        
        # Print the DataFrame with all columns
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', 1000)
        print("\nRace data in database:")
        print(df)
        
        # Close the connection
        conn.close()
        
    except sqlite3.Error as e:
        print(f"Database error: {e}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    generate_html_report()
    print("\nAlso printing database contents to console:")
    check_database() 