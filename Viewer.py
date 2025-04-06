# -*- coding: utf-8 -*-
import sqlite3
import pandas as pd
import os
import webbrowser
from datetime import datetime
import http.server
import socketserver
import threading
import urllib.parse

# Global variable to track if server is running
server_thread = None
PORT = 8000

class RacingDataHandler(http.server.SimpleHTTPRequestHandler):
    """Custom HTTP request handler for racing data viewer"""
    
    def do_GET(self):
        """Handle GET requests"""
        # Handle all requests with the standard handler
        return http.server.SimpleHTTPRequestHandler.do_GET(self)
    
    def log_message(self, format, *args):
        """Suppress server logs"""
        return

def start_server():
    """Start the HTTP server in a separate thread"""
    global server_thread
    
    # Only start if not already running
    if server_thread is None or not server_thread.is_alive():
        # Create a server
        handler = RacingDataHandler
        httpd = socketserver.TCPServer(("", PORT), handler)
        
        # Start server in a new thread
        server_thread = threading.Thread(target=httpd.serve_forever)
        server_thread.daemon = True  # So it exits when the main program exits
        server_thread.start()
        print(f"Server started on port {PORT}")

def generate_html_report():
    """Generate an HTML report from the racing_data.db file with a tabbed interface"""
    try:
        # Connect to the database
        conn = sqlite3.connect('racing_data.db')
        
        # Get table names
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        table_names = [table[0] for table in tables]
        
        # Check if we have future races
        has_future_races = False
        future_races_data = []
        try:
            cursor.execute("SELECT url, race_date, race_time FROM processed_urls WHERE status='future' ORDER BY race_date, race_time")
            future_races_data = cursor.fetchall()
            has_future_races = len(future_races_data) > 0
        except sqlite3.OperationalError:
            # Table might not have the required columns yet
            pass
            
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
                    padding: 40px 0;
                    text-align: center;
                    margin-bottom: 20px;
                }}
                .hero-image {{
                    max-width: 100px;
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
                .refresh-button {{
                    background-color: #4c6ef5;
                    color: white;
                    border: none;
                    padding: 10px 15px;
                    border-radius: 4px;
                    cursor: pointer;
                    font-size: 16px;
                    transition: background-color 0.3s;
                }}
                .refresh-button:hover {{
                    background-color: #364fc7;
                }}
                /* Tab styling */
                .tabs {{
                    overflow: hidden;
                    background-color: #f1f1f1;
                    border-radius: 4px 4px 0 0;
                }}
                .tabs button {{
                    background-color: inherit;
                    float: left;
                    border: none;
                    outline: none;
                    cursor: pointer;
                    padding: 14px 16px;
                    transition: 0.3s;
                    font-size: 16px;
                }}
                .tabs button:hover {{
                    background-color: #ddd;
                }}
                .tabs button.active {{
                    background-color: #4c6ef5;
                    color: white;
                }}
                .tabcontent {{
                    display: none;
                    padding: 6px 12px;
                    border: 1px solid #ccc;
                    border-top: none;
                    border-radius: 0 0 4px 4px;
                    background-color: white;
                }}
                /* Table styling */
                .table-container {{
                    overflow-x: auto;
                    max-height: 600px;
                    margin-top: 20px;
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
                    white-space: nowrap;
                }}
                th {{
                    position: sticky;
                    top: 0;
                    background-color: #4c6ef5;
                    color: white;
                    z-index: 10;
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
                /* URL styling */
                .url-cell {{
                    max-width: 300px;
                    overflow: hidden;
                    text-overflow: ellipsis;
                    white-space: nowrap;
                }}
                .url-cell:hover {{
                    overflow: visible;
                    white-space: normal;
                    word-break: break-all;
                }}
                /* Status styling */
                .status-processed {{
                    background-color: #d4edda;
                    color: #155724;
                    border-radius: 4px;
                    padding: 3px 6px;
                }}
                .status-future {{
                    background-color: #cce5ff;
                    color: #004085;
                    border-radius: 4px;
                    padding: 3px 6px;
                }}
                .status-error {{
                    background-color: #f8d7da;
                    color: #721c24;
                    border-radius: 4px;
                    padding: 3px 6px;
                }}
                /* Future races section */
                .future-races-section {{
                    background-color: #e6f7ff;
                    border: 1px solid #91d5ff;
                    border-radius: 4px;
                    padding: 15px;
                    margin-bottom: 20px;
                }}
                .future-races-table {{
                    width: 100%;
                    border-collapse: collapse;
                }}
                .future-races-table th {{
                    background-color: #1890ff;
                }}
                .future-date-header {{
                    background-color: #f0f5ff;
                    font-weight: bold;
                    text-align: left;
                    padding: 8px;
                    border-bottom: 1px solid #d9d9d9;
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
                <div class="info-box">
                    <div>
                        <h2>Database Information</h2>
                        <p>Tables in database: {', '.join(table_names)}</p>
                        <p>Report generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                    </div>
                </div>
        """
        
        # Add future races section if we have future races
        if has_future_races:
            html_content += """
                <div class="future-races-section">
                    <h2>Upcoming Races</h2>
                    <p>Races that have been identified but are scheduled for the future:</p>
                    <div class="table-container">
                        <table class="future-races-table">
                            <thead>
                                <tr>
                                    <th>Date</th>
                                    <th>Time</th>
                                    <th>URL</th>
                                </tr>
                            </thead>
                            <tbody>
            """
            
            # Group future races by date
            current_date = None
            for url, race_date, race_time in future_races_data:
                # Format the date for display
                if race_date != current_date:
                    current_date = race_date
                    html_content += f"""
                                <tr>
                                    <td colspan="3" class="future-date-header">{race_date}</td>
                                </tr>
                    """
                
                html_content += f"""
                                <tr>
                                    <td>{race_date}</td>
                                    <td>{race_time}</td>
                                    <td class="url-cell"><a href="{url}" target="_blank">{url}</a></td>
                                </tr>
                """
            
            html_content += """
                            </tbody>
                        </table>
                    </div>
                </div>
            """
                
        # Generate tabs section
        html_content += """
                <div class="tabs">
        """
        
        # Generate tab buttons
        for i, table in enumerate(table_names):
            active = "active" if i == 0 else ""
            html_content += f'<button class="tablinks {active}" onclick="openTab(event, \'{table}\')">{table}</button>\n'
        
        html_content += """
                </div>
        """
        
        # Generate tab content for each table
        for i, table in enumerate(table_names):
            # Get data for this table
            query = f"SELECT * FROM {table}"
            df = pd.read_sql_query(query, conn)
            
            # Get column information
            cursor.execute(f"PRAGMA table_info({table})")
            columns = cursor.fetchall()
            column_names = [col[1] for col in columns]
            
            # Count rows in the table
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            
            # Set display style for first tab
            display = "block" if i == 0 else "none"
            
            html_content += f"""
                <div id="{table}" class="tabcontent" style="display: {display};">
                    <h3>{table.capitalize()} Table</h3>
                    <p>Total rows: {count}</p>
                    <div class="table-container">
                        <table>
                            <thead>
                                <tr>
            """
            
            # Add table headers
            for col in column_names:
                html_content += f"<th>{col}</th>\n"
            
            html_content += """
                                </tr>
                            </thead>
                            <tbody>
            """
            
            # Add table rows
            if not df.empty:
                for _, row in df.iterrows():
                    html_content += "<tr>\n"
                    for col in column_names:
                        # Special handling for URL fields
                        if 'url' in col.lower():
                            html_content += f'<td class="url-cell"><a href="{row[col]}" target="_blank">{row[col]}</a></td>\n'
                        # Special handling for status field in processed_urls table
                        elif table == 'processed_urls' and col == 'status':
                            status_class = 'status-processed'
                            if row[col] == 'future':
                                status_class = 'status-future'
                            elif row[col] == 'error':
                                status_class = 'status-error'
                            html_content += f'<td><span class="{status_class}">{row[col]}</span></td>\n'
                        else:
                            cell_value = row[col] if pd.notna(row[col]) else ""
                            html_content += f"<td>{cell_value}</td>\n"
                    html_content += "</tr>\n"
            else:
                html_content += f"<tr><td colspan='{len(column_names)}'>No data found in this table</td></tr>\n"
            
            html_content += """
                            </tbody>
                        </table>
                    </div>
                </div>
            """
        
        # Add JavaScript for tab functionality
        html_content += """
                <div class="footer">
                    <p>Generated by Racing Database Viewer</p>
                </div>
            </div>
            
            <script>
            function openTab(evt, tabName) {
                var i, tabcontent, tablinks;
                
                // Hide all tab content
                tabcontent = document.getElementsByClassName("tabcontent");
                for (i = 0; i < tabcontent.length; i++) {
                    tabcontent[i].style.display = "none";
                }
                
                // Remove active class from all tab buttons
                tablinks = document.getElementsByClassName("tablinks");
                for (i = 0; i < tablinks.length; i++) {
                    tablinks[i].className = tablinks[i].className.replace(" active", "");
                }
                
                // Show the current tab and add active class to the button
                document.getElementById(tabName).style.display = "block";
                evt.currentTarget.className += " active";
            }
            </script>
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
        
        # Check for future races
        try:
            cursor.execute("SELECT COUNT(*) FROM processed_urls WHERE status='future'")
            future_count = cursor.fetchone()[0]
            if future_count > 0:
                print(f"\nFound {future_count} future races in the processed_urls table")
                print("To view details, run newnewmarket.py with the --list-future flag")
        except sqlite3.OperationalError:
            # Table might not have the required column
            pass
            
        # For each table, get and display information
        for table in tables:
            table_name = table[0]
            
            # Get column information
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = cursor.fetchall()
            print(f"\nColumns in {table_name} table:")
            for col in columns:
                print(f"  {col[1]} ({col[2]})")
            
            # Count rows in the table
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            print(f"\nTotal rows in {table_name} table: {count}")
            
            # Get all data and display it using pandas (limited to first 10 rows)
            query = f"SELECT * FROM {table_name} LIMIT 10"
            df = pd.read_sql_query(query, conn)
            
            # Print the DataFrame with all columns
            pd.set_option('display.max_columns', None)
            pd.set_option('display.width', 1000)
            print(f"\nData in {table_name} table (first 10 rows):")
            print(df)
            print("-" * 80)
        
        # Close the connection
        conn.close()
        
    except sqlite3.Error as e:
        print(f"Database error: {e}")
    except Exception as e:
        print(f"Error: {e}")

def open_html_report():
    """Generate the report and open it in a browser using a local server"""
    try:
        # Generate HTML report
        generate_html_report()
        
        # Start the HTTP server if not already running
        start_server()
        
        # Open the report in the default browser
        url = f"http://localhost:{PORT}/viewer.html"
        print(f"Opening in browser: {url}")
        webbrowser.open(url)
        
        return True
        
    except sqlite3.Error as e:
        print(f"Database error: {e}")
        return False
    except Exception as e:
        print(f"Error: {e}")
        return False

if __name__ == "__main__":
    open_html_report()
    print("\nAlso printing database contents to console:")
    check_database()
    
    # Keep the main thread running to maintain the server
    try:
        while server_thread and server_thread.is_alive():
            # Check every 5 seconds if we should continue
            server_thread.join(5)
    except KeyboardInterrupt:
        print("Shutting down...") 