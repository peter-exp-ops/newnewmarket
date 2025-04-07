import tkinter as tk
from tkinter import ttk
from tkinter import scrolledtext
import sqlite3
from tkinter import messagebox
import os

class ScraperUI:
    def __init__(self, root):
        self.root = root
        self.root.title("Scraper")
        
        # Set window icon
        icon_path = os.path.join(os.path.dirname(__file__), "Icon 32px.png")
        if os.path.exists(icon_path):
            self.root.iconphoto(True, tk.PhotoImage(file=icon_path))
        
        self.db_connection = None
        
        # Default values
        self.default_base_url = "https://www.sportinglife.com/racing/results/"
        self.default_timeout = "1"
        self.default_max_urls = "100"
        self.default_saturation_limit = "5"
        
        # Create main sections
        self.create_database_section()
        self.create_crawl_section()
        self.create_scrape_section()
        self.create_log_section()

    def connect_to_database(self):
        """Connect to the SQLite database."""
        try:
            self.db_connection = sqlite3.connect('racing_data.db')
            self.log_message("Successfully connected to database")
            self.refresh_stats()
            return True
        except sqlite3.Error as e:
            self.log_message(f"Database connection error: {e}")
            messagebox.showerror("Database Error", f"Failed to connect to database: {e}")
            return False

    def refresh_stats(self):
        """Refresh the statistics in the table."""
        if not self.db_connection:
            self.log_message("Cannot refresh stats: No database connection")
            messagebox.showwarning("Warning", "Please connect to database first")
            return

        try:
            cursor = self.db_connection.cursor()
            
            # Get counts for each type
            stats = {}
            total_stats = {'unprocessed': 0, 'failed': 0, 'succeeded': 0, 'total': 0}
            
            for type_ in ['races', 'jockeys', 'trainers', 'horses']:
                cursor.execute(f"""
                    SELECT 
                        COUNT(CASE WHEN status = 'unprocessed' THEN 1 END) as unprocessed,
                        COUNT(CASE WHEN status = 'failed' THEN 1 END) as failed,
                        COUNT(CASE WHEN status = 'succeeded' THEN 1 END) as succeeded,
                        COUNT(*) as total
                    FROM urls 
                    WHERE type = ?
                """, (type_,))
                
                result = cursor.fetchone()
                stats[type_] = {
                    'unprocessed': result[0],
                    'failed': result[1],
                    'succeeded': result[2],
                    'total': result[3]
                }
                
                # Update totals
                total_stats['unprocessed'] += result[0]
                total_stats['failed'] += result[1]
                total_stats['succeeded'] += result[2]
                total_stats['total'] += result[3]
            
            # Update the treeview
            for item in self.stats_tree.get_children():
                type_ = self.stats_tree.item(item)['values'][0]
                if type_ == 'total':
                    values = (
                        type_,
                        total_stats['unprocessed'],
                        total_stats['failed'],
                        total_stats['succeeded'],
                        total_stats['total']
                    )
                else:
                    type_stats = stats[type_]
                    values = (
                        type_,
                        type_stats['unprocessed'],
                        type_stats['failed'],
                        type_stats['succeeded'],
                        type_stats['total']
                    )
                self.stats_tree.item(item, values=values)
            
            self.log_message("Statistics refreshed successfully")
            
        except sqlite3.Error as e:
            self.log_message(f"Error refreshing stats: {e}")
            messagebox.showerror("Database Error", f"Failed to refresh statistics: {e}")

    def log_message(self, message):
        """Add a message to the log output."""
        self.log_output.insert(tk.END, f"{message}\n")
        self.log_output.see(tk.END)

    def create_database_section(self):
        # Database Connection Frame
        db_frame = ttk.LabelFrame(self.root, text="Database Connection", padding="5")
        db_frame.grid(row=0, column=0, padx=5, pady=5, sticky="nsew")
        
        # Configure grid columns to expand
        db_frame.grid_columnconfigure(0, weight=1)
        db_frame.grid_columnconfigure(1, weight=1)

        # Buttons
        connect_btn = ttk.Button(db_frame, text="Connect to Database", command=self.connect_to_database)
        connect_btn.grid(row=0, column=0, padx=5, pady=5, sticky="ew")
        
        refresh_btn = ttk.Button(db_frame, text="Refresh Stats", command=self.refresh_stats)
        refresh_btn.grid(row=0, column=1, padx=5, pady=5, sticky="ew")

        # Stats Table
        columns = ('type', 'unprocessed', 'failed', 'succeeded', 'total')
        self.stats_tree = ttk.Treeview(db_frame, columns=columns, show='headings', height=6)
        
        # Set column headings
        self.stats_tree.heading('type', text='Type')
        self.stats_tree.column('type', width=100)
        
        for col in ['unprocessed', 'failed', 'succeeded', 'total']:
            self.stats_tree.heading(col, text=col.capitalize())
            self.stats_tree.column(col, width=100)

        # Add rows
        rows = ['races', 'jockeys', 'trainers', 'horses', 'total']
        for row in rows:
            self.stats_tree.insert('', 'end', values=(row, 0, 0, 0, 0))

        self.stats_tree.grid(row=1, column=0, columnspan=2, padx=5, pady=5, sticky="ew")

    def create_crawl_section(self):
        # Crawl Frame
        crawl_frame = ttk.LabelFrame(self.root, text="Crawl", padding="5")
        crawl_frame.grid(row=1, column=0, padx=5, pady=5, sticky="nsew")
        
        # Configure grid columns to expand
        crawl_frame.grid_columnconfigure(1, weight=1)

        # Base URL
        ttk.Label(crawl_frame, text="Base URL:").grid(row=0, column=0, padx=5, pady=5, sticky="w")
        self.base_url_entry = ttk.Entry(crawl_frame)
        self.base_url_entry.insert(0, self.default_base_url)
        self.base_url_entry.grid(row=0, column=1, columnspan=2, padx=5, pady=5, sticky="ew")

        # Timeout
        ttk.Label(crawl_frame, text="Timeout (mins):").grid(row=1, column=0, padx=5, pady=5, sticky="w")
        self.timeout_entry = ttk.Entry(crawl_frame, width=10)
        self.timeout_entry.insert(0, self.default_timeout)
        self.timeout_entry.grid(row=1, column=1, padx=5, pady=5, sticky="w")

        # Max URLs
        ttk.Label(crawl_frame, text="Max URLs:").grid(row=2, column=0, padx=5, pady=5, sticky="w")
        self.max_urls_entry = ttk.Entry(crawl_frame, width=10)
        self.max_urls_entry.insert(0, self.default_max_urls)
        self.max_urls_entry.grid(row=2, column=1, padx=5, pady=5, sticky="w")
        
        # Saturation Limit
        ttk.Label(crawl_frame, text="Saturation limit (%):").grid(row=3, column=0, padx=5, pady=5, sticky="w")
        self.saturation_limit_entry = ttk.Entry(crawl_frame, width=10)
        self.saturation_limit_entry.insert(0, self.default_saturation_limit)
        self.saturation_limit_entry.grid(row=3, column=1, padx=5, pady=5, sticky="w")

        # Crawl Button
        crawl_btn = ttk.Button(crawl_frame, text="Crawl")
        crawl_btn.grid(row=4, column=0, columnspan=2, pady=10, sticky="ew")

    def create_scrape_section(self):
        # Scrape Frame
        scrape_frame = ttk.LabelFrame(self.root, text="Scrape", padding="5")
        scrape_frame.grid(row=2, column=0, padx=5, pady=5, sticky="nsew")
        
        # Configure grid columns to expand
        scrape_frame.grid_columnconfigure(1, weight=1)

        # Timeout
        ttk.Label(scrape_frame, text="Timeout (mins):").grid(row=0, column=0, padx=5, pady=5, sticky="w")
        self.scrape_timeout_entry = ttk.Entry(scrape_frame, width=10)
        self.scrape_timeout_entry.insert(0, self.default_timeout)
        self.scrape_timeout_entry.grid(row=0, column=1, padx=5, pady=5, sticky="w")

        # Max URLs
        ttk.Label(scrape_frame, text="Max URLs:").grid(row=1, column=0, padx=5, pady=5, sticky="w")
        self.scrape_max_urls_entry = ttk.Entry(scrape_frame, width=10)
        self.scrape_max_urls_entry.insert(0, self.default_max_urls)
        self.scrape_max_urls_entry.grid(row=1, column=1, padx=5, pady=5, sticky="w")

        # Scrape Button
        scrape_btn = ttk.Button(scrape_frame, text="Scrape")
        scrape_btn.grid(row=2, column=0, columnspan=2, pady=10, sticky="ew")

    def create_log_section(self):
        # Log Frame
        log_frame = ttk.LabelFrame(self.root, text="Log", padding="5")
        log_frame.grid(row=3, column=0, padx=5, pady=5, sticky="nsew")
        
        # Configure grid columns to expand
        log_frame.grid_columnconfigure(0, weight=1)

        # Log Output
        self.log_output = scrolledtext.ScrolledText(log_frame, width=60, height=10)
        self.log_output.grid(row=0, column=0, padx=5, pady=5, sticky="nsew")

def main():
    root = tk.Tk()
    app = ScraperUI(root)
    root.mainloop()

if __name__ == "__main__":
    main() 