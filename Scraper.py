#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Scraper.py - UI for racing data scraping operations

This script provides a basic user interface for scraping racing data
with sections for database connection, crawling, scraping, and output.
"""

print("Starting Scraper.py...")

import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox
import os
import sys
import sqlite3
import pandas as pd

class ScraperUI:
    def __init__(self, root):
        print("Initializing ScraperUI...")
        self.root = root
        self.root.title("Newmarket - Racing Data Scraper")
        self.root.geometry("800x600")
        
        # Database connection
        self.conn = None
        
        # Set application icon if available
        try:
            icon_path = "Icon 32px.png"
            if os.path.exists(icon_path):
                # For Windows
                self.root.iconbitmap(icon_path)
                # For cross-platform
                icon_img = tk.PhotoImage(file=icon_path)
                self.root.tk.call('wm', 'iconphoto', self.root._w, icon_img)
        except Exception as e:
            print(f"Icon error: {e}")
        
        # Configure main window
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(0, weight=1)
        
        # Create scrollable canvas
        self.create_scrollable_canvas()
        
        # Create frames for each section inside the canvas
        self.create_database_frame()
        self.create_crawl_frame()
        self.create_scrape_frame()
        self.create_output_frame()
        
        # Update the canvas scroll region
        self.update_scroll_region()
        
        print("ScraperUI initialization complete")
        
        # Ensure window doesn't close immediately
        self.root.protocol("WM_DELETE_WINDOW", self.on_closing)
    
    def on_closing(self):
        """Handle window closing"""
        print("Closing application...")
        # Close database connection if open
        if self.conn:
            self.conn.close()
            print("Database connection closed")
        self.root.destroy()
    
    def create_scrollable_canvas(self):
        """Create a scrollable canvas for the main content"""
        print("Creating scrollable canvas...")
        # Create a frame with scrollbar
        self.main_frame = ttk.Frame(self.root)
        self.main_frame.grid(row=0, column=0, sticky="nsew")
        self.main_frame.columnconfigure(0, weight=1)
        self.main_frame.rowconfigure(0, weight=1)
        
        # Create a canvas with scrollbar
        self.canvas = tk.Canvas(self.main_frame, borderwidth=0, highlightthickness=0)
        self.scrollbar = ttk.Scrollbar(self.main_frame, orient="vertical", command=self.canvas.yview)
        self.canvas.configure(yscrollcommand=self.scrollbar.set)
        
        # Place canvas and scrollbar in grid
        self.canvas.grid(row=0, column=0, sticky="nsew")
        self.scrollbar.grid(row=0, column=1, sticky="ns")
        
        # Create a frame inside the canvas to hold the content
        self.content_frame = ttk.Frame(self.canvas)
        self.canvas_window = self.canvas.create_window((0, 0), window=self.content_frame, anchor="nw")
        
        # Configure the content frame and canvas
        self.content_frame.columnconfigure(0, weight=1)
        self.content_frame.bind("<Configure>", self.update_scroll_region)
        self.canvas.bind("<Configure>", self.on_canvas_configure)
        
        # Bind scroll events for mouse wheel
        try:
            # Windows mouse wheel binding
            self.canvas.bind_all("<MouseWheel>", self.on_mousewheel_windows)
            # Linux mouse wheel binding
            self.canvas.bind_all("<Button-4>", self.on_mousewheel_linux)
            self.canvas.bind_all("<Button-5>", self.on_mousewheel_linux)
            print("Mouse wheel bindings set up")
        except Exception as e:
            print(f"Error setting up mouse wheel bindings: {e}")
    
    def on_canvas_configure(self, event):
        """Update the canvas width when the window is resized"""
        canvas_width = event.width
        self.canvas.itemconfig(self.canvas_window, width=canvas_width)
    
    def update_scroll_region(self, event=None):
        """Update the canvas scroll region to encompass all content"""
        self.canvas.configure(scrollregion=self.canvas.bbox("all"))
    
    def on_mousewheel_windows(self, event):
        """Handle mouse wheel scrolling for Windows"""
        try:
            self.canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")
        except Exception as e:
            print(f"Windows scrolling error: {e}")
    
    def on_mousewheel_linux(self, event):
        """Handle mouse wheel scrolling for Linux"""
        try:
            if event.num == 4:  # Scroll up
                self.canvas.yview_scroll(-1, "units")
            elif event.num == 5:  # Scroll down
                self.canvas.yview_scroll(1, "units")
        except Exception as e:
            print(f"Linux scrolling error: {e}")
    
    def connect_to_database(self):
        """Connect to the racing_data.db database"""
        try:
            # Close existing connection if any
            if self.conn:
                self.conn.close()
                self.conn = None
            
            # Connect to the database
            self.conn = sqlite3.connect('racing_data.db')
            self.log("Successfully connected to racing_data.db")
            self.connection_status_var.set("Connected")
            
            # Update the stats
            self.get_database_stats()
            
        except Exception as e:
            self.log(f"Error connecting to database: {e}")
            self.connection_status_var.set("Connection Failed")
            messagebox.showerror("Database Connection Error", f"Could not connect to racing_data.db: {e}")
    
    def get_database_stats(self):
        """Get statistics from the database and update the table"""
        if not self.conn:
            self.log("Please connect to the database first")
            messagebox.showinfo("Not Connected", "Please connect to the database first.")
            return
        
        try:
            # Check if the urls table exists
            cursor = self.conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='urls'")
            if not cursor.fetchone():
                self.log("The urls table does not exist in the database")
                messagebox.showinfo("Table Missing", "The urls table does not exist in the database.")
                return
            
            # Initialize the data dictionary
            stats_data = {
                'Type': ['Races', 'Jockeys', 'Trainers', 'Horses', 'Total'],
                'Unprocessed': [0, 0, 0, 0, 0],
                'Failed': [0, 0, 0, 0, 0],
                'Succeeded': [0, 0, 0, 0, 0],
                'Total': [0, 0, 0, 0, 0]
            }
            
            # Query for the counts by type and status
            types = ['races', 'jockeys', 'trainers', 'horses']
            statuses = ['unprocessed', 'error', 'processed']
            status_mapping = {'unprocessed': 'Unprocessed', 'error': 'Failed', 'processed': 'Succeeded'}
            
            # Get counts for each type and status
            for i, type_name in enumerate(types):
                for status in statuses:
                    cursor.execute(f"SELECT COUNT(*) FROM urls WHERE Type=? AND status=?", (type_name, status))
                    count = cursor.fetchone()[0]
                    
                    # Update the stats data
                    stats_data[status_mapping[status]][i] = count
                    
                    # Add to the total row
                    stats_data[status_mapping[status]][4] += count
                
                # Calculate the total for this type
                stats_data['Total'][i] = sum(stats_data[col][i] for col in ['Unprocessed', 'Failed', 'Succeeded'])
            
            # Calculate total of totals
            stats_data['Total'][4] = sum(stats_data['Total'][0:4])
            
            # Create a DataFrame for easier handling
            df = pd.DataFrame(stats_data)
            
            # Update the treeview
            self.update_stats_treeview(df)
            
            self.log("Database stats updated successfully")
            
        except Exception as e:
            self.log(f"Error getting database stats: {e}")
            messagebox.showerror("Database Error", f"Error getting stats: {e}")
    
    def update_stats_treeview(self, df):
        """Update the treeview with the stats data"""
        # Clear existing items
        for item in self.stats_tree.get_children():
            self.stats_tree.delete(item)
        
        # Add the new data
        for i in range(len(df)):
            values = [df.iloc[i, col] for col in range(len(df.columns))]
            self.stats_tree.insert("", "end", values=values)
    
    def create_database_frame(self):
        """Create the database connection section"""
        print("Creating database frame...")
        self.db_frame = ttk.LabelFrame(self.content_frame, text="Database Connection")
        self.db_frame.grid(row=0, column=0, padx=5, pady=(0, 2), sticky="ew")
        
        # Make the frame expand horizontally
        self.db_frame.columnconfigure(0, weight=1)
        
        # Connection status
        status_frame = ttk.Frame(self.db_frame)
        status_frame.pack(fill="x", padx=5, pady=5)
        
        ttk.Label(status_frame, text="Status:").pack(side="left", padx=(0, 5))
        self.connection_status_var = tk.StringVar(value="Not Connected")
        ttk.Label(status_frame, textvariable=self.connection_status_var).pack(side="left")
        
        # Connect button
        connect_button = ttk.Button(self.db_frame, text="Connect to database", command=self.connect_to_database)
        connect_button.pack(fill="x", padx=5, pady=5)
        
        # Get stats button
        stats_button = ttk.Button(self.db_frame, text="Get stats", command=self.get_database_stats)
        stats_button.pack(fill="x", padx=5, pady=5)
        
        # Stats table frame - using pack with expand=False to make it fit the content's height
        table_frame = ttk.Frame(self.db_frame)
        table_frame.pack(fill="x", expand=False, padx=5, pady=5)
        
        # Create the treeview (table) with scrollbars
        # Set a fixed height for 5 rows (one for each type + header)
        self.stats_tree = ttk.Treeview(table_frame, columns=("Type", "Unprocessed", "Failed", "Succeeded", "Total"), height=5)
        
        # Configure columns
        self.stats_tree.column("#0", width=0, stretch=tk.NO)  # Hide the first column
        self.stats_tree.column("Type", width=100, anchor="center")  # Center alignment for Type column
        self.stats_tree.column("Unprocessed", width=100, anchor="center")
        self.stats_tree.column("Failed", width=100, anchor="center")
        self.stats_tree.column("Succeeded", width=100, anchor="center")
        self.stats_tree.column("Total", width=100, anchor="center")
        
        # Configure headings
        self.stats_tree.heading("#0", text="")
        self.stats_tree.heading("Type", text="Type")
        self.stats_tree.heading("Unprocessed", text="Unprocessed")
        self.stats_tree.heading("Failed", text="Failed")
        self.stats_tree.heading("Succeeded", text="Succeeded")
        self.stats_tree.heading("Total", text="Total")
        
        # Add a scrollbar
        tree_scrollbar = ttk.Scrollbar(table_frame, orient="vertical", command=self.stats_tree.yview)
        self.stats_tree.configure(yscrollcommand=tree_scrollbar.set)
        
        # Pack the treeview and scrollbar
        self.stats_tree.pack(side="left", fill="x", expand=True)
        tree_scrollbar.pack(side="right", fill="y")
        
        # Add some initial data rows with capitalized type values
        initial_data = [
            ("Races", 0, 0, 0, 0),
            ("Jockeys", 0, 0, 0, 0),
            ("Trainers", 0, 0, 0, 0),
            ("Horses", 0, 0, 0, 0),
            ("Total", 0, 0, 0, 0)
        ]
        
        for row in initial_data:
            self.stats_tree.insert("", "end", values=row)
    
    def create_crawl_frame(self):
        """Create the crawl section"""
        print("Creating crawl frame...")
        self.crawl_frame = ttk.LabelFrame(self.content_frame, text="Crawl")
        self.crawl_frame.grid(row=1, column=0, padx=5, pady=2, sticky="ew")
        
        # Make the frame expand horizontally
        self.crawl_frame.columnconfigure(0, weight=1)
        
        # Base URL field
        base_url_frame = ttk.Frame(self.crawl_frame)
        base_url_frame.pack(fill="x", padx=5, pady=5)
        ttk.Label(base_url_frame, text="Base URL:").pack(anchor="w")
        self.base_url_var = tk.StringVar(value="https://www.sportinglife.com/racing/results/")
        ttk.Entry(base_url_frame, textvariable=self.base_url_var).pack(fill="x", pady=(2, 0))
        
        # Timeout field
        timeout_frame = ttk.Frame(self.crawl_frame)
        timeout_frame.pack(fill="x", padx=5, pady=5)
        ttk.Label(timeout_frame, text="Timeout (mins):").pack(anchor="w")
        self.timeout_var = tk.StringVar(value="1")
        ttk.Entry(timeout_frame, textvariable=self.timeout_var).pack(fill="x", pady=(2, 0))
        
        # Max URLs field
        max_urls_frame = ttk.Frame(self.crawl_frame)
        max_urls_frame.pack(fill="x", padx=5, pady=5)
        ttk.Label(max_urls_frame, text="Max URLs:").pack(anchor="w")
        self.max_urls_var = tk.StringVar(value="100")
        ttk.Entry(max_urls_frame, textvariable=self.max_urls_var).pack(fill="x", pady=(2, 0))
        
        # Saturation limit field
        saturation_frame = ttk.Frame(self.crawl_frame)
        saturation_frame.pack(fill="x", padx=5, pady=5)
        ttk.Label(saturation_frame, text="Saturation limit (%):").pack(anchor="w")
        self.saturation_var = tk.StringVar(value="5")
        ttk.Entry(saturation_frame, textvariable=self.saturation_var).pack(fill="x", pady=(2, 0))
        
        # Crawl button
        crawl_button = ttk.Button(self.crawl_frame, text="Crawl")
        crawl_button.pack(fill="x", padx=5, pady=5)
    
    def create_scrape_frame(self):
        """Create the scrape section"""
        print("Creating scrape frame...")
        self.scrape_frame = ttk.LabelFrame(self.content_frame, text="Scrape")
        self.scrape_frame.grid(row=2, column=0, padx=5, pady=2, sticky="ew")
        
        # Make the frame expand horizontally
        self.scrape_frame.columnconfigure(0, weight=1)
        
        # Add a placeholder widget to make the frame visible
        ttk.Label(self.scrape_frame, text="Scrape section").pack(pady=5)
    
    def create_output_frame(self):
        """Create the output section"""
        print("Creating output frame...")
        self.output_frame = ttk.LabelFrame(self.content_frame, text="Output")
        self.output_frame.grid(row=3, column=0, padx=5, pady=(2, 0), sticky="ew")
        
        # Make the frame expand horizontally
        self.output_frame.columnconfigure(0, weight=1)
        self.output_frame.rowconfigure(0, weight=1)
        
        # Add a text area for output
        self.output_text = scrolledtext.ScrolledText(self.output_frame, height=10)
        self.output_text.grid(row=0, column=0, sticky="ew", padx=5, pady=5)
        self.output_text.insert(tk.END, "Output will appear here...")
    
    def log(self, message):
        """Log a message to the output text area"""
        self.output_text.insert(tk.END, f"\n{message}")
        self.output_text.see(tk.END)

def main():
    """Main function to run the application"""
    print(f"Python version: {sys.version}")
    print(f"Tkinter version: {tk.TkVersion}")
    
    root = tk.Tk()
    app = ScraperUI(root)
    print("Starting main loop...")
    
    # This will block until the window is closed
    root.mainloop()
    
    print("Main loop exited.")

# Make sure this block executes properly
if __name__ == "__main__":
    print("Initializing Scraper application...")
    try:
        main()
    except Exception as e:
        print(f"Error initializing application: {e}") 