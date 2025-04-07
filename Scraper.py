import tkinter as tk
from tkinter import ttk, messagebox
import sqlite3
import os
import time

class ScraperUI:
    def __init__(self):
        self.root = tk.Tk()
        self.root.title("Racing Data Scraper")
        self.root.geometry("600x400")
        self.db_conn = None
        self.setup_ui()
        
    def setup_ui(self):
        """Set up the main UI components"""
        # Main frame
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # Database connection section
        db_frame = ttk.LabelFrame(main_frame, text="Database Connection", padding="10")
        db_frame.pack(fill=tk.X, pady=5)
        
        # Status display
        status_frame = ttk.Frame(db_frame)
        status_frame.pack(fill=tk.X, pady=5)
        
        ttk.Label(status_frame, text="Status:").pack(side=tk.LEFT, padx=5)
        self.db_status_var = tk.StringVar(value="Not Connected")
        ttk.Label(status_frame, textvariable=self.db_status_var).pack(side=tk.LEFT, padx=5)
        
        # Buttons
        button_frame = ttk.Frame(db_frame)
        button_frame.pack(fill=tk.X, pady=5)
        
        ttk.Button(button_frame, text="Connect to Database", command=self.connect_to_database).pack(side=tk.LEFT, padx=5)
        ttk.Button(button_frame, text="Refresh Data", command=self.update_url_stats).pack(side=tk.LEFT, padx=5)
        
        # URL Statistics table
        stats_frame = ttk.LabelFrame(main_frame, text="URL Statistics", padding="10")
        stats_frame.pack(fill=tk.BOTH, expand=True, pady=5)
        
        # Create a frame for the table
        self.stats_table_frame = ttk.Frame(stats_frame)
        self.stats_table_frame.pack(fill=tk.BOTH, expand=True)
        
        # Log section
        log_frame = ttk.LabelFrame(main_frame, text="Log", padding="10")
        log_frame.pack(fill=tk.X, pady=5)
        
        self.log_text = tk.Text(log_frame, height=5)
        self.log_text.pack(fill=tk.X)
        
    def connect_to_database(self):
        """Connect to the SQLite database"""
        try:
            # Check if database file exists
            if not os.path.exists('racing_data.db'):
                messagebox.showerror("Database Error", "Database file 'racing_data.db' not found.")
                return
            
            # Connect to database
            self.db_conn = sqlite3.connect('racing_data.db')
            self.db_status_var.set("Connected")
            self.log("Connected to database: racing_data.db")
            
            # Update URL stats
            self.update_url_stats()
            
        except Exception as e:
            self.db_status_var.set("Connection Failed")
            self.log(f"Database connection error: {str(e)}")
            messagebox.showerror("Database Error", f"Failed to connect to database: {str(e)}")
    
    def update_url_stats(self):
        """Update the URL statistics table"""
        if not self.db_conn:
            messagebox.showwarning("Database Warning", "Not connected to database. Please connect first.")
            return
        
        try:
            # Clear existing table
            for widget in self.stats_table_frame.winfo_children():
                widget.destroy()
            
            # Get URL statistics from database
            cursor = self.db_conn.cursor()
            
            # Get all URL types
            cursor.execute("SELECT DISTINCT Type FROM urls")
            url_types = [row[0] for row in cursor.fetchall()]
            
            # Get all URL statuses
            cursor.execute("SELECT DISTINCT status FROM urls")
            url_statuses = [row[0] for row in cursor.fetchall()]
            
            # Create table headers
            ttk.Label(self.stats_table_frame, text="Type", font=("", 9, "bold")).grid(row=0, column=0, padx=5, pady=2, sticky="w")
            
            for col, status in enumerate(url_statuses, 1):
                ttk.Label(self.stats_table_frame, text=status, font=("", 9, "bold")).grid(row=0, column=col, padx=5, pady=2, sticky="w")
            
            # Add total column
            ttk.Label(self.stats_table_frame, text="Total", font=("", 9, "bold")).grid(row=0, column=len(url_statuses) + 1, padx=5, pady=2, sticky="w")
            
            # Populate table with data
            for row, url_type in enumerate(url_types, 1):
                ttk.Label(self.stats_table_frame, text=url_type).grid(row=row, column=0, padx=5, pady=2, sticky="w")
                
                row_total = 0
                
                for col, status in enumerate(url_statuses, 1):
                    cursor.execute("SELECT COUNT(*) FROM urls WHERE Type = ? AND status = ?", (url_type, status))
                    count = cursor.fetchone()[0]
                    ttk.Label(self.stats_table_frame, text=str(count)).grid(row=row, column=col, padx=5, pady=2, sticky="w")
                    row_total += count
                
                # Add row total
                ttk.Label(self.stats_table_frame, text=str(row_total)).grid(row=row, column=len(url_statuses) + 1, padx=5, pady=2, sticky="w")
            
            # Add column totals
            ttk.Label(self.stats_table_frame, text="Total", font=("", 9, "bold")).grid(row=len(url_types) + 1, column=0, padx=5, pady=2, sticky="w")
            
            for col, status in enumerate(url_statuses, 1):
                cursor.execute("SELECT COUNT(*) FROM urls WHERE status = ?", (status,))
                count = cursor.fetchone()[0]
                ttk.Label(self.stats_table_frame, text=str(count), font=("", 9, "bold")).grid(row=len(url_types) + 1, column=col, padx=5, pady=2, sticky="w")
            
            # Add grand total
            cursor.execute("SELECT COUNT(*) FROM urls")
            grand_total = cursor.fetchone()[0]
            ttk.Label(self.stats_table_frame, text=str(grand_total), font=("", 9, "bold")).grid(row=len(url_types) + 1, column=len(url_statuses) + 1, padx=5, pady=2, sticky="w")
            
            self.log("URL statistics updated")
            
        except Exception as e:
            self.log(f"Error updating URL statistics: {str(e)}")
            messagebox.showerror("Database Error", f"Failed to update URL statistics: {str(e)}")
    
    def log(self, message):
        """Add a message to the log"""
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
        self.log_text.insert(tk.END, f"[{timestamp}] {message}\n")
        self.log_text.see(tk.END)
        
    def run(self):
        """Start the UI"""
        self.root.mainloop()

if __name__ == "__main__":
    app = ScraperUI()
    app.run()
