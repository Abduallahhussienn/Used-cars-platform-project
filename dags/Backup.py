import os
import tkinter as tk
from tkinter import ttk
from tkinter import messagebox
from google.cloud import bigquery
import pandas as pd
import joblib

# Load the model from the file 
loaded_model = joblib.load('model.pkl') 

# Set environment variable for Google Application Credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "D:\\Mine\\Ready 2024\\Final_Project\\used_cars_platform-ready-project\\dags\\service-account-file.json"

# Initialize BigQuery client
client = bigquery.Client()
table_id = 'ready-data-de24.Landing_ahussien.cars_data'  # Update with your project, dataset, and table name

# Define the labels
labels = [
    "Brand", "Model", "Year", "Mileage", "Engine", "Engine Size",
    "Transmission", "Automatic Transmission", "Fuel Type", "Drivetrain",
    "Min MPG", "Max MPG", "Damaged", "First Owner", "Personal Using",
    "Turbo", "Alloy Wheels", "Adaptive Cruise Control", "Navigation System",
    "Power Liftgate", "Backup Camera", "Keyless Start", "Remote Start",
    "Sunroof_or_Moonroof", "Automatic Emergency Braking", "Stability Control",
    "Leather Seats", "Memory Seat", "Third Row Seating", "Apple Car Play_or_Android Auto",
    "Bluetooth", "USB Port", "Heated Seats", "Interior Color", "Exterior Color", "Price"
]

textbox_labels = [
    "Brand", "Model", "Year", "Mileage", "Engine", "Engine Size",
    "Transmission", "Fuel Type", "Drivetrain", "Interior Color", "Exterior Color",
    "Min MPG", "Max MPG", "Price"
]

# Create fields dynamically
entries = {}
dropdown_values = ["0.0", "1.0"]

# Create the main window
root = tk.Tk()
root.title("Car Details Entry")
root.geometry("800x800")

# Create a frame for better organization
frame = tk.Frame(root)
frame.pack(pady=100)

# Title
title = tk.Label(root, text="WELCOME", font=("Algerian", 35, "bold"), foreground="#8b0000")
title.pack()
title.place(x=300, y=20)

# Create labels and input fields dynamically
for i, label in enumerate(labels):
    if i%2 == 0:
        tk.Label(frame, text=label, anchor='w').grid(row=i, column=0, padx=10, pady=5, sticky='w')
        
        if label in textbox_labels:
            entry = tk.Entry(frame)
        else:
            entry = ttk.Combobox(frame, values=dropdown_values)
            entry['state'] = 'readonly'
        
        entry.grid(row=i, column=1, padx=10, pady=5)
        entries[label] = entry  # Store entry widgets in a dictionary
    else:
        tk.Label(frame, text=label, anchor='w').grid(row=i-1, column=3, padx=10, pady=5, sticky='w')
        
        if label in textbox_labels:
            entry = tk.Entry(frame)
        else:
            entry = ttk.Combobox(frame, values=dropdown_values)
            entry['state'] = 'readonly'
        
        entry.grid(row=i-1, column=4, padx=10, pady=5)
        entries[label] = entry  # Store entry widgets in a dictionary

# Function to append data to BigQuery
def append_to_bigquery(df):
    try:
        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField(label.lower().replace(" ", "_"), bigquery.enums.SqlTypeNames.STRING) for label in labels
            ],
            write_disposition="WRITE_APPEND",
        )
        
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()  # Wait for the job to complete

        table = client.get_table(table_id)
        print(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")
        return True
    except Exception as e:
        print(f"Error appending to BigQuery: {e}")
        return False

def submit_data():
    new_record = {label.lower().replace(" ", "_"): entries[label].get() for label in labels}
    new_data_df = pd.DataFrame([new_record])
    
    if append_to_bigquery(new_data_df):
        messagebox.showinfo("Success", "Data appended successfully!")
        clear_fields()
    else:
        messagebox.showerror("Error", "Failed to append data to BigQuery.")

def clear_fields():
    for label in labels:
        entries[label].delete(0, tk.END) if isinstance(entries[label], tk.Entry) else entries[label].set(dropdown_values[0])

# Submit button
submit_btn = tk.Button(root, text="Submit", width=30, height=2, bg="#8b0000", fg="white", command=submit_data)
submit_btn.pack()
submit_btn.place(x=300, y=680)

# Run the Tkinter event loop
root.mainloop()