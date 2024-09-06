import os
import tkinter as tk
from tkinter import ttk
from tkinter import messagebox
from google.cloud import bigquery
import pandas as pd
import joblib
import numpy as np

# Load the model from the file 
loaded_model = joblib.load('model.pkl') 
loaded_scaler = joblib.load('scaler.pkl') 
loaded_encoder = joblib.load('label_encoder.pkl') 


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
input_text_features = [
    'brand','model','year','engine',"transmission","automatic_transmission",
    "fuel_type", "drivetrain","damaged", "first_owner", "personal_using",
    "turbo", "adaptive_cruise_control", "navigation_system",
    "power_liftgate", "backup_camera", "remote_start",
    "sunroof_or_moonroof",
    "leather_seats", "memory_seat", "apple_car_play_or_android_auto",
    "heated_seats","exterior_color","interior_color"
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

def predict_price():
    
    #input_data = {label.lower().replace(" ", "_"): entries[label].get() for label in input_text_features}
    # input_data = pd.DataFrame({
    #         'brand': ['FIAT'],
    #         'model': ['124 Spider Lusso'],
    #         'year': ['2020'],
    #         'mileage': ['30830'],
    #         'engine': ['1.4L I4 16V MPFI SOHC Turbo'],
    #         'engine_size': ['1.4'],
    #         'transmission': ['6-Speed Automatic'],
    #         'automatic_transmission': ['1.0'],
    #         'fuel_type': ['Gasoline'],
    #         'drivetrain': ['Rear-wheel Drive'],
    #         'max_mpg': ['36'],
    #         'damaged': ['0.0'],
    #         'first_owner': ['0.0'],
    #         'personal_using': ['1.0'],
    #         'turbo': ['1.0'],
    #         'adaptive_cruise_control': ['0.0'],
    #         'navigation_system': ['0.0'],
    #         'power_liftgate': ['0.0'],
    #         'backup_camera': ['0.0'],
    #         'remote_start': ['1.0'],
    #         'sunroof_or_moonroof': ['1.0'],
    #         'leather_seats': ['0.0'],
    #         'memory_seat': ['0.0'],
    #         'apple_car_play_or_android_auto': ['0.0'],
    #         'heated_seats': ['1.0'],
    #         'interior_color': ['Saddle'],
    #         'exterior_color': ['Forte Black Metallic']
    #     })
    input_data = pd.DataFrame({
        'brand': [entries['Brand'].get()],
        'model': [entries["Model"].get()],
        'year': [entries["Year"].get()],
        'mileage': [entries["Mileage"].get()],
        'engine': [entries["Engine"].get()],
        'engine_size': [entries["Engine Size"].get()],
        'transmission': [entries["Transmission"].get()],
        'automatic_transmission': [entries["Automatic Transmission"].get()],
        'fuel_type': [entries["Fuel Type"].get()],
        'drivetrain': [entries["Drivetrain"].get()],
        'max_mpg': [entries["Max MPG"].get()],
        'damaged': [entries["Damaged"].get()],
        'first_owner': [entries["First Owner"].get()],
        'personal_using': [entries["Personal Using"].get()],
        'turbo': [entries["Turbo"].get()],
        'adaptive_cruise_control': [entries["Adaptive Cruise Control"].get()],
        'navigation_system': [entries["Navigation System"].get()],
        'power_liftgate': [entries["Power Liftgate"].get()],
        'backup_camera': [entries["Backup Camera"].get()],
       'remote_start': [entries["Remote Start"].get()],
       'sunroof_or_moonroof': [entries["Sunroof_or_Moonroof"].get()],
       'leather_seats': [entries["Leather Seats"].get()],
        'memory_seat': [entries["Memory Seat"].get()],
        'apple_car_play_or_android_auto': [entries["Apple Car Play_or_Android Auto"].get()],
        'heated_seats': [entries["Heated Seats"].get()],
        'interior_color': [entries["Interior Color"].get()],
        'exterior_color': [entries["Exterior Color"].get()]
        })
    for i in input_text_features:
        input_data[i] = loaded_encoder[i].transform(input_data[i])
    input_data = loaded_scaler.transform(input_data)
    print(loaded_model.predict(input_data)**2)
    prediction = tk.Label(root, text=f"Recommended price :{np.float(np.round(loaded_model.predict(input_data)**2,2))}$", font=("Romain", 14, "bold"))
    prediction.pack()
    prediction.place(x=220, y=700, width=400)
# Predict button
predict_btn = tk.Button(root, text="Recommend a price", width=20, height=2, bg="#8b0000", fg="white", command=predict_price)
predict_btn.pack()
predict_btn.place(x=50, y=700)

# Submit button
submit_btn = tk.Button(root, text="Submit", width=20, height=2, bg="#8b0000", fg="white", command=submit_data)
submit_btn.pack()
submit_btn.place(x=640, y=700)
root.resizable(False, False)
# Run the Tkinter event loop
root.mainloop()