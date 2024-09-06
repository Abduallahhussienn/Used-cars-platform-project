# imports
from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.operators.python import PythonOperator
import os
# Dags initialization
dag = DAG(
    dag_id="Abduallah_from_GCStoBQ",
    description="Transfering_from_GCStoBQ",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
)

def check_data_quality(**kwargs):
    """
    Checks the data quality of the car_data.csv file.
    """
    bucket_name = 'ready-project-dataset'
    source_object = 'cars-com_dataset/cars-com_dataset.csv'
    
    # Check if the file exists in the GCS bucket
    if not os.path.exists(source_object):
        raise ValueError(f"File {source_object} not found in the GCS bucket {bucket_name}")
    
    # Check the file size
    file_size = os.path.getsize(source_object)
    if file_size == 0:
        raise ValueError(f"File {source_object} is empty")
    
    # Check the number of rows in the file
    with open(source_object, 'r') as f:
        row_count = len(f.readlines()) - 1  # Exclude the header row
    if row_count < 1:
        raise ValueError(f"File {source_object} has less than 1 row of data")
    
    print(f"Data quality check passed. File {source_object} has {row_count} rows.")

# Variables declaration
schema_fields = [
                {'name': 'brand', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'model', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'year', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'mileage', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'engine', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'engine_size', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'transmission', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'automatic_transmission', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'fuel_type', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'drivetrain', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'min_mpg', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'max_mpg', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'damaged', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'first_owner', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'personal_using', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'turbo', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'alloy_wheels', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'adaptive_cruise_control', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'navigation_system', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'power_liftgate', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'backup_camera', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'keyless_start', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'remote_start', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'sunroof_or_moonroof', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'automatic_emergency_braking', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'stability_control', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'leather_seats', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'memory_seat', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'third_row_seating', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'apple_car_play_or_android_auto', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'bluetooth', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'usb_port', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'heated_seats', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'interior_color', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'exterior_color', 'type': 'STRING', 'mode': 'NULLABLE'},
                {'name': 'price', 'type': 'STRING', 'mode': 'NULLABLE'}
            ]

source_objects_path = "cars-com_dataset/cars-com_dataset.csv"
destination_project = "ready-data-de24"
destination_dataset = "Landing_ahussien"
destination_table = "cars_data"

start_task = EmptyOperator(task_id="start_task", dag=dag)

load_csv = GCSToBigQueryOperator(
    task_id="gcs_to_bigquery", # unique identifier for the task within the Airflow DAG 
    bucket="ready-project-dataset", # the name of the Google Cloud Storage (GCS) bucket from which the data will be loaded into BigQuery.
    source_format='CSV', # defines the format of the data in the GCS bucket
    skip_leading_rows=1,  # Skip the header row
    field_delimiter=',', # the character used to separate fields in the CSV data.
    max_bad_records=100, # the maximum number of bad records that can be encountered during the data load process before the task fails
    source_objects=[source_objects_path], # the path to the file in the GCS bucket that will be loaded into BigQuery
    destination_project_dataset_table=f"{destination_project}.{destination_dataset}.{destination_table}", # the target BigQuery table where the data will be loaded
    schema_fields = schema_fields, # the schema for the target BigQuery table
    write_disposition="WRITE_TRUNCATE", # determines the behavior of the data load process
    create_disposition= "CREATE_IF_NEEDED" # the target BigQuery table should be created if it does not already exist
)
check_data_quality_task = PythonOperator(
        task_id='check_data_quality',
        python_callable=check_data_quality,
        provide_context=True,
    )
end_task = EmptyOperator(task_id="end_task", dag=dag)

start_task >> load_csv >> check_data_quality_task >> end_task