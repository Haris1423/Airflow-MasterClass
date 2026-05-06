from airflow.sdk import dag,task,asset
from pendulum import datetime,duration
import os
from Assets_final_15 import fetch_data
@asset(
    schedule=fetch_data,
    #this is the path where the Data will be stored, this is an optional 
    # but good to include for clarity about the assets storage location
    uri ="/opt/airflow/logs/data/data_processed.txt",
    name = "process_data"

)
def process_data(self):
    
    os.makedirs(os.path.dirname(self.uri), exist_ok=True)
    with open(self.uri, 'w') as f:
        f.write("Data processed on successfully!")
    
    print(f"Data processed and stored at {self.uri}")