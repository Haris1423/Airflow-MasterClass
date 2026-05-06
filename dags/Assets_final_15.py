from airflow.sdk import dag,task,asset
from pendulum import datetime,duration
import os


@asset(
    schedule='@daily',
    #this is the path where the Data will be stored, this is an optional 
    # but good to include for clarity about the assets storage location
    uri ="/opt/airflow/logs/data/data_fetched.txt",
    
    name = "fetch_data"

)
def fetch_data(self):
    
    os.makedirs(os.path.dirname(self.uri), exist_ok=True)
    with open(self.uri, 'w') as f:
        f.write("Data fetched on successfully!")
    
    print(f"Data fetched and stored at {self.uri}")