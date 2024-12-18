import json 
import pathlib
import airflow  

import requests 
import requests.exceptions as request_exceptions
from airflow import DAG 

from airflow.operators.bash import BashOperator 
from airflow.operators.python import PythonOperator 

dag = DAG(
    dag_id = "download_rocket_launch",
    start_date = airflow.utils.dates.days_ago(14),
    schedule_interval = None,
)

launch_files_download = BashOperator(
    task_id = "launch_files_download",
    bash_command = "curl -o /tmp/launches.json -L 'https://ll.thespacedevs.com/2.0.0/launch/upcoming'",
    dag = dag,
)

def _get_images():
    # check if the json response directory exists or not
    pathlib.Path("/tmp/images").mkdir(parents=True, exist_ok=True)

    #Download all pictures in launches.json
    with open("/tmp/launches.json") as f:
        launches = json.load(f)
        image_urls = [launch["image"] for launch in launches["results"]]
        for image_url in image_urls:
            try:
                response = requests.get(image_url)
                image_filename = image_url.split("/")[-1]
                target_file = f"/tmp/images/{image_filename}"
                with open(target_file, "wb") as f:
                    f.write(response.content)
                print(f"Downloaded {image_url} to {target_file}")

            except request_exceptions.MissingSchema:
                print(f"{image_url} appears to be an invalid URL")

            except request_exceptions.ConnectionError:
                print(f"Could not connect to {image_url}")


get_images = PythonOperator(
    task_id = "get_images",
    python_callable= _get_images,
    dag = dag
)

notify = BashOperator(
    task_id="notify",
    bash_command='echo"There are now $(ls /tmp/images/ | wd -l) images."',
    dag = dag
)

launch_files_download >> get_images >> notify
