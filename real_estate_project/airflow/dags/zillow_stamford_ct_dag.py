import os
import logging
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import pyarrow.csv as pv
import pyarrow.parquet as pq
from google.cloud import storage
from datetime import datetime
from pathlib import Path
import requests
import json
import pandas as pd
from datetime import date
import time

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

today = date.today()
LOCAL_OUTPUT_PATH = AIRFLOW_HOME + "/stamford_ct_zillow_" + str(today) + ".csv"
GCS_PATH = "real_estate/stamford_ct_zillow_" + str(today) + ".csv"

stamford_ct_params = dict(
    city="Stamford",
    state="CT",
    west="-73.67487426538085",
    east="-73.45411773461913",
    south="40.9626044146077",
    north="41.180536765064744",
    region_id="27239",
    region_type="6",
)


def upload_to_gcs(bucket, object_name, local_file):
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround
    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


def search_zillow(params):
    search = Search(**params)
    print(f"Passing {LOCAL_OUTPUT_PATH}")
    search.get_results(LOCAL_OUTPUT_PATH)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "retries": 1,
}

## Stanform CT
with DAG(
    dag_id="zillow_stamford_ct_dag",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=["dtc-de"],
) as dag:

    get_data_task = PythonOperator(
        task_id="get_data_task",
        python_callable=search_zillow,
        op_kwargs={
            "params": stamford_ct_params,
        },
    )

    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": GCS_PATH,
            "local_file": LOCAL_OUTPUT_PATH,
        },
    )

    rm_task = BashOperator(task_id="rm_task", bash_command=f"rm {LOCAL_OUTPUT_PATH}")

    get_data_task >> local_to_gcs_task >> rm_task

## Scraper
headers = {
    "accept": "*/*",
    "accept-encoding": "gzip, deflate, br",
    "accept-language": "en-US,en;q=0.8",
    "cache-control": "no-cache",
    "cookie": 'x-amz-continuous-deployment-state=AYABeA4KyGb4zEJ3XKUdfAAevmUAPgACAAFEAB1kM2Jsa2Q0azB3azlvai5jbG91ZGZyb250Lm5ldAABRwAVRzA3NjA0OTUxU1JKRU1BTUNBQVQzAAEAAkNEABpDb29raWUAAACAAAAADD3Gg9cJm66IrD9kzgAwmqZEMHDLaKsJKEILJzWupQpktPHJkXlEVC1NKcxHFBHLTiVvkm7igjh2DiudDaEYAgAAAAAMAAQAAAAAAAAAAAAAAAAAADEvN8IJC8dNWdBeqG5ZL4%2F%2F%2F%2F%2F%2FAAAAAQAAAAAAAAAAAAAAAQAAAAw3K3sdb4GQsHwvItavm4lpciyjDEl12n2INhY9; zguid=24|%24da400462-e207-413d-818c-d38b78154ad7; zgsession=1|b85c8191-470c-46e7-9d25-b418c919df0d; pxcts=84a9259e-86c5-11ed-ba57-55664f4e4d66; _pxvid=84a91879-86c5-11ed-ba57-55664f4e4d66; g_state={"i_p":1672249213433,"i_l":1}; JSESSIONID=E80C8556B79F00C267C0AA7321EF3BD0; x-amz-continuous-deployment-state=AYABeM0nQJSoJTu%2FyxyewDbLvyoAPgACAAFEAB1kM2Jsa2Q0azB3azlvai5jbG91ZGZyb250Lm5ldAABRwAVRzA3NjA0OTUxU1JKRU1BTUNBQVQzAAEAAkNEABpDb29raWUAAACAAAAADEx%2FBFrQM0ZmiRpkXgAw4tMng0d8wdCLX%2FZKp9PQBsR1yOZQqV+uuMkGi232e6BWHm8JlKi74VyEoAZqqHaPAgAAAAAMAAQAAAAAAAAAAAAAAAAAAFEUqzXM1CqgGZIJpCgl%2FL%2F%2F%2F%2F%2F%2FAAAAAQAAAAAAAAAAAAAAAQAAAAyRBWBa0221DO3xuj4kUVk+UMT4HFcCN844nXiOUVk+UMT4HFcCN844nXiOUVk+UMT4HFcCN844nXiO; _pxff_cc=U2FtZVNpdGU9TGF4Ow==; _pxff_bsco=1; _px3=6a882be5b329512d98a4781079f11717b7195dc28935defee582a7be979e1a48:tzb+eJJBnJz+KOMrShs2g4OnlHhDfs0X5wg6sSNSjs4Iypr6J+7qYh93yXao/eJIJ5zw6AvrVy/2yyILlCMZBQ==:1000:AEKNgUWslz+DRb1AyjKpG81vu9ImJ6dbvvGF38wtWBLzdk7t9t0SdzxfX6PkE1REC/Mn1ryIRHzZsQYZnaEgHQM9g1qLS7HzZljO39OQxlbzimJL//iV1QYPQ+QOWTVNDiMyzy22NW8s2B5CWAkK8g1RljW9QaxZLuugqz56rT6Es8INRqrzberjzWzuxv6MZZSTlNGJkuazoOQSKJmoOA==; search=6|1674847113635%7Crect%3D26.50555956808795%252C-79.98077468896484%252C26.246575883089772%252C-80.38246231103516%26rid%3D10477%26disp%3Dmap%26mdm%3Dauto%26p%3D1%26z%3D1%26fs%3D0%26fr%3D1%26mmm%3D0%26rs%3D0%26ah%3D0%26singlestory%3D0%26housing-connector%3D0%26abo%3D0%26garage%3D0%26pool%3D0%26ac%3D0%26waterfront%3D0%26finished%3D0%26unfinished%3D0%26cityview%3D0%26mountainview%3D0%26parkview%3D0%26waterview%3D0%26hoadata%3D1%26zillow-owned%3D0%263dhome%3D0%26featuredMultiFamilyBuilding%3D0%26excludeNullAvailabilityDates%3D0%26commuteMode%3Ddriving%26commuteTimeOfDay%3Dnow%09%0910477%09%09%09%09%09%09; AWSALB=J4DWC7ePccthKXnOBtVsFRqpUby7sRpDk6tTawHhWtnMQ1m+lkkfzzmJm01nLtqNEi7o1QZ8PIN6CWAx7h994nf+6SZeL5eVvlWqQS6KZROGOFc/vZMz7Tm9gM8y; AWSALBCORS=J4DWC7ePccthKXnOBtVsFRqpUby7sRpDk6tTawHhWtnMQ1m+lkkfzzmJm01nLtqNEi7o1QZ8PIN6CWAx7h994nf+6SZeL5eVvlWqQS6KZROGOFc/vZMz7Tm9gM8y',
    "pragma": "no-cache",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "sec-gpc": "1",
    "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Safari/537.36",
}


class Search:
    def __init__(self, city, state, west, east, south, north, region_id, region_type):
        self.current_page = 1
        self.city = city
        self.state = state
        self.total_pages = 1
        self.west = west
        self.east = east
        self.south = south
        self.north = north
        self.region_id = region_id
        self.region_type = region_type

    def build_filename(self):
        # fl-boca-raton-2023-01-09.csv
        city = str.replace(self.city, " ", "-")
        return f"{str.lower(self.state)}-{str.lower(city)}-{date.today()}.csv"

    def build_url(self):
        return (
            'https://www.zillow.com/search/GetSearchPageState.htm?searchQueryState={"pagination":{"currentPage":'
            + str(self.current_page)
            + '},"usersSearchTerm":"'
            + self.city
            + ","
            + self.state
            + '","mapBounds":{"west":'
            + self.west
            + ',"east":'
            + self.east
            + ',"south":'
            + self.south
            + ',"north":'
            + self.north
            + '},"regionSelection":[{"regionId":'
            + self.region_id
            + ',"regionType": '
            + self.region_type
            + '}],"isMapVisible":true,"filterState":{"sortSelection":{"value":"globalrelevanceex"},"isAllHomes":{"value":true}},"isListVisible":true}&wants={"cat1":["listResults", "mapResults"],"cat2":["total"]}&requestId=4'
        )

    def get_properties(self):
        url = self.build_url()
        response = requests.get(url, headers=headers)
        print(f"Reaching... {url}")
        print(response.status_code)
        data = json.loads(response.text)
        properties = data["cat1"]["searchResults"]["listResults"]
        self.total_pages = data["cat1"]["searchList"]["totalPages"]
        print(f"Total pages for this search: {self.total_pages}")
        return properties

    def get_property(self, property, page):
        fields = ["zpid", "price", "beds", "baths", "area", "latLong", "statusType"]
        data = {}
        for field in fields:
            try:
                value = property.get(field)
                if field == "latLong":
                    data["latitude"] = value.get("latitude")
                    data["longitude"] = value.get("longitude")
                else:
                    data[field] = value
            except:
                data[field] = None
                print("Error to read field " + field)

        data["city"] = self.city
        data["state"] = self.state
        data["page"] = page

        return data

    def get_results(self, output_path):
        results = []

        while self.current_page <= self.total_pages:
            properties = self.get_properties()

            for prop in properties:
                property = self.get_property(prop, self.current_page)
                results.append(property)

            self.current_page += 1
            time.sleep(3)

        self.save_csv(results, output_path)

    def save_csv(self, data, output_path):
        df = pd.read_json(json.dumps(data))
        print(df)
        print(f"Saving to {output_path}")
        df.to_csv(output_path)
