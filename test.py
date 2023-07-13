
"""
response_example = {
    'metadata': {
        'stations': [
            {
                'id': str, #id da estacao de clima
                'device_id': str, #id do dispositivo de medicao
                'name': str, #nome da estacao
                'location': {
                    'latitude': float,
                    'longitude': float
                }
            },
            # More station objects...
        ],
        'reading_type': str, #tipo de medicao
        'reading_unit': str  #tipo de unidade (graus C)
    },
    'items': [
        {
            'timestamp': str, #hora da medida
            'readings': [
                {
                    'station_id': str,  #id da estacao que mediu
                    'value': float      #valor medido
                },
                # More reading objects...
            ]
        }
    ],
    'api_info': {
        'status': str #status da API
    }
}

"""

import requests
import pandas as pd
from prefect import flow, task
from datetime import datetime
from pytz import timezone

# @task
def filter_readings(response_data, station_id):
    if response_data == None:
        print("Empty data")
        return
    print("Data Filtering Start")
    response_data = response_data.json()

    # Create a DataFrame from the readings
    df = pd.DataFrame(response_data['items'][0]['readings'])

    # Filter the DataFrame by station_id
    filtered_df = df[df['station_id'] == station_id]
    print("Data Filtering End\n")
    return filtered_df

# @task
def print_readings(filtered_df):
    # Print the filtered readings
    for _, row in filtered_df.iterrows():
        print(f"Station: {row['station_id']}, Temperature: {row['value']}°C")

# Define the Prefect flow
# @flow(name="Filter Readings") 
def test_flow():
    print("Flow init\n")

    # Define the task inputs
    url = "https://api.data.gov.sg/v1/environment/air-temperature"
    raw_datetime = datetime.now(timezone('Asia/Singapore'))
    date_time = raw_datetime.strftime('%Y-%m-%dT%H:%M:%S')
    date = raw_datetime.strftime('%Y-%m-%d')

    # Set the query parameters
    params = {
        "date_time": date_time,
        "date": date
    }

    # Set the headers
    headers = {
        "Content-Type": "application/json"
    }

    # Send the GET request
    try:
        response_data = requests.get(url, params=params, headers=headers)
        print(f"Response status: {response_data.status_code}")
        print(f"Response content: {response_data.content}")
    except requests.exceptions.RequestException as e:
        print(f"ERROR on the GET request: {e}")
        response = None
    print("GET done\n")

    station_id = 'S50'

    # Define the task dependencies
    filtered_readings = filter_readings(response_data, station_id)
    print_readings(filtered_readings)
    print("Flow end")

# Run the Prefect flow
test_flow()

