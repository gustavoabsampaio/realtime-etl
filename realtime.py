import requests
import pandas as pd
from prefect import flow, task
from datetime import datetime
from pytz import timezone

def extract():
    print("EXTRACT START")

    base_url = "https://api.data.gov.sg/v1/environment/"

    airtemp_url = base_url + 'air-temperature'

    rainfall_url = base_url + 'rainfall'

    raw_datetime = datetime.now(timezone('Asia/Singapore'))
    date_time = raw_datetime.strftime('%Y-%m-%dT%H:%M:%S')
    date = raw_datetime.strftime('%Y-%m-%d')

    # request query parameters
    params = {
        "date_time": date_time,
        "date": date
    }

    # request headers
    headers = {
        "Content-Type": "application/json"
    }

    # Send the GET request
    try:
        response_data_airtemp = requests.get(airtemp_url, params=params, headers=headers)
        print(f"Response status: {response_data_airtemp.status_code}")
        print(f"Response content: {response_data_airtemp.content}")
    except requests.exceptions.RequestException as e:
        print(f"ERROR on the airtemp GET request: {e}")
        response_data_airtemp = None
        raise ValueError("ERROR on the airtemp GET request ", e)

    try:
        response_data_rainfall = requests.get(rainfall_url, params=params, headers=headers)
        print(f"Response status: {response_data_rainfall.status_code}")
        print(f"Response content: {response_data_rainfall.content}")
    except requests.exceptions.RequestException as e:
        # print(f"ERROR on the rainfall GET request: {e}")
        response_data_rainfall = None
        raise ValueError("ERROR on the rainfall GET request ", e)
        

    print("EXTRACT END")

    response_data = [response_data_airtemp, response_data_rainfall]

    return response_data

def transform(response_data):
    if response_data == None:
        print("Empty data")
        return
    print("Data Filtering Start")
    response_data_airtemp = response_data[0].json()
    response_data_rainfall = response_data[1].json()
    

    # Create a DataFrame with the station data
    stations = pd.DataFrame(response_data_airtemp['metadata']['stations'])
    stations = stations.drop(columns=['device_id'])
    
    # Create a DataFrame with the readings data
    readings_airtemp = pd.DataFrame(response_data_airtemp['items'][0]['readings'])
    readings_rainfall = pd.DataFrame(response_data_rainfall['items'][0]['readings'])

    reading_timestamp = response_data_airtemp['items'][0]['timestamp']

    timestamp_df = pd.to_datetime(reading_timestamp)

    readings_airtemp['timestamp'] = timestamp_df

    df = pd.merge(stations, readings_airtemp, left_on='id', right_on='station_id')
    df = pd.merge(df, readings_rainfall, left_on='id', right_on='station_id', suffixes=(None, "_y"))
    df = df.drop(['id','station_id_y'], axis=1)
    df = df.rename(columns={'station_id': 'station_id', 'value_x': 'airtemp', 'value_y': 'rainfall'})
    # print(stations)
    print(df)
    print("Data Filtering End\n")
    return df

def load():
    pass

def print_readings(filtered_df):
    # Print the filtered readings
    for _, row in filtered_df.iterrows():
        print(f"Station: {row['station_id']}, Name: {row['name']}, Lat/Long: ({row['location']['latitude']}, {row['location']['longitude']}), Temperature: {row['value']}°C, Horario(SGT): {row['timestamp']}")

def main_flow():
    response_data = extract()
    response_df = transform(response_data)
    print_readings(response_df)
    
main_flow()