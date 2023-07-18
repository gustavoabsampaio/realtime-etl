import requests
import pandas as pd

from datetime import datetime
from prefect import flow, task
from pytz import timezone
import psycopg2
from sqlalchemy import create_engine
from time import sleep

@task
def extract():
    print("\nEXTRACT START")

    # request urls
    base_url = "https://api.data.gov.sg/v1/environment/"

    airtemp_url = base_url + 'air-temperature'

    rainfall_url = base_url + 'rainfall'

    # request datetime and date
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

    # Send the GET requests
    try:
        response_data_airtemp = requests.get(airtemp_url, params=params, headers=headers)
    except requests.exceptions.RequestException as e:
        response_data_airtemp = None
        raise ValueError("ERROR on the airtemp GET request: ", e)

    try:
        response_data_rainfall = requests.get(rainfall_url, params=params, headers=headers)
    except requests.exceptions.RequestException as e:
        response_data_rainfall = None
        raise ValueError("ERROR on the rainfall GET request: ", e)

    print("EXTRACT END\n")

    response_data = [response_data_airtemp, response_data_rainfall]

    return response_data

@task
def transform(response_data):    
    print("\nDATA FILTERING START")

    if error_sim % 10 == 0:
        raise ValueError("TEST ERROR")

    if response_data == None:
        raise ValueError("No data")

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

    # Merge stations and readings dataframes
    df = pd.merge(stations, readings_airtemp, left_on='id', right_on='station_id')
    df = pd.merge(df, readings_rainfall, left_on='id', right_on='station_id', suffixes=(None, "_y"))

    # Drop unnecessary columns and rename columns
    df = df.drop(['id','station_id_y'], axis=1)
    df = df.rename(columns={'station_id': 'station_id', 'value': 'temperature', 'value_y': 'rainfall', 'timestamp': 'measurement_timestamp', 'name': 'station_name'})

    df['latitude'] = df['location'].apply(lambda x: x['latitude'])
    df['longitude'] = df['location'].apply(lambda x: x['longitude'])

    # Drop the original 'location' column
    df.drop('location', axis=1, inplace=True)

    # print(df)

    print("DATA FILTERING END\n")
    return df

@task
def load(transformed_data):
    print("\nLOAD START")
    try:

        # connect to the database
        engine = create_engine('postgresql://gustavo:postgres@localhost:5432/gustavo')

        table = 'weather_data'
        # convert Dataframe to psql and append to table
        transformed_data.to_sql(table, engine, if_exists='append', index=False, method='multi')

    except Exception as e:
        e = str(e).split("\"")
        if e[1] == "unique_measurements_constraint":
            print("No new measurements, skipping insertion")
            print("LOAD SKIP\n")
            pass
        else:
            raise("Data insertion failed: ", e)
    print("LOAD END\n")
    
def print_readings(filtered_df):
    # Print the filtered readings
    for _, row in filtered_df.iterrows():
        print(f"Station: {row['station_id']}, Name: {row['name']}, Lat/Long: ({row['location']['latitude']}, {row['location']['longitude']}), Temperature: {row['value']}°C, Horario(SGT): {row['timestamp']}")

@flow
def main_flow():
    response_data = extract()
    transformed_data = transform(response_data)
    load(transformed_data)
    # print_readings(transformed_data)
    

# every 10 runs there will be an exception to test the exception handling and pipeline consistency
error_sim = 1

while True:
    # main flow
    # return state must be true to keep the pipeline running in case of exceptions
    main_flow(return_state=True)
    error_sim += 1
    sleep(60)

# main_flow()