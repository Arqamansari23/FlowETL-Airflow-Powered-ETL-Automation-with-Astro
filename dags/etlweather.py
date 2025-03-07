from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook   # Http hook for reading data From Api 
from airflow.providers.postgres.hooks.postgres import PostgresHook   # PostgresHook for pushing data after Transformation into Postgres data base 
from airflow.decorators import task   # Creating Diffrent task in Dag 
from airflow.utils.dates import days_ago
import requests
import json


# Latitude and longitude for the desired location (London in this case)
LATITUDE = '51.5074'
LONGITUDE = '-0.1278'
POSTGRES_CONN_ID='postgres_default'
API_CONN_ID='open_meteo_api'   # using open meteo api for getting the wether data 


# defining Default arguments
default_args={
    'owner':'airflow',
    'start_date':days_ago(1)
}

## Defining DAG
with DAG(dag_id='weather_etl_pipeline',  # pipeline name 
         default_args=default_args,
         schedule_interval='@daily',  # shedule daily 
         catchup=False) as dags:


    # Defining tasks. task 1 is to Extract the data from Api
    @task()

    def extract_weather_data():
        """Extract weather data from Open-Meteo API using Airflow Connection."""

        # Use HTTP Hook to get connection details from Airflow connection

        http_hook=HttpHook(http_conn_id=API_CONN_ID,method='GET')


        #https://api.open-meteo.com/    # this is derfault Api in thich we gonna give longitute and letitude to give responce 

        # https://api.open-meteo.com/v1/forecast?latitude=51.5074&longitude=-0.1278&current_weather=true  # This is full api with longitite and letitute which will give us output 




        # Creating Endpoint 
        endpoint=f'/v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}&current_weather=true'

        ## Make the request via the HTTP Hook
        response=http_hook.run(endpoint)

        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to fetch weather data: {response.status_code}")






     # Task 2 is to transform the data    
    @task()
    def transform_weather_data(weather_data):
        """Transform the extracted weather data."""
        current_weather = weather_data['current_weather']   # getting Current wether data 


        # Creating Dictionary that collects all the information from Current wether data 
        transformed_data = {
            'latitude': LATITUDE,
            'longitude': LONGITUDE,
            'temperature': current_weather['temperature'],
            'windspeed': current_weather['windspeed'],
            'winddirection': current_weather['winddirection'],
            'weathercode': current_weather['weathercode']
        }
        return transformed_data





    # Task 3 is to load the transformd data into PostgreSQL 
    @task()
    def load_weather_data(transformed_data):
        """Load transformed data into PostgreSQL."""



        # Creating a hook to connect with Airflow and Postgres 
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)



        # Creating data base cursor 
        conn = pg_hook.get_conn()
        cursor = conn.cursor()



        # Create table if it doesn't exist
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS weather_data (
            latitude FLOAT,
            longitude FLOAT,
            temperature FLOAT,
            windspeed FLOAT,
            winddirection FLOAT,
            weathercode INT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)



        # Insert transformed data into the table
        cursor.execute("""
        INSERT INTO weather_data (latitude, longitude, temperature, windspeed, winddirection, weathercode)
        VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            transformed_data['latitude'],
            transformed_data['longitude'],
            transformed_data['temperature'],
            transformed_data['windspeed'],
            transformed_data['winddirection'],
            transformed_data['weathercode']
        ))



         # Commiting Changes and clossing the connection 
        conn.commit()
        cursor.close()






    ## DAG Worflow- ETL Pipeline
    weather_data= extract_weather_data()
    transformed_data=transform_weather_data(weather_data)
    load_weather_data(transformed_data)

        
    




    

